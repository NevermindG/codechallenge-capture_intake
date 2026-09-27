//go:build integration

package postgres

import (
	"bytes"
	"context"
	"encoding/json"
	"io"
	"log/slog"
	"mime/multipart"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"sync"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/NevermindG/309-capture-intake/internal/capture/adapters/filesystem"
	httpadapter "github.com/NevermindG/309-capture-intake/internal/capture/adapters/http"
	"github.com/NevermindG/309-capture-intake/internal/capture/application/intake"
	"github.com/NevermindG/309-capture-intake/internal/capture/domain"
)

type integrationClock struct{ now time.Time }

func (c integrationClock) Now() time.Time { return c.now }

type integrationIDs struct{}

func (integrationIDs) NewUUID() (domain.UUID, error) { return domain.NewUUID() }

func TestConcurrentHTTPIdempotencyLeavesOneCaptureImageAndOutbox(t *testing.T) {
	ctx := context.Background()
	databaseURL := getenvRequired(t, "DATABASE_URL")
	pool, err := pgxpool.New(ctx, databaseURL)
	if err != nil {
		t.Fatal(err)
	}
	defer pool.Close()
	if err := Migrate(ctx, pool); err != nil {
		t.Fatal(err)
	}

	images, err := filesystem.NewStore(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	repo := NewRepository(pool)
	clock := integrationClock{now: time.Date(2026, 9, 27, 17, 0, 0, 0, time.UTC)}
	amount, err := domain.ParseDecimal("123.450000")
	if err != nil {
		t.Fatal(err)
	}
	manifest := domain.Manifest{CapturedAt: clock.now.Add(-time.Hour), Amount: amount, Currency: "PEN"}
	service := intake.NewService(repo, images, clock, &integrationIDs{})
	handler := httpadapter.NewHandler(service, repo, 2*1024*1024, 1024*1024, slog.Default())
	server := httptest.NewServer(handler.Routes())
	defer server.Close()

	key := "integration-race-" + time.Now().UTC().Format("20060102T150405.000000000")
	body, contentType := multipartPayload(t, manifest, []byte{0x89, 0x50, 0x4e, 0x47, 0x0d, 0x0a, 0x1a, 0x0a, 0x00, 0x00, 0x00, 0x0d, 0x49, 0x48, 0x44, 0x52})

	start := make(chan struct{})
	responses := make([]struct {
		status int
		body   []byte
	}, 2)
	errs := make([]error, 2)
	var wg sync.WaitGroup
	for i := range responses {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			<-start
			req, err := http.NewRequest(http.MethodPost, server.URL+"/v1/captures", bytes.NewReader(body))
			if err != nil {
				errs[i] = err
				return
			}
			req.Header.Set("Content-Type", contentType)
			req.Header.Set("Idempotency-Key", key)
			resp, err := http.DefaultClient.Do(req)
			if err != nil {
				errs[i] = err
				return
			}
			defer resp.Body.Close()
			data, err := io.ReadAll(resp.Body)
			if err != nil {
				errs[i] = err
				return
			}
			responses[i].status = resp.StatusCode
			responses[i].body = data
		}(i)
	}
	close(start)
	wg.Wait()
	for _, err := range errs {
		if err != nil {
			t.Fatal(err)
		}
	}

	created, duplicate := 0, 0
	captureIDs := make(map[string]struct{})
	for _, response := range responses {
		if response.status == http.StatusCreated {
			created++
		} else if response.status == http.StatusOK {
			duplicate++
		} else {
			t.Fatalf("unexpected HTTP status=%d body=%s", response.status, response.body)
		}
		var payload struct {
			ID        string `json:"id"`
			Duplicate bool   `json:"duplicate"`
		}
		if err := json.Unmarshal(response.body, &payload); err != nil {
			t.Fatal(err)
		}
		captureIDs[payload.ID] = struct{}{}
		if (response.status == http.StatusCreated && payload.Duplicate) || (response.status == http.StatusOK && !payload.Duplicate) {
			t.Fatalf("status/duplicate mismatch: status=%d body=%s", response.status, response.body)
		}
	}
	if created != 1 || duplicate != 1 {
		t.Fatalf("created=%d duplicate=%d want 1/1", created, duplicate)
	}
	if len(captureIDs) != 1 {
		t.Fatalf("capture ids=%v want exactly one", captureIDs)
	}

	var captureCount, outboxCount int
	if err := pool.QueryRow(ctx, `SELECT count(*) FROM captures WHERE idempotency_key=$1`, key).Scan(&captureCount); err != nil {
		t.Fatal(err)
	}
	if err := pool.QueryRow(ctx, `SELECT count(*) FROM outbox WHERE capture_id=(SELECT id FROM captures WHERE idempotency_key=$1)`, key).Scan(&outboxCount); err != nil {
		t.Fatal(err)
	}
	matches, err := filepath.Glob(filepath.Join(images.Root(), "*.img"))
	if err != nil {
		t.Fatal(err)
	}
	if captureCount != 1 || outboxCount != 1 || len(matches) != 1 {
		t.Fatalf("capture=%d outbox=%d images=%d, want 1/1/1", captureCount, outboxCount, len(matches))
	}

	var storedAmount string
	if err := pool.QueryRow(ctx, `SELECT amount::text FROM captures WHERE idempotency_key=$1`, key).Scan(&storedAmount); err != nil {
		t.Fatal(err)
	}
	if storedAmount != "123.450000" && storedAmount != "123.45" {
		t.Fatalf("unexpected stored numeric: %q", storedAmount)
	}
}

func multipartPayload(t *testing.T, manifest domain.Manifest, image []byte) ([]byte, string) {
	t.Helper()
	var b bytes.Buffer
	mw := multipart.NewWriter(&b)
	manifestPart, err := mw.CreateFormField("manifest")
	if err != nil {
		t.Fatal(err)
	}
	manifestJSON, err := json.Marshal(struct {
		CapturedAt string `json:"capturedAt"`
		Amount     string `json:"amount"`
		Currency   string `json:"currency"`
	}{manifest.CapturedAt.Format(time.RFC3339Nano), manifest.Amount.String(), manifest.Currency})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := manifestPart.Write(manifestJSON); err != nil {
		t.Fatal(err)
	}
	framePart, err := mw.CreateFormFile("frame", "frame.png")
	if err != nil {
		t.Fatal(err)
	}
	if _, err := framePart.Write(image); err != nil {
		t.Fatal(err)
	}
	if err := mw.Close(); err != nil {
		t.Fatal(err)
	}
	return b.Bytes(), mw.FormDataContentType()
}

func getenvRequired(t *testing.T, key string) string {
	t.Helper()
	v := os.Getenv(key)
	if v == "" {
		t.Skipf("%s is required for integration tests", key)
	}
	return v
}
