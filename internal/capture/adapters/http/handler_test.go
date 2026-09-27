package httpadapter

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"io"
	"log/slog"
	"mime/multipart"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/NevermindG/309-capture-intake/internal/capture/application/intake"
	"github.com/NevermindG/309-capture-intake/internal/capture/domain"
)

type fakeTx struct{ capture *domain.Capture }

func (f *fakeTx) FindByIdempotencyKey(_ context.Context, _ string) (*domain.Capture, error) {
	if f.capture == nil {
		return nil, nil
	}
	c := *f.capture
	return &c, nil
}

func (f *fakeTx) InsertAcceptedCapture(_ context.Context, c domain.Capture, _ domain.OutboxMessage) error {
	f.capture = &c
	return nil
}

type fakeUOW struct {
	tx  *fakeTx
	err error
}

func (u *fakeUOW) InIdempotencyScope(ctx context.Context, _ string, fn func(context.Context, intake.CaptureTx) error) error {
	if u.err != nil {
		return u.err
	}
	return fn(ctx, u.tx)
}

type fakeImages struct{}

func (fakeImages) PutAtomically(_ context.Context, _ string, src io.Reader) error {
	_, err := io.Copy(io.Discard, src)
	return err
}

type fakeIDs struct{ n byte }

func (g *fakeIDs) NewUUID() (domain.UUID, error) {
	var id domain.UUID
	id[0] = g.n
	g.n++
	return id, nil
}

type fakeClock struct{ now time.Time }

func (c fakeClock) Now() time.Time { return c.now }

type txReader struct{ tx *fakeTx }

func (r txReader) FindByIdempotencyKey(_ context.Context, _ string) (*domain.Capture, error) {
	capture, err := r.tx.FindByIdempotencyKey(context.Background(), "")
	if err != nil {
		return nil, err
	}
	if capture == nil {
		return nil, domain.ErrNotFound
	}
	return capture, nil
}

func newTestHandler() *Handler {
	tx := &fakeTx{}
	service := intake.NewService(
		&fakeUOW{tx: tx},
		fakeImages{},
		fakeClock{now: time.Date(2026, 9, 27, 12, 0, 0, 0, time.UTC)},
		&fakeIDs{},
	)
	return NewHandler(service, txReader{tx: tx}, 1024*1024, 1024, slog.Default())
}

func TestCreateErrorContracts(t *testing.T) {
	validManifest := `{"capturedAt":"2026-09-27T12:00:00Z","amount":"10.0000","currency":"PEN"}`
	cases := []struct {
		name     string
		request  func() *http.Request
		wantHTTP int
		wantCode int
	}{
		{
			name: "missing key",
			request: func() *http.Request {
				return httptest.NewRequest(http.MethodPost, "/v1/captures", bytes.NewReader(nil))
			},
			wantHTTP: 400, wantCode: 400001,
		},
		{
			name: "invalid key",
			request: func() *http.Request {
				body, contentType := multipartBody(t, validManifest, []byte("GIF89a"))
				req := httptest.NewRequest(http.MethodPost, "/v1/captures", body)
				req.Header.Set("Content-Type", contentType)
				req.Header.Set("Idempotency-Key", "bad/key")
				return req
			},
			wantHTTP: 400, wantCode: 400002,
		},
		{
			name: "malformed multipart",
			request: func() *http.Request {
				req := httptest.NewRequest(http.MethodPost, "/v1/captures", bytes.NewBufferString("no"))
				req.Header.Set("Content-Type", "multipart/form-data; boundary=does-not-match")
				req.Header.Set("Idempotency-Key", "key-1")
				return req
			},
			wantHTTP: 400, wantCode: 400003,
		},
		{
			name: "manifest missing",
			request: func() *http.Request {
				body, contentType := multipartFrameOnly(t, []byte("GIF89a"))
				req := httptest.NewRequest(http.MethodPost, "/v1/captures", body)
				req.Header.Set("Content-Type", contentType)
				req.Header.Set("Idempotency-Key", "key-1")
				return req
			},
			wantHTTP: 422, wantCode: 422001,
		},
		{
			name: "invalid manifest",
			request: func() *http.Request {
				body, contentType := multipartBody(t, `{"capturedAt":"bad","amount":"10.00","currency":"PEN"}`, []byte("GIF89a"))
				req := httptest.NewRequest(http.MethodPost, "/v1/captures", body)
				req.Header.Set("Content-Type", contentType)
				req.Header.Set("Idempotency-Key", "key-1")
				return req
			},
			wantHTTP: 422, wantCode: 422003,
		},
		{
			name: "frame missing",
			request: func() *http.Request {
				body, contentType := multipartManifestOnly(t, validManifest)
				req := httptest.NewRequest(http.MethodPost, "/v1/captures", body)
				req.Header.Set("Content-Type", contentType)
				req.Header.Set("Idempotency-Key", "key-1")
				return req
			},
			wantHTTP: 422, wantCode: 422004,
		},
		{
			name: "invalid image",
			request: func() *http.Request {
				body, contentType := multipartBody(t, validManifest, []byte("not-image"))
				req := httptest.NewRequest(http.MethodPost, "/v1/captures", body)
				req.Header.Set("Content-Type", contentType)
				req.Header.Set("Idempotency-Key", "key-1")
				return req
			},
			wantHTTP: 422, wantCode: 422002,
		},
		{
			name: "image too large",
			request: func() *http.Request {
				body, contentType := multipartBody(t, validManifest, append([]byte("GIF89a"), bytes.Repeat([]byte("x"), 1020)...))
				req := httptest.NewRequest(http.MethodPost, "/v1/captures", body)
				req.Header.Set("Content-Type", contentType)
				req.Header.Set("Idempotency-Key", "key-1")
				return req
			},
			wantHTTP: 413, wantCode: 413002,
		},
		{
			name: "request too large",
			request: func() *http.Request {
				body, contentType := multipartBody(t, validManifest, []byte("GIF89a"))
				req := httptest.NewRequest(http.MethodPost, "/v1/captures", body)
				req.ContentLength = 2 * 1024 * 1024
				req.Header.Set("Content-Type", contentType)
				req.Header.Set("Idempotency-Key", "key-1")
				return req
			},
			wantHTTP: 413, wantCode: 413001,
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			req := tc.request()
			w := httptest.NewRecorder()
			newTestHandler().createCapture(w, req)
			var got apiError
			if err := json.Unmarshal(w.Body.Bytes(), &got); err != nil {
				t.Fatalf("invalid error response: %v body=%s", err, w.Body.String())
			}
			if w.Code != tc.wantHTTP || got.Code != tc.wantCode || got.HTTPCode != tc.wantHTTP {
				t.Fatalf("status=%d error=%+v, want status=%d code=%d", w.Code, got, tc.wantHTTP, tc.wantCode)
			}
		})
	}
}

func TestCreateStorageFailureReturns503(t *testing.T) {
	tx := &fakeTx{}
	service := intake.NewService(
		&fakeUOW{tx: tx, err: errors.New("db unavailable")},
		fakeImages{},
		fakeClock{now: time.Now()},
		&fakeIDs{},
	)
	h := NewHandler(service, txReader{tx: tx}, 1024*1024, 1024, slog.Default())
	body, contentType := multipartBody(t, `{"capturedAt":"2026-09-27T12:00:00Z","amount":"10.00","currency":"PEN"}`, []byte("GIF89a"))
	req := httptest.NewRequest(http.MethodPost, "/v1/captures", body)
	req.Header.Set("Content-Type", contentType)
	req.Header.Set("Idempotency-Key", "key-storage")
	w := httptest.NewRecorder()
	h.createCapture(w, req)
	var got apiError
	if err := json.Unmarshal(w.Body.Bytes(), &got); err != nil {
		t.Fatal(err)
	}
	if w.Code != 503 || got.Code != 503001 || got.HTTPCode != 503 {
		t.Fatalf("unexpected: %+v", got)
	}
}

func TestGetNotFoundUsesContract(t *testing.T) {
	h := newTestHandler()
	req := httptest.NewRequest(http.MethodGet, "/v1/captures/missing-key", nil)
	w := httptest.NewRecorder()
	h.Routes().ServeHTTP(w, req)
	var got apiError
	if err := json.Unmarshal(w.Body.Bytes(), &got); err != nil {
		t.Fatal(err)
	}
	if w.Code != 404 || got.Code != 404001 || got.HTTPCode != 404 {
		t.Fatalf("unexpected: %+v", got)
	}
}

func TestCreateSuccessReturns201AndRequestID(t *testing.T) {
	body, contentType := multipartBody(t, `{"capturedAt":"2026-09-27T12:00:00Z","amount":"10.0000","currency":"PEN"}`, []byte("GIF89a"))
	req := httptest.NewRequest(http.MethodPost, "/v1/captures", body)
	req.Header.Set("Content-Type", contentType)
	req.Header.Set("Idempotency-Key", "key-success")
	req.Header.Set("X-Request-ID", "req-test")
	w := httptest.NewRecorder()
	newTestHandler().createCapture(w, req)
	if w.Code != 201 {
		t.Fatalf("status=%d body=%s", w.Code, w.Body.String())
	}
	if w.Header().Get("X-Request-ID") != "req-test" {
		t.Fatalf("request id not propagated")
	}
}

func multipartBody(t *testing.T, manifest string, frame []byte) (*bytes.Buffer, string) {
	t.Helper()
	var b bytes.Buffer
	mw := multipart.NewWriter(&b)
	part, err := mw.CreateFormField("manifest")
	if err != nil {
		t.Fatal(err)
	}
	if _, err := part.Write([]byte(manifest)); err != nil {
		t.Fatal(err)
	}
	framePart, err := mw.CreateFormFile("frame", "frame.gif")
	if err != nil {
		t.Fatal(err)
	}
	if _, err := framePart.Write(frame); err != nil {
		t.Fatal(err)
	}
	if err := mw.Close(); err != nil {
		t.Fatal(err)
	}
	return &b, mw.FormDataContentType()
}

func multipartFrameOnly(t *testing.T, frame []byte) (*bytes.Buffer, string) {
	t.Helper()
	var b bytes.Buffer
	mw := multipart.NewWriter(&b)
	part, err := mw.CreateFormFile("frame", "frame.gif")
	if err != nil {
		t.Fatal(err)
	}
	if _, err := part.Write(frame); err != nil {
		t.Fatal(err)
	}
	if err := mw.Close(); err != nil {
		t.Fatal(err)
	}
	return &b, mw.FormDataContentType()
}

func multipartManifestOnly(t *testing.T, manifest string) (*bytes.Buffer, string) {
	t.Helper()
	var b bytes.Buffer
	mw := multipart.NewWriter(&b)
	part, err := mw.CreateFormField("manifest")
	if err != nil {
		t.Fatal(err)
	}
	if _, err := part.Write([]byte(manifest)); err != nil {
		t.Fatal(err)
	}
	if err := mw.Close(); err != nil {
		t.Fatal(err)
	}
	return &b, mw.FormDataContentType()
}
