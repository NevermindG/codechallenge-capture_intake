//go:build integration

package outbox_test

import (
	"context"
	"encoding/json"
	"log/slog"
	"os"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/NevermindG/309-capture-intake/internal/capture/adapters/postgres"
	"github.com/NevermindG/309-capture-intake/internal/capture/application/outbox"
	"github.com/NevermindG/309-capture-intake/internal/capture/domain"
)

type integrationDownstream struct {
	calls atomic.Int64
	done  chan struct{}
	once  sync.Once
}

func (d *integrationDownstream) Deliver(_ context.Context, _ string, _ string, _ []byte) error {
	d.calls.Add(1)
	d.once.Do(func() { close(d.done) })
	return nil
}

type integrationWorkerClock struct{ now time.Time }

func (c integrationWorkerClock) Now() time.Time { return c.now }

func TestTwoWorkersCannotClaimSameOutboxRow(t *testing.T) {
	ctx := context.Background()
	databaseURL := osRequired(t, "DATABASE_URL")
	pool, err := pgxpool.New(ctx, databaseURL)
	if err != nil {
		t.Fatal(err)
	}
	defer pool.Close()
	if err := postgres.Migrate(ctx, pool); err != nil {
		t.Fatal(err)
	}

	captureID, err := domain.NewUUID()
	if err != nil {
		t.Fatal(err)
	}
	outboxID, err := domain.NewUUID()
	if err != nil {
		t.Fatal(err)
	}
	key := "worker-race-" + time.Now().UTC().Format("20060102T150405.000000000")
	_, err = pool.Exec(ctx, `
		INSERT INTO captures (id, idempotency_key, captured_at, amount, currency, image_key, created_at)
		VALUES ($1::uuid, $2, now(), '1.00'::numeric, 'PEN', $3, now())
	`, captureID.String(), key, captureID.String()+".img")
	if err != nil {
		t.Fatal(err)
	}
	payload, err := json.Marshal(map[string]string{"captureId": captureID.String()})
	if err != nil {
		t.Fatal(err)
	}
	_, err = pool.Exec(ctx, `
		INSERT INTO outbox (id, capture_id, event_type, delivery_key, payload, status, attempts, next_attempt_at)
		VALUES ($1::uuid, $2::uuid, 'capture.accepted', $3, $4::jsonb, 'pending', 0, now())
	`, outboxID.String(), captureID.String(), "capture:"+captureID.String(), payload)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _, _ = pool.Exec(ctx, `DELETE FROM outbox WHERE id=$1::uuid`, outboxID.String()) }()
	defer func() { _, _ = pool.Exec(ctx, `DELETE FROM captures WHERE id=$1::uuid`, captureID.String()) }()

	repo := postgres.NewRepository(pool)
	downstream := &integrationDownstream{done: make(chan struct{})}
	clock := integrationWorkerClock{now: time.Now().UTC()}
	cfg := outbox.Config{
		PollInterval: time.Millisecond,
		Lease:        30 * time.Second,
		MaxAttempts:  3,
		Retry:        outbox.RetryPolicy{Base: time.Second, Ceiling: time.Minute},
	}
	w1 := outbox.NewWorker(repo, downstream, clock, cfg, slog.Default())
	w2 := outbox.NewWorker(repo, downstream, clock, cfg, slog.Default())

	runCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	var wg sync.WaitGroup
	for _, worker := range []*outbox.Worker{w1, w2} {
		wg.Add(1)
		go func(w *outbox.Worker) {
			defer wg.Done()
			_ = w.Run(runCtx)
		}(worker)
	}

	select {
	case <-downstream.done:
		cancel()
	case <-runCtx.Done():
		t.Fatal("workers did not deliver within timeout")
	}
	wg.Wait()

	if downstream.calls.Load() != 1 {
		t.Fatalf("downstream calls=%d want 1", downstream.calls.Load())
	}
}

func osRequired(t *testing.T, key string) string {
	t.Helper()
	v := os.Getenv(key)
	if v == "" {
		t.Skipf("%s is required for integration tests", key)
	}
	return v
}
