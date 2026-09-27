package outbox

import (
	"context"
	"errors"
	"log/slog"
	"testing"
	"time"

	"github.com/NevermindG/309-capture-intake/internal/capture/domain"
)

type fakeOutboxRepo struct {
	message   *domain.OutboxMessage
	delivered bool
	retryAt   time.Time
	dead      bool
}

func (f *fakeOutboxRepo) ClaimNext(_ context.Context, _ time.Time, _ time.Duration) (*domain.OutboxMessage, error) {
	if f.message == nil {
		return nil, nil
	}
	m := *f.message
	m.Attempts++
	return &m, nil
}
func (f *fakeOutboxRepo) MarkDelivered(_ context.Context, _ domain.UUID, _ time.Time) error {
	f.delivered = true
	f.message = nil
	return nil
}
func (f *fakeOutboxRepo) MarkRetry(_ context.Context, _ domain.UUID, at time.Time, _ string) error {
	f.retryAt = at
	return nil
}
func (f *fakeOutboxRepo) MarkDead(_ context.Context, _ domain.UUID, _ time.Time, _ string) error {
	f.dead = true
	return nil
}

type fakeDownstream struct{ err error }

func (f fakeDownstream) Deliver(_ context.Context, _ string, _ string, _ []byte) error { return f.err }

type fixedClock struct{ now time.Time }

func (c fixedClock) Now() time.Time { return c.now }

func TestWorkerMarksSuccessfulDelivery(t *testing.T) {
	id, _ := domain.NewUUID()
	repo := &fakeOutboxRepo{message: &domain.OutboxMessage{ID: id, DeliveryKey: "capture:test", EventType: "capture.accepted", Payload: []byte(`{}`)}}
	w := NewWorker(repo, fakeDownstream{}, fixedClock{now: time.Date(2026, 9, 27, 12, 0, 0, 0, time.UTC)}, Config{Lease: time.Minute, MaxAttempts: 3, Retry: RetryPolicy{Base: time.Second, Ceiling: time.Minute}}, slog.Default())
	if err := w.processOne(context.Background()); err != nil {
		t.Fatal(err)
	}
	if !repo.delivered || repo.dead {
		t.Fatalf("unexpected repo state: delivered=%v dead=%v", repo.delivered, repo.dead)
	}
}

func TestWorkerSchedulesRetryWithoutSleeping(t *testing.T) {
	id, _ := domain.NewUUID()
	repo := &fakeOutboxRepo{message: &domain.OutboxMessage{ID: id, DeliveryKey: "capture:test", EventType: "capture.accepted", Payload: []byte(`{}`)}}
	now := time.Date(2026, 9, 27, 12, 0, 0, 0, time.UTC)
	w := NewWorker(repo, fakeDownstream{err: errors.New("503")}, fixedClock{now: now}, Config{Lease: time.Minute, MaxAttempts: 3, Retry: RetryPolicy{Base: 2 * time.Second, Ceiling: time.Minute, Jitter: fixedJitter{value: 0}}}, slog.Default())
	if err := w.processOne(context.Background()); err != nil {
		t.Fatal(err)
	}
	if repo.retryAt.Sub(now) != 2*time.Second {
		t.Fatalf("retryAt delta: got %v", repo.retryAt.Sub(now))
	}
	if repo.dead {
		t.Fatal("message should not be dead after first attempt")
	}
}

func TestWorkerMovesMessageToDeadAtAttemptCeiling(t *testing.T) {
	id, _ := domain.NewUUID()
	repo := &fakeOutboxRepo{message: &domain.OutboxMessage{ID: id, DeliveryKey: "capture:dead", EventType: "capture.accepted", Payload: []byte(`{}`), Attempts: 2}}
	w := NewWorker(repo, fakeDownstream{err: errors.New("permanent")}, fixedClock{now: time.Date(2026, 9, 27, 12, 0, 0, 0, time.UTC)}, Config{Lease: time.Minute, MaxAttempts: 3, Retry: RetryPolicy{Base: time.Second, Ceiling: time.Minute}}, slog.Default())
	if err := w.processOne(context.Background()); err != nil {
		t.Fatal(err)
	}
	if !repo.dead {
		t.Fatal("message should be terminal at the max attempt")
	}
}
