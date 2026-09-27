package intake

import (
	"bytes"
	"context"
	"io"
	"sync"
	"testing"
	"time"

	"github.com/NevermindG/309-capture-intake/internal/capture/domain"
)

type fakeTx struct {
	mu      sync.Mutex
	capture *domain.Capture
	outbox  *domain.OutboxMessage
}

func (f *fakeTx) FindByIdempotencyKey(_ context.Context, key string) (*domain.Capture, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.capture == nil || f.capture.IdempotencyKey != key {
		return nil, nil
	}
	c := *f.capture
	return &c, nil
}
func (f *fakeTx) InsertAcceptedCapture(_ context.Context, c domain.Capture, o domain.OutboxMessage) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.capture = &c
	f.outbox = &o
	return nil
}

type fakeUOW struct{ tx *fakeTx }

func (u fakeUOW) InIdempotencyScope(ctx context.Context, key string, fn func(context.Context, CaptureTx) error) error {
	return fn(ctx, u.tx)
}

type fakeStore struct {
	mu   sync.Mutex
	puts int
	data map[string][]byte
}

func (s *fakeStore) PutAtomically(_ context.Context, key string, src io.Reader) error {
	b, _ := io.ReadAll(src)
	s.mu.Lock()
	defer s.mu.Unlock()
	s.puts++
	s.data[key] = b
	return nil
}

type fakeClock struct{ now time.Time }

func (c fakeClock) Now() time.Time { return c.now }

type seqIDs struct{ next byte }

func (g *seqIDs) NewUUID() (domain.UUID, error) {
	var id domain.UUID
	id[0] = g.next
	g.next++
	return id, nil
}

func TestServiceCreatesCaptureAndOutboxTogether(t *testing.T) {
	tx := &fakeTx{}
	store := &fakeStore{data: make(map[string][]byte)}
	service := NewService(fakeUOW{tx: tx}, store, fakeClock{now: time.Date(2026, 9, 27, 12, 0, 0, 0, time.UTC)}, &seqIDs{})
	amount, _ := domain.ParseDecimal("99.9900")
	manifest := domain.Manifest{CapturedAt: time.Date(2026, 9, 27, 11, 0, 0, 0, time.FixedZone("-05", -5*3600)), Amount: amount, Currency: "PEN"}
	result, err := service.Accept(context.Background(), "key-1", manifest, bytes.NewReader([]byte("image-data")))
	if err != nil {
		t.Fatal(err)
	}
	if result.Duplicate {
		t.Fatal("first request must not be duplicate")
	}
	if tx.capture == nil || tx.outbox == nil {
		t.Fatal("capture and outbox must both be created")
	}
	if store.puts != 1 {
		t.Fatalf("puts=%d want 1", store.puts)
	}
}

func TestServiceReturnsExistingResultWithoutWritingSecondImage(t *testing.T) {
	id, _ := domain.NewUUID()
	amount, _ := domain.ParseDecimal("1.00")
	existing := domain.Capture{ID: id, IdempotencyKey: "same-key", CapturedAt: time.Now(), Amount: amount, Currency: "PEN", ImageKey: "abc.img", CreatedAt: time.Now()}
	tx := &fakeTx{capture: &existing}
	store := &fakeStore{data: make(map[string][]byte)}
	service := NewService(fakeUOW{tx: tx}, store, fakeClock{now: time.Now()}, &seqIDs{})
	result, err := service.Accept(context.Background(), "same-key", domain.Manifest{CapturedAt: existing.CapturedAt, Amount: amount, Currency: "PEN"}, bytes.NewReader([]byte("new-image")))
	if err != nil {
		t.Fatal(err)
	}
	if !result.Duplicate {
		t.Fatal("expected duplicate")
	}
	if store.puts != 0 {
		t.Fatalf("duplicate must not write image; puts=%d", store.puts)
	}
	if result.Capture.ID != existing.ID {
		t.Fatal("duplicate must return original capture")
	}
}

func TestServiceDoesNotDeduplicateDifferentKeysWithSameBody(t *testing.T) {
	tx := &fakeTx{}
	store := &fakeStore{data: make(map[string][]byte)}
	service := NewService(fakeUOW{tx: tx}, store, fakeClock{now: time.Date(2026, 9, 27, 12, 0, 0, 0, time.UTC)}, &seqIDs{})
	amount, _ := domain.ParseDecimal("42.00")
	manifest := domain.Manifest{CapturedAt: time.Date(2026, 9, 27, 11, 0, 0, 0, time.UTC), Amount: amount, Currency: "PEN"}

	first, err := service.Accept(context.Background(), "key-a", manifest, bytes.NewReader([]byte("same-image")))
	if err != nil {
		t.Fatal(err)
	}
	second, err := service.Accept(context.Background(), "key-b", manifest, bytes.NewReader([]byte("same-image")))
	if err != nil {
		t.Fatal(err)
	}
	if first.Duplicate || second.Duplicate {
		t.Fatal("different idempotency keys must not be treated as duplicates")
	}
	if first.Capture.ID == second.Capture.ID {
		t.Fatal("different keys must produce different capture ids")
	}
	if store.puts != 2 {
		t.Fatalf("image writes=%d want 2", store.puts)
	}
}
