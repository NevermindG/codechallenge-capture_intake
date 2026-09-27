package intake

import (
	"context"
	"io"
	"time"

	"github.com/NevermindG/309-capture-intake/internal/capture/domain"
)

type CaptureTx interface {
	FindByIdempotencyKey(ctx context.Context, key string) (*domain.Capture, error)
	InsertAcceptedCapture(ctx context.Context, capture domain.Capture, outbox domain.OutboxMessage) error
}

type CaptureUnitOfWork interface {
	InIdempotencyScope(ctx context.Context, key string, fn func(context.Context, CaptureTx) error) error
}

type ImageStore interface {
	PutAtomically(ctx context.Context, key string, src io.Reader) error
}

type Clock interface {
	Now() time.Time
}

type UUIDGenerator interface {
	NewUUID() (domain.UUID, error)
}

// AcceptanceResult makes the duplicate outcome explicit to the HTTP adapter.
type AcceptanceResult struct {
	Capture   domain.Capture
	Duplicate bool
}
