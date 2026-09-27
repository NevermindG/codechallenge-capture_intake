package outbox

import (
	"context"
	"time"

	"github.com/NevermindG/309-capture-intake/internal/capture/domain"
)

type Repository interface {
	ClaimNext(ctx context.Context, now time.Time, lease time.Duration) (*domain.OutboxMessage, error)
	MarkDelivered(ctx context.Context, id domain.UUID, at time.Time) error
	MarkRetry(ctx context.Context, id domain.UUID, nextAttemptAt time.Time, reason string) error
	MarkDead(ctx context.Context, id domain.UUID, at time.Time, reason string) error
}

type Downstream interface {
	Deliver(ctx context.Context, deliveryKey string, eventType string, payload []byte) error
}

type Clock interface {
	Now() time.Time
}

type Jitter interface {
	Float64() float64
}
