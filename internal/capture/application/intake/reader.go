package intake

import (
	"context"

	"github.com/NevermindG/309-capture-intake/internal/capture/domain"
)

type CaptureReader interface {
	FindByIdempotencyKey(ctx context.Context, key string) (*domain.Capture, error)
}
