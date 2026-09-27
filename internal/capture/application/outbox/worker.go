package outbox

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"math/rand"
	"time"
)

type Config struct {
	PollInterval time.Duration
	Lease        time.Duration
	MaxAttempts  int
	Retry        RetryPolicy
}

type Worker struct {
	repo       Repository
	downstream Downstream
	clock      Clock
	config     Config
	logger     *slog.Logger
}

func NewWorker(repo Repository, downstream Downstream, clock Clock, config Config, logger *slog.Logger) *Worker {
	return &Worker{repo: repo, downstream: downstream, clock: clock, config: config, logger: logger}
}

func (w *Worker) Run(ctx context.Context) error {
	ticker := time.NewTicker(w.config.PollInterval)
	defer ticker.Stop()
	for {
		if ctx.Err() != nil {
			return ctx.Err()
		}

		// The shutdown context controls whether a new iteration may start.
		// The current delivery gets its own bounded operation context so an in-flight
		// downstream call can finish instead of being cancelled halfway through.
		operationCtx, cancel := context.WithTimeout(context.Background(), w.config.Lease)
		err := w.processOne(operationCtx)
		cancel()
		if err != nil && !errors.Is(err, context.Canceled) {
			w.logger.Error("outbox worker iteration failed", slog.String("error", err.Error()))
		}

		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-ticker.C:
		}
	}
}

func (w *Worker) processOne(ctx context.Context) error {
	now := w.clock.Now().UTC()
	msg, err := w.repo.ClaimNext(ctx, now, w.config.Lease)
	if err != nil {
		return fmt.Errorf("claim outbox message: %w", err)
	}
	if msg == nil {
		return nil
	}

	w.logger.Info("outbox delivery started",
		slog.String("requestId", "outbox-"+msg.ID.String()),
		slog.String("deliveryKey", msg.DeliveryKey),
		slog.Int("attempt", msg.Attempts),
	)

	if err := w.downstream.Deliver(ctx, msg.DeliveryKey, msg.EventType, msg.Payload); err == nil {
		if err := w.repo.MarkDelivered(ctx, msg.ID, w.clock.Now().UTC()); err != nil {
			return fmt.Errorf("mark delivered: %w", err)
		}
		return nil
	} else {
		if msg.Attempts >= w.config.MaxAttempts {
			if markErr := w.repo.MarkDead(ctx, msg.ID, w.clock.Now().UTC(), err.Error()); markErr != nil {
				return fmt.Errorf("mark dead: delivery=%v mark=%w", err, markErr)
			}
			w.logger.Error("outbox delivery moved to dead", slog.String("deliveryKey", msg.DeliveryKey), slog.Int("attempt", msg.Attempts), slog.String("error", err.Error()))
			return nil
		}
		delay := w.config.Retry.Next(msg.Attempts)
		if markErr := w.repo.MarkRetry(ctx, msg.ID, w.clock.Now().UTC().Add(delay), err.Error()); markErr != nil {
			return fmt.Errorf("mark retry: delivery=%v mark=%w", err, markErr)
		}
		w.logger.Warn("outbox delivery scheduled for retry", slog.String("deliveryKey", msg.DeliveryKey), slog.Int("attempt", msg.Attempts), slog.String("error", err.Error()), slog.Duration("delay", delay))
		return nil
	}
}

type SystemClock struct{}

func (SystemClock) Now() time.Time { return time.Now() }

type RandomJitter struct{}

func (RandomJitter) Float64() float64 { return rand.Float64() }
