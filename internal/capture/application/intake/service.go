package intake

import (
	"context"
	"crypto/sha256"
	"encoding/json"
	"fmt"
	"io"
	"strings"

	"github.com/NevermindG/309-capture-intake/internal/capture/domain"
)

const (
	DeliveryEventType = "capture.accepted"
)

type Service struct {
	uow    CaptureUnitOfWork
	images ImageStore
	clock  Clock
	ids    UUIDGenerator
}

func NewService(uow CaptureUnitOfWork, images ImageStore, clock Clock, ids UUIDGenerator) *Service {
	return &Service{uow: uow, images: images, clock: clock, ids: ids}
}

func (s *Service) Accept(ctx context.Context, key string, manifest domain.Manifest, image io.Reader) (AcceptanceResult, error) {
	key = strings.TrimSpace(key)
	if err := domain.ValidateIdempotencyKey(key); err != nil {
		return AcceptanceResult{}, err
	}
	if err := manifest.Validate(); err != nil {
		return AcceptanceResult{}, fmt.Errorf("validate manifest: %w", err)
	}

	var result AcceptanceResult
	err := s.uow.InIdempotencyScope(ctx, key, func(txCtx context.Context, tx CaptureTx) error {
		existing, err := tx.FindByIdempotencyKey(txCtx, key)
		if err != nil {
			return err
		}
		if existing != nil {
			result = AcceptanceResult{Capture: *existing, Duplicate: true}
			return nil
		}

		id, err := s.ids.NewUUID()
		if err != nil {
			return fmt.Errorf("generate capture id: %w", err)
		}
		deliveryID, err := s.ids.NewUUID()
		if err != nil {
			return fmt.Errorf("generate delivery id: %w", err)
		}

		imageKey := keyHash(key) + ".img"
		if err := s.images.PutAtomically(txCtx, imageKey, image); err != nil {
			return fmt.Errorf("store image: %w", err)
		}

		now := s.clock.Now().UTC()
		capture := domain.Capture{
			ID:             id,
			IdempotencyKey: key,
			CapturedAt:     manifest.CapturedAt.UTC(),
			Amount:         manifest.Amount,
			Currency:       manifest.Currency,
			ImageKey:       imageKey,
			CreatedAt:      now,
		}
		payload, err := json.Marshal(struct {
			ID             string `json:"captureId"`
			IdempotencyKey string `json:"idempotencyKey"`
			CapturedAt     string `json:"capturedAt"`
			Amount         string `json:"amount"`
			Currency       string `json:"currency"`
			ImageKey       string `json:"imageKey"`
		}{
			ID: capture.ID.String(), IdempotencyKey: key, CapturedAt: capture.CapturedAt.Format("2006-01-02T15:04:05.999999999Z07:00"),
			Amount: capture.Amount.String(), Currency: capture.Currency, ImageKey: capture.ImageKey,
		})
		if err != nil {
			return fmt.Errorf("marshal outbox payload: %w", err)
		}

		outbox := domain.OutboxMessage{
			ID:            deliveryID,
			CaptureID:     capture.ID,
			EventType:     DeliveryEventType,
			DeliveryKey:   "capture:" + capture.ID.String(),
			Payload:       payload,
			Attempts:      0,
			Status:        "pending",
			NextAttemptAt: now,
		}
		if err := tx.InsertAcceptedCapture(txCtx, capture, outbox); err != nil {
			return err
		}
		result = AcceptanceResult{Capture: capture, Duplicate: false}
		return nil
	})
	if err != nil {
		return AcceptanceResult{}, err
	}
	return result, nil
}

func keyHash(key string) string {
	sum := sha256.Sum256([]byte(key))
	return fmt.Sprintf("%x", sum[:])
}
