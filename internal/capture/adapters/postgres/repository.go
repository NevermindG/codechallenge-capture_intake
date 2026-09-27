package postgres

import (
	"context"
	"crypto/sha256"
	"encoding/binary"
	"fmt"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/NevermindG/309-capture-intake/internal/capture/application/intake"
	"github.com/NevermindG/309-capture-intake/internal/capture/application/outbox"
	"github.com/NevermindG/309-capture-intake/internal/capture/domain"
)

type Repository struct {
	pool *pgxpool.Pool
}

func NewRepository(pool *pgxpool.Pool) *Repository { return &Repository{pool: pool} }

func (r *Repository) InIdempotencyScope(ctx context.Context, key string, fn func(context.Context, intake.CaptureTx) error) error {
	conn, err := r.pool.Acquire(ctx)
	if err != nil {
		return fmt.Errorf("acquire db connection: %w", err)
	}
	defer conn.Release()

	tx, err := conn.BeginTx(ctx, pgx.TxOptions{IsoLevel: pgx.ReadCommitted})
	if err != nil {
		return fmt.Errorf("begin transaction: %w", err)
	}
	defer func() { _ = tx.Rollback(ctx) }()

	lockKey := advisoryKey(key)
	if _, err := tx.Exec(ctx, `SELECT pg_advisory_xact_lock($1::bigint)`, lockKey); err != nil {
		return fmt.Errorf("lock idempotency key: %w", err)
	}

	txAdapter := &transaction{tx: tx}
	if err := fn(ctx, txAdapter); err != nil {
		return err
	}
	if err := tx.Commit(ctx); err != nil {
		return fmt.Errorf("commit acceptance transaction: %w", err)
	}
	return nil
}

type transaction struct{ tx pgx.Tx }

func (t *transaction) FindByIdempotencyKey(ctx context.Context, key string) (*domain.Capture, error) {
	row := t.tx.QueryRow(ctx, `
		SELECT id::text, idempotency_key, captured_at, amount::text, currency, image_key, created_at
		FROM captures
		WHERE idempotency_key = $1
	`, key)

	var (
		idText, storedKey, amountText, currency, imageKey string
		capturedAt, createdAt                             time.Time
	)
	if err := row.Scan(&idText, &storedKey, &capturedAt, &amountText, &currency, &imageKey, &createdAt); err != nil {
		if err == pgx.ErrNoRows {
			return nil, nil
		}
		return nil, fmt.Errorf("find capture by idempotency key: %w", err)
	}
	id, err := domain.ParseUUID(idText)
	if err != nil {
		return nil, fmt.Errorf("parse capture uuid: %w", err)
	}
	amount, err := domain.ParseDecimal(amountText)
	if err != nil {
		return nil, fmt.Errorf("parse stored amount: %w", err)
	}
	return &domain.Capture{ID: id, IdempotencyKey: storedKey, CapturedAt: capturedAt, Amount: amount, Currency: currency, ImageKey: imageKey, CreatedAt: createdAt}, nil
}

func (t *transaction) InsertAcceptedCapture(ctx context.Context, capture domain.Capture, message domain.OutboxMessage) error {
	if _, err := t.tx.Exec(ctx, `
		INSERT INTO captures (id, idempotency_key, captured_at, amount, currency, image_key, created_at)
		VALUES ($1::uuid, $2, $3, $4::numeric, $5, $6, $7)
	`, capture.ID.String(), capture.IdempotencyKey, capture.CapturedAt, capture.Amount.String(), capture.Currency, capture.ImageKey, capture.CreatedAt); err != nil {
		return fmt.Errorf("insert capture: %w", err)
	}
	if _, err := t.tx.Exec(ctx, `
		INSERT INTO outbox (id, capture_id, event_type, delivery_key, payload, status, attempts, next_attempt_at)
		VALUES ($1::uuid, $2::uuid, $3, $4, $5::jsonb, 'pending', 0, $6)
	`, message.ID.String(), capture.ID.String(), message.EventType, message.DeliveryKey, message.Payload, message.NextAttemptAt); err != nil {
		return fmt.Errorf("insert outbox: %w", err)
	}
	return nil
}

func (r *Repository) FindByIdempotencyKey(ctx context.Context, key string) (*domain.Capture, error) {
	row := r.pool.QueryRow(ctx, `
		SELECT id::text, idempotency_key, captured_at, amount::text, currency, image_key, created_at
		FROM captures WHERE idempotency_key = $1
	`, key)
	var idText, storedKey, amountText, currency, imageKey string
	var capturedAt, createdAt time.Time
	if err := row.Scan(&idText, &storedKey, &capturedAt, &amountText, &currency, &imageKey, &createdAt); err != nil {
		if err == pgx.ErrNoRows {
			return nil, domain.ErrNotFound
		}
		return nil, fmt.Errorf("find capture: %w", err)
	}
	id, err := domain.ParseUUID(idText)
	if err != nil {
		return nil, fmt.Errorf("parse capture uuid: %w", err)
	}
	amount, err := domain.ParseDecimal(amountText)
	if err != nil {
		return nil, fmt.Errorf("parse amount: %w", err)
	}
	return &domain.Capture{ID: id, IdempotencyKey: storedKey, CapturedAt: capturedAt, Amount: amount, Currency: currency, ImageKey: imageKey, CreatedAt: createdAt}, nil
}

func (r *Repository) ClaimNext(ctx context.Context, now time.Time, lease time.Duration) (*domain.OutboxMessage, error) {
	lockedUntil := now.Add(lease)
	row := r.pool.QueryRow(ctx, `
		WITH candidate AS (
			SELECT id
			FROM outbox
			WHERE (status = 'pending' AND next_attempt_at <= $1)
			   OR (status = 'processing' AND locked_until <= $1)
			ORDER BY next_attempt_at, created_at
			FOR UPDATE SKIP LOCKED
			LIMIT 1
		)
		UPDATE outbox AS o
		SET status = 'processing', attempts = o.attempts + 1, locked_until = $2, last_error = NULL
		FROM candidate
		WHERE o.id = candidate.id
		RETURNING o.id::text, o.capture_id::text, o.event_type, o.delivery_key, o.payload::text,
		          o.attempts, o.status, o.next_attempt_at, o.locked_until, COALESCE(o.last_error, '')
	`, now, lockedUntil)

	var idText, captureIDText, eventType, deliveryKey, payloadText, status, lastError string
	var attempts int
	var nextAttemptAt, lockTime time.Time
	if err := row.Scan(&idText, &captureIDText, &eventType, &deliveryKey, &payloadText, &attempts, &status, &nextAttemptAt, &lockTime, &lastError); err != nil {
		if err == pgx.ErrNoRows {
			return nil, nil
		}
		return nil, fmt.Errorf("claim outbox: %w", err)
	}
	id, err := domain.ParseUUID(idText)
	if err != nil {
		return nil, fmt.Errorf("parse outbox uuid: %w", err)
	}
	captureID, err := domain.ParseUUID(captureIDText)
	if err != nil {
		return nil, fmt.Errorf("parse capture uuid: %w", err)
	}
	return &domain.OutboxMessage{ID: id, CaptureID: captureID, EventType: eventType, DeliveryKey: deliveryKey, Payload: []byte(payloadText), Attempts: attempts, Status: status, NextAttemptAt: nextAttemptAt, LockedUntil: &lockTime, LastError: lastError}, nil
}

func (r *Repository) MarkDelivered(ctx context.Context, id domain.UUID, at time.Time) error {
	cmd, err := r.pool.Exec(ctx, `
		UPDATE outbox SET status='delivered', delivered_at=$2, locked_until=NULL, last_error=NULL
		WHERE id=$1::uuid AND status='processing'
	`, id.String(), at)
	if err != nil {
		return fmt.Errorf("mark delivered: %w", err)
	}
	if cmd.RowsAffected() != 1 {
		return fmt.Errorf("mark delivered: message not in processing state")
	}
	return nil
}

func (r *Repository) MarkRetry(ctx context.Context, id domain.UUID, next time.Time, reason string) error {
	cmd, err := r.pool.Exec(ctx, `
		UPDATE outbox SET status='pending', next_attempt_at=$2, locked_until=NULL, last_error=$3
		WHERE id=$1::uuid AND status='processing'
	`, id.String(), next, reason)
	if err != nil {
		return fmt.Errorf("mark retry: %w", err)
	}
	if cmd.RowsAffected() != 1 {
		return fmt.Errorf("mark retry: message not in processing state")
	}
	return nil
}

func (r *Repository) MarkDead(ctx context.Context, id domain.UUID, _ time.Time, reason string) error {
	cmd, err := r.pool.Exec(ctx, `
		UPDATE outbox SET status='dead', locked_until=NULL, last_error=$2
		WHERE id=$1::uuid AND status='processing'
	`, id.String(), reason)
	if err != nil {
		return fmt.Errorf("mark dead: %w", err)
	}
	if cmd.RowsAffected() != 1 {
		return fmt.Errorf("mark dead: message not in processing state")
	}
	return nil
}

func advisoryKey(key string) int64 {
	sum := sha256.Sum256([]byte(key))
	return int64(binary.BigEndian.Uint64(sum[:8]))
}

var _ intake.CaptureUnitOfWork = (*Repository)(nil)
var _ intake.CaptureReader = (*Repository)(nil)
var _ outbox.Repository = (*Repository)(nil)
