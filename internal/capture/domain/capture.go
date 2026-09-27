package domain

import "time"

type Capture struct {
	ID             UUID
	IdempotencyKey string
	CapturedAt     time.Time
	Amount         Decimal
	Currency       string
	ImageKey       string
	CreatedAt      time.Time
}

type OutboxMessage struct {
	ID            UUID
	CaptureID     UUID
	EventType     string
	DeliveryKey   string
	Payload       []byte
	Attempts      int
	Status        string
	NextAttemptAt time.Time
	LockedUntil   *time.Time
	LastError     string
}
