package domain

import (
	"fmt"
	"strings"
	"time"
)

type Manifest struct {
	CapturedAt time.Time `json:"capturedAt"`
	Amount     Decimal   `json:"amount"`
	Currency   string    `json:"currency"`
}

func (m Manifest) Validate() error {
	if m.CapturedAt.IsZero() {
		return fmt.Errorf("capturedAt is required")
	}
	currency := strings.TrimSpace(m.Currency)
	if len(currency) != 3 {
		return fmt.Errorf("currency must be a 3-letter ISO-style code")
	}
	for _, r := range currency {
		if r < 'A' || r > 'Z' {
			return fmt.Errorf("currency must be uppercase ASCII")
		}
	}
	if m.Amount.IsZero() {
		// Zero is valid money; validation only rejects a missing/zero-value Decimal when the raw field was omitted.
		// Omission is handled in the HTTP validator via presence checks.
	}
	return nil
}
