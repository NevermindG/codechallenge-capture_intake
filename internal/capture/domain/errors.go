package domain

import (
	"errors"
	"fmt"
	"strings"
)

var (
	ErrNotFound              = errors.New("capture not found")
	ErrInvalidManifest       = errors.New("invalid manifest")
	ErrInvalidIdempotencyKey = errors.New("invalid idempotency key")
)

func ValidateIdempotencyKey(key string) error {
	key = strings.TrimSpace(key)
	if key == "" {
		return ErrInvalidIdempotencyKey
	}
	if len(key) > 200 {
		return fmt.Errorf("%w: maximum length is 200", ErrInvalidIdempotencyKey)
	}
	for _, r := range key {
		if (r >= 'a' && r <= 'z') || (r >= 'A' && r <= 'Z') || (r >= '0' && r <= '9') || r == '-' || r == '_' || r == '.' || r == ':' {
			continue
		}
		return fmt.Errorf("%w: unsupported character %q", ErrInvalidIdempotencyKey, r)
	}
	return nil
}
