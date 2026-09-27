package domain

import (
	"crypto/rand"
	"encoding/hex"
	"fmt"
)

type UUID [16]byte

func NewUUID() (UUID, error) {
	var id UUID
	if _, err := rand.Read(id[:]); err != nil {
		return UUID{}, fmt.Errorf("generate uuid: %w", err)
	}
	id[6] = (id[6] & 0x0f) | 0x40
	id[8] = (id[8] & 0x3f) | 0x80
	return id, nil
}

func ParseUUID(s string) (UUID, error) {
	var id UUID
	if len(s) != 36 || s[8] != '-' || s[13] != '-' || s[18] != '-' || s[23] != '-' {
		return id, fmt.Errorf("invalid uuid: %q", s)
	}
	decoded, err := hex.DecodeString(s[:8] + s[9:13] + s[14:18] + s[19:23] + s[24:])
	if err != nil || len(decoded) != 16 {
		return id, fmt.Errorf("invalid uuid: %q", s)
	}
	copy(id[:], decoded)
	return id, nil
}

func (id UUID) String() string {
	return fmt.Sprintf("%08x-%04x-%04x-%04x-%012x", id[0:4], id[4:6], id[6:8], id[8:10], id[10:16])
}
