package config

import "testing"

func TestLoadFailsWhenRequiredConfigurationIsMissing(t *testing.T) {
	t.Setenv("DATABASE_URL", "")
	t.Setenv("DOWNSTREAM_URL", "")
	if _, err := Load(); err == nil {
		t.Fatal("expected missing required configuration to fail")
	}
}

func TestLoadRejectsInvalidDuration(t *testing.T) {
	t.Setenv("DATABASE_URL", "postgres://example")
	t.Setenv("DOWNSTREAM_URL", "http://downstream")
	t.Setenv("DOWNSTREAM_TIMEOUT", "not-a-duration")
	if _, err := Load(); err == nil {
		t.Fatal("expected invalid duration to fail")
	}
}
