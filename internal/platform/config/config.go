package config

import (
	"fmt"
	"os"
	"strconv"
	"time"
)

type Config struct {
	DatabaseURL       string
	DownstreamURL     string
	HTTPAddr          string
	StorageDir        string
	MaxBodyBytes      int64
	MaxImageBytes     int64
	WorkerPoll        time.Duration
	WorkerLease       time.Duration
	DownstreamTimeout time.Duration
	RetryBase         time.Duration
	RetryCeiling      time.Duration
	MaxAttempts       int
}

func Load() (Config, error) {
	maxBody, err := envInt64("MAX_BODY_BYTES", 11*1024*1024)
	if err != nil {
		return Config{}, err
	}
	maxImage, err := envInt64("MAX_IMAGE_BYTES", 10*1024*1024)
	if err != nil {
		return Config{}, err
	}
	workerPoll, err := envDuration("WORKER_POLL_INTERVAL", 500*time.Millisecond)
	if err != nil {
		return Config{}, err
	}
	workerLease, err := envDuration("WORKER_LEASE", 30*time.Second)
	if err != nil {
		return Config{}, err
	}
	downstreamTimeout, err := envDuration("DOWNSTREAM_TIMEOUT", 10*time.Second)
	if err != nil {
		return Config{}, err
	}
	retryBase, err := envDuration("RETRY_BASE", time.Second)
	if err != nil {
		return Config{}, err
	}
	retryCeiling, err := envDuration("RETRY_CEILING", time.Minute)
	if err != nil {
		return Config{}, err
	}
	maxAttempts64, err := envInt64("MAX_ATTEMPTS", 6)
	if err != nil {
		return Config{}, err
	}

	cfg := Config{
		DatabaseURL:       os.Getenv("DATABASE_URL"),
		DownstreamURL:     os.Getenv("DOWNSTREAM_URL"),
		HTTPAddr:          envString("HTTP_ADDR", ":8080"),
		StorageDir:        envString("STORAGE_DIR", "./data/images"),
		MaxBodyBytes:      maxBody,
		MaxImageBytes:     maxImage,
		WorkerPoll:        workerPoll,
		WorkerLease:       workerLease,
		DownstreamTimeout: downstreamTimeout,
		RetryBase:         retryBase,
		RetryCeiling:      retryCeiling,
		MaxAttempts:       int(maxAttempts64),
	}
	if cfg.DatabaseURL == "" {
		return Config{}, fmt.Errorf("DATABASE_URL is required")
	}
	if cfg.DownstreamURL == "" {
		return Config{}, fmt.Errorf("DOWNSTREAM_URL is required")
	}
	if cfg.MaxBodyBytes <= 0 || cfg.MaxImageBytes <= 0 || cfg.MaxImageBytes >= cfg.MaxBodyBytes {
		return Config{}, fmt.Errorf("MAX_IMAGE_BYTES must be positive and lower than MAX_BODY_BYTES")
	}
	if cfg.WorkerPoll <= 0 || cfg.WorkerLease <= 0 || cfg.DownstreamTimeout <= 0 {
		return Config{}, fmt.Errorf("worker and downstream durations must be positive")
	}
	if cfg.WorkerLease <= cfg.DownstreamTimeout {
		return Config{}, fmt.Errorf("WORKER_LEASE must be greater than DOWNSTREAM_TIMEOUT to avoid overlapping claims")
	}
	if cfg.RetryBase <= 0 || cfg.RetryCeiling < cfg.RetryBase {
		return Config{}, fmt.Errorf("RETRY_CEILING must be >= RETRY_BASE > 0")
	}
	if cfg.MaxAttempts < 1 {
		return Config{}, fmt.Errorf("MAX_ATTEMPTS must be >= 1")
	}
	return cfg, nil
}

func envString(key, fallback string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return fallback
}

func envInt64(key string, fallback int64) (int64, error) {
	v := os.Getenv(key)
	if v == "" {
		return fallback, nil
	}
	n, err := strconv.ParseInt(v, 10, 64)
	if err != nil {
		return 0, fmt.Errorf("%s must be an integer: %w", key, err)
	}
	return n, nil
}

func envDuration(key string, fallback time.Duration) (time.Duration, error) {
	v := os.Getenv(key)
	if v == "" {
		return fallback, nil
	}
	d, err := time.ParseDuration(v)
	if err != nil {
		return 0, fmt.Errorf("%s must be a duration: %w", key, err)
	}
	return d, nil
}
