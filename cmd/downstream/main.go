package main

import (
	"encoding/json"
	"log/slog"
	"net/http"
	"os"
	"strconv"
	"sync"
	"sync/atomic"
	"time"
)

type stub struct {
	seen     sync.Map
	failLeft atomic.Int64
	delay    time.Duration
	logger   *slog.Logger
}

func main() {
	logger := slog.New(slog.NewJSONHandler(os.Stdout, nil))
	failFirst := envInt64("STUB_FAIL_FIRST_N", 0)
	if failFirst < 0 {
		failFirst = 0
	}
	stub := &stub{delay: envDuration("STUB_DELAY_MS", 0), logger: logger}
	stub.failLeft.Store(failFirst)

	mux := http.NewServeMux()
	mux.HandleFunc("GET /healthz", func(w http.ResponseWriter, _ *http.Request) { w.WriteHeader(http.StatusOK) })
	mux.HandleFunc("GET /stats", stub.stats)
	mux.HandleFunc("POST /v1/deliveries", stub.deliver)

	addr := envString("DOWNSTREAM_ADDR", ":8081")
	logger.Info("stub downstream listening", slog.String("addr", addr))
	if err := http.ListenAndServe(addr, mux); err != nil {
		logger.Error("stub downstream stopped", slog.String("error", err.Error()))
		os.Exit(1)
	}
}

func (s *stub) deliver(w http.ResponseWriter, r *http.Request) {
	key := r.Header.Get("X-Delivery-Key")
	if key == "" {
		writeErr(w, http.StatusBadRequest, "X-Delivery-Key is required")
		return
	}
	if s.delay > 0 {
		time.Sleep(s.delay)
	}
	if left := s.failLeft.Load(); left > 0 {
		if s.failLeft.CompareAndSwap(left, left-1) {
			writeErr(w, http.StatusServiceUnavailable, "configured transient failure")
			return
		}
	}
	_, loaded := s.seen.LoadOrStore(key, time.Now().UTC())
	if loaded {
		_ = json.NewEncoder(w).Encode(map[string]any{"status": "duplicate", "deliveryKey": key})
		return
	}
	s.logger.Info("stub downstream accepted delivery", slog.String("deliveryKey", key), slog.String("eventType", r.Header.Get("X-Event-Type")))
	_ = json.NewEncoder(w).Encode(map[string]any{"status": "accepted", "deliveryKey": key})
}

func (s *stub) stats(w http.ResponseWriter, _ *http.Request) {
	var count int
	s.seen.Range(func(_, _ any) bool { count++; return true })
	_ = json.NewEncoder(w).Encode(map[string]any{"uniqueDeliveries": count})
}

func writeErr(w http.ResponseWriter, code int, message string) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(code)
	_ = json.NewEncoder(w).Encode(map[string]string{"message": message})
}

func envString(key, fallback string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return fallback
}
func envInt64(key string, fallback int64) int64 {
	if v := os.Getenv(key); v != "" {
		if n, err := strconv.ParseInt(v, 10, 64); err == nil {
			return n
		}
	}
	return fallback
}
func envDuration(key string, ms int64) time.Duration {
	if v := os.Getenv(key); v != "" {
		n, err := strconv.ParseInt(v, 10, 64)
		if err == nil {
			return time.Duration(n) * time.Millisecond
		}
	}
	return time.Duration(ms) * time.Millisecond
}
