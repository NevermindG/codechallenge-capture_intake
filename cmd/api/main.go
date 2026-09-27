package main

import (
	"context"
	"errors"
	"log/slog"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/go-chi/chi/v5"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/NevermindG/309-capture-intake/internal/capture/adapters/downstream"
	"github.com/NevermindG/309-capture-intake/internal/capture/adapters/filesystem"
	httpadapter "github.com/NevermindG/309-capture-intake/internal/capture/adapters/http"
	"github.com/NevermindG/309-capture-intake/internal/capture/adapters/postgres"
	"github.com/NevermindG/309-capture-intake/internal/capture/application/intake"
	outboxapp "github.com/NevermindG/309-capture-intake/internal/capture/application/outbox"
	"github.com/NevermindG/309-capture-intake/internal/capture/domain"
	"github.com/NevermindG/309-capture-intake/internal/platform/config"
	"github.com/NevermindG/309-capture-intake/internal/platform/logging"
)

type systemClock struct{}

func (systemClock) Now() time.Time { return time.Now() }

type uuidGenerator struct{}

func (uuidGenerator) NewUUID() (domain.UUID, error) { return domain.NewUUID() }

func main() {
	logger := logging.New()
	cfg, err := config.Load()
	if err != nil {
		logger.Error("invalid configuration", slog.String("error", err.Error()))
		os.Exit(1)
	}

	poolCfg, err := pgxpool.ParseConfig(cfg.DatabaseURL)
	if err != nil {
		logger.Error("invalid database configuration", slog.String("error", err.Error()))
		os.Exit(1)
	}
	poolCfg.MaxConns = 10
	poolCfg.MinConns = 1
	pool := pgxpool.NewWithConfig(context.Background(), poolCfg)
	defer pool.Close()

	startupCtx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	if err := waitForDatabase(startupCtx, pool); err != nil {
		logger.Error("database unavailable", slog.String("error", err.Error()))
		os.Exit(1)
	}
	if err := postgres.Migrate(startupCtx, pool); err != nil {
		logger.Error("migration failed", slog.String("error", err.Error()))
		os.Exit(1)
	}

	images, err := filesystem.NewStore(cfg.StorageDir)
	if err != nil {
		logger.Error("image store unavailable", slog.String("error", err.Error()))
		os.Exit(1)
	}
	repo := postgres.NewRepository(pool)
	clock := systemClock{}
	service := intake.NewService(repo, images, clock, uuidGenerator{})
	handler := httpadapter.NewHandler(service, repo, cfg.MaxBodyBytes, cfg.MaxImageBytes, logger)
	downstreamClient := downstream.NewHTTPClient(cfg.DownstreamURL, cfg.DownstreamTimeout)
	worker := outboxapp.NewWorker(repo, downstreamClient, clock, outboxapp.Config{
		PollInterval: cfg.WorkerPoll,
		Lease:        cfg.WorkerLease,
		MaxAttempts:  cfg.MaxAttempts,
		Retry: outboxapp.RetryPolicy{
			Base: cfg.RetryBase, Ceiling: cfg.RetryCeiling, Jitter: outboxapp.RandomJitter{},
		},
	}, logger)

	router := chi.NewRouter()
	router.Get("/healthz", func(w http.ResponseWriter, _ *http.Request) { w.WriteHeader(http.StatusOK) })
	router.Get("/readyz", func(w http.ResponseWriter, r *http.Request) {
		ctx, cancel := context.WithTimeout(r.Context(), time.Second)
		defer cancel()
		if err := pool.Ping(ctx); err != nil {
			w.WriteHeader(http.StatusServiceUnavailable)
			_, _ = w.Write([]byte(`{"status":"not_ready"}`))
			return
		}
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{"status":"ready"}`))
	})
	router.Mount("/", handler.Routes())

	srv := &http.Server{Addr: cfg.HTTPAddr, Handler: router, ReadHeaderTimeout: 5 * time.Second, IdleTimeout: 60 * time.Second}

	runCtx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	workerDone := make(chan error, 1)
	go func() { workerDone <- worker.Run(runCtx) }()
	serverDone := make(chan error, 1)
	go func() { serverDone <- srv.ListenAndServe() }()

	select {
	case <-runCtx.Done():
		shutdownCtx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		if err := srv.Shutdown(shutdownCtx); err != nil {
			logger.Error("http shutdown failed", slog.String("error", err.Error()))
		}
		cancel()
		<-workerDone
	case err := <-serverDone:
		if !errors.Is(err, http.ErrServerClosed) {
			logger.Error("http server stopped", slog.String("error", err.Error()))
			stop()
		}
		<-workerDone
	}

	logger.Info("service stopped")
}

func waitForDatabase(ctx context.Context, pool *pgxpool.Pool) error {
	for {
		if err := pool.Ping(ctx); err == nil {
			return nil
		} else if ctx.Err() != nil {
			return ctx.Err()
		}
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(500 * time.Millisecond):
		}
	}
}
