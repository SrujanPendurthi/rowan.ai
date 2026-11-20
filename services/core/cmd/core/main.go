// Command core serves the Rowan agent HTTP API.
package main

import (
	"context"
	"errors"
	"log"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"golang.org/x/time/rate"

	"github.com/jackc/pgx/v5/pgxpool"

	"rowan.ai/rowan/services/core/internal/api"
	"rowan.ai/rowan/services/core/internal/db"
	"rowan.ai/rowan/services/core/internal/enrich"
	"rowan.ai/rowan/services/core/internal/ledger"
)

func main() {
	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()
	if err := run(ctx); err != nil {
		log.Fatal(err)
	}
}

func run(ctx context.Context) error {
	var provider enrich.Provider
	switch os.Getenv("ROWAN_PROVIDER") {
	case "fake":
		provider = &enrich.FakeProvider{}
	default:
		// Real providers arrive in Plan 4.
		return errors.New("no provider configured: set ROWAN_PROVIDER=fake")
	}
	dsn := os.Getenv("DATABASE_URL")
	if dsn == "" {
		log.Fatal("DATABASE_URL is required")
	}
	pool, err := db.Open(ctx, dsn)
	if err != nil {
		return err
	}
	defer pool.Close()
	if err := db.Migrate(ctx, pool); err != nil {
		return err
	}
	artifactDir := os.Getenv("ROWAN_ARTIFACT_DIR")
	if artifactDir == "" {
		artifactDir = "artifacts"
	}
	port := os.Getenv("PORT")
	if port == "" {
		port = "8080"
	}
	svc := &enrich.Service{Pool: pool, Provider: provider,
		// ponytail: per-process provider limiter; share it (Redis/PG) if more than one instance runs.
		Limiter:  rate.NewLimiter(5, 5),
		CacheTTL: 30 * 24 * time.Hour, Now: time.Now}
	srv := &http.Server{
		Addr:              ":" + port,
		Handler:           api.New(api.Deps{Pool: pool, Enrich: svc, ArtifactDir: artifactDir, Now: time.Now}),
		ReadHeaderTimeout: 10 * time.Second,
		ReadTimeout:       30 * time.Second,
		// Above the API's 10m idempotent-handler deadline so slow handlers can still write their response.
		WriteTimeout: 11 * time.Minute,
		IdleTimeout:  120 * time.Second,
	}
	go sweep(ctx, pool)
	errc := make(chan error, 1)
	go func() { errc <- srv.ListenAndServe() }()
	log.Printf("core listening on :%s", port)
	select {
	case err := <-errc:
		return err
	case <-ctx.Done():
	}
	shutdownCtx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	if err := srv.Shutdown(shutdownCtx); err != nil {
		return err
	}
	return nil
}

// sweep expires due credit lots and refunds stale reservations at startup, then every minute until ctx ends.
// Errors are logged; the loop keeps running.
func sweep(ctx context.Context, pool *pgxpool.Pool) {
	ticker := time.NewTicker(time.Minute)
	defer ticker.Stop()
	for {
		sweepOnce(ctx, pool)
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
		}
	}
}

func sweepOnce(ctx context.Context, pool *pgxpool.Pool) {
	now := time.Now()
	if _, err := ledger.ExpireDue(ctx, pool, now); err != nil {
		log.Printf("expire credits: %v", err)
	}
	if _, err := ledger.ReleaseStale(ctx, pool, 15*time.Minute, now); err != nil {
		log.Printf("release stale reservations: %v", err)
	}
}
