package db

import (
	"context"
	"errors"
	"fmt"
	"io/fs"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"

	"rowan.ai/rowan/migrations"
)

// Open creates a pool and verifies that the database is reachable.
func Open(ctx context.Context, url string) (*pgxpool.Pool, error) {
	pool, err := pgxpool.New(ctx, url)
	if err != nil {
		return nil, fmt.Errorf("create database pool: %w", err)
	}
	if err := pool.Ping(ctx); err != nil {
		pool.Close()
		return nil, fmt.Errorf("ping database: %w", err)
	}
	return pool, nil
}

// Migrate applies unapplied SQL files in lexical order, each in its own transaction.
func Migrate(ctx context.Context, pool *pgxpool.Pool) error {
	files, err := fs.Glob(migrations.FS, "*.sql")
	if err != nil {
		return fmt.Errorf("list migrations: %w", err)
	}
	for _, file := range files {
		if err := InTx(ctx, pool, func(tx pgx.Tx) error {
			// Serialize ledger creation and migration checks across all callers.
			if _, err := tx.Exec(ctx, "SELECT pg_advisory_xact_lock(726692413001)"); err != nil {
				return fmt.Errorf("lock migrations: %w", err)
			}
			if _, err := tx.Exec(ctx, `CREATE TABLE IF NOT EXISTS schema_migrations (
				version text PRIMARY KEY,
				applied_at timestamptz NOT NULL DEFAULT now()
			)`); err != nil {
				return fmt.Errorf("create migration ledger: %w", err)
			}

			var applied bool
			if err := tx.QueryRow(ctx, "SELECT EXISTS (SELECT 1 FROM schema_migrations WHERE version = $1)", file).Scan(&applied); err != nil {
				return fmt.Errorf("check migration: %w", err)
			}
			if applied {
				return nil
			}
			sql, err := migrations.FS.ReadFile(file)
			if err != nil {
				return fmt.Errorf("read migration: %w", err)
			}
			if _, err := tx.Exec(ctx, string(sql)); err != nil {
				return fmt.Errorf("execute migration: %w", err)
			}
			if _, err := tx.Exec(ctx, "INSERT INTO schema_migrations (version, applied_at) VALUES ($1, now())", file); err != nil {
				return fmt.Errorf("record migration: %w", err)
			}
			return nil
		}); err != nil {
			return fmt.Errorf("migrate %s: %w", file, err)
		}
	}
	return nil
}

// InTx commits on a nil callback error and rolls back on errors or panics.
func InTx(ctx context.Context, pool *pgxpool.Pool, fn func(pgx.Tx) error) (err error) {
	tx, err := pool.Begin(ctx)
	if err != nil {
		return fmt.Errorf("begin transaction: %w", err)
	}
	defer func() {
		// Cancellation must not prevent cleanup of an open transaction.
		cleanupCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), 10*time.Second)
		defer cancel()
		if rollbackErr := tx.Rollback(cleanupCtx); rollbackErr != nil && !errors.Is(rollbackErr, pgx.ErrTxClosed) {
			err = errors.Join(err, fmt.Errorf("rollback transaction: %w", rollbackErr))
		}
	}()
	if err := fn(tx); err != nil {
		return fmt.Errorf("transaction callback: %w", err)
	}
	if err := tx.Commit(ctx); err != nil {
		return fmt.Errorf("commit transaction: %w", err)
	}
	return nil
}
