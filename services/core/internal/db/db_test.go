package db_test

import (
	"context"
	"errors"
	"io/fs"
	"sync"
	"testing"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"

	"rowan.ai/rowan/migrations"
	"rowan.ai/rowan/services/core/internal/db"
	"rowan.ai/rowan/services/core/internal/testdb"
)

func TestMigrateIdempotent(t *testing.T) {
	pool := testdb.New(t)
	ctx := context.Background()
	if err := db.Migrate(ctx, pool); err != nil {
		t.Fatalf("migrate again: %v", err)
	}
	files, err := fs.Glob(migrations.FS, "*.sql")
	if err != nil {
		t.Fatal(err)
	}
	var count int
	if err := pool.QueryRow(ctx, "SELECT count(*) FROM schema_migrations").Scan(&count); err != nil {
		t.Fatal(err)
	}
	if count != len(files) {
		t.Fatalf("migration count = %d, want %d", count, len(files))
	}
}

func TestInTxRollsBackOnError(t *testing.T) {
	pool := singleConnectionPool(t)
	ctx := context.Background()
	if _, err := pool.Exec(ctx, "CREATE TEMP TABLE tx_test (value integer)"); err != nil {
		t.Fatal(err)
	}
	boom := errors.New("boom")
	err := db.InTx(ctx, pool, func(tx pgx.Tx) error {
		if _, err := tx.Exec(ctx, "INSERT INTO tx_test (value) VALUES ($1)", 1); err != nil {
			return err
		}
		return boom
	})
	if !errors.Is(err, boom) {
		t.Fatalf("InTx error = %v, want wrapped boom", err)
	}
	var count int
	if err := pool.QueryRow(ctx, "SELECT count(*) FROM tx_test").Scan(&count); err != nil {
		t.Fatal(err)
	}
	if count != 0 {
		t.Fatalf("row count = %d, want 0", count)
	}
}

func TestInTxCommits(t *testing.T) {
	pool := testdb.New(t)
	ctx := context.Background()
	if err := db.InTx(ctx, pool, func(tx pgx.Tx) error {
		_, err := tx.Exec(ctx, "CREATE TABLE committed (value integer)")
		return err
	}); err != nil {
		t.Fatal(err)
	}
	if _, err := pool.Exec(ctx, "SELECT value FROM committed"); err != nil {
		t.Fatalf("committed table unavailable: %v", err)
	}
}

// A temporary table belongs to one session, so keep these operations on one connection.
func singleConnectionPool(t *testing.T) *pgxpool.Pool {
	t.Helper()
	config := testdb.New(t).Config()
	config.MaxConns = 1
	pool, err := pgxpool.NewWithConfig(context.Background(), config)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(pool.Close)
	return pool
}

func TestMigrateConcurrent(t *testing.T) {
	pool := testdb.New(t)
	ctx := context.Background()
	// Exercise both migration ledger creation and unapplied migrations concurrently.
	if _, err := pool.Exec(ctx, "DROP SCHEMA public CASCADE; CREATE SCHEMA public"); err != nil {
		t.Fatal(err)
	}
	var wg sync.WaitGroup
	start := make(chan struct{})
	errs := make(chan error, 8)
	for range 8 {
		wg.Go(func() { <-start; errs <- db.Migrate(ctx, pool) })
	}
	close(start)
	wg.Wait()
	close(errs)
	for err := range errs {
		if err != nil {
			t.Errorf("concurrent migrate: %v", err)
		}
	}
	files, err := fs.Glob(migrations.FS, "*.sql")
	if err != nil {
		t.Fatal(err)
	}
	var count int
	if err := pool.QueryRow(ctx, "SELECT count(*) FROM schema_migrations").Scan(&count); err != nil {
		t.Fatal(err)
	}
	if count != len(files) {
		t.Fatalf("migration count = %d, want %d", count, len(files))
	}
}
