package testdb

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"os"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"

	"rowan.ai/rowan/services/core/internal/db"
)

// New creates and migrates an isolated database, then drops it after the test.
func New(t *testing.T) *pgxpool.Pool {
	t.Helper()
	url := os.Getenv("ROWAN_TEST_DATABASE_URL")
	if url == "" {
		if testing.Short() {
			t.Skip("ROWAN_TEST_DATABASE_URL unset; run make db-up")
		}
		t.Fatal("ROWAN_TEST_DATABASE_URL unset; run make db-up")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	admin, err := db.Open(ctx, url)
	if err != nil {
		t.Fatalf("open test database admin pool: %v", err)
	}
	t.Cleanup(admin.Close)
	var random [16]byte
	if _, err := rand.Read(random[:]); err != nil {
		t.Fatalf("generate test database name: %v", err)
	}
	name := "rowan_test_" + hex.EncodeToString(random[:])
	// PostgreSQL identifiers cannot be bind parameters; quote the generated name.
	identifier := pgx.Identifier{name}.Sanitize()
	if _, err := admin.Exec(ctx, "CREATE DATABASE "+identifier); err != nil {
		t.Fatalf("create test database: %v", err)
	}
	t.Cleanup(func() {
		cleanupCtx, cleanupCancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cleanupCancel()
		if _, err := admin.Exec(cleanupCtx, "DROP DATABASE "+identifier+" WITH (FORCE)"); err != nil {
			t.Errorf("drop test database: %v", err)
		}
	})
	config := admin.Config()
	config.ConnConfig.Database = name
	pool, err := pgxpool.NewWithConfig(ctx, config)
	if err != nil {
		t.Fatalf("open isolated test database pool: %v", err)
	}
	t.Cleanup(pool.Close)
	if err := db.Migrate(ctx, pool); err != nil {
		t.Fatalf("migrate test database: %v", err)
	}
	return pool
}
