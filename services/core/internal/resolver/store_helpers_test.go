package resolver

import (
	"context"
	"encoding/json"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	"rowan.ai/rowan/services/core/internal/db"
	"rowan.ai/rowan/services/core/internal/quality"
)

var resolverNow = time.Date(2026, 11, 20, 12, 0, 0, 0, time.UTC)

func seedWorkspace(t *testing.T, pool *pgxpool.Pool) string {
	t.Helper()
	return seedID(t, pool, `INSERT INTO workspaces(name,tier) VALUES ('resolver','solo') RETURNING id::text`)
}

func seedID(t *testing.T, pool *pgxpool.Pool, query string, args ...any) string {
	t.Helper()
	var id string
	if err := pool.QueryRow(context.Background(), query, args...).Scan(&id); err != nil {
		t.Fatal(err)
	}
	return id
}

func execSQL(t *testing.T, pool *pgxpool.Pool, query string, args ...any) {
	t.Helper()
	if _, err := pool.Exec(context.Background(), query, args...); err != nil {
		t.Fatal(err)
	}
}

func countSQL(t *testing.T, pool *pgxpool.Pool, query string, args ...any) int {
	t.Helper()
	var n int
	if err := pool.QueryRow(context.Background(), query, args...).Scan(&n); err != nil {
		t.Fatal(err)
	}
	return n
}

func recordInput(workspace, ref string, facts RecordFacts) SourceRecordInput {
	return SourceRecordInput{WorkspaceID: workspace, SourceSystem: "filing", ExternalRef: ref,
		Facts: facts, FetchedAt: resolverNow}
}

func stateCitation(status quality.Status) quality.Citation {
	return quality.Citation{FieldName: "state", Value: json.RawMessage(`"TX"`),
		Origin: quality.RawSource, Status: status, ExportPolicy: quality.DisplayOnly,
		SourceSystem: "filing", SourceRef: "snapshot", TableName: "filings", RowKey: "row",
		PageOrSpan: "page 1", RetrievedAt: resolverNow, SourceRecordID: "ignored-input-id"}
}

// ingest is shared with split/unmerge tests: Resolve never commits the caller's transaction.
func ingest(t *testing.T, pool *pgxpool.Pool, in SourceRecordInput) ResolveResult {
	t.Helper()
	var result ResolveResult
	err := db.InTx(context.Background(), pool, func(tx pgx.Tx) error {
		var err error
		result, err = Resolve(context.Background(), tx, in, resolverNow)
		return err
	})
	if err != nil {
		t.Fatal(err)
	}
	return result
}

func review(t *testing.T, pool *pgxpool.Pool, id, action string, now time.Time) error {
	t.Helper()
	return db.InTx(context.Background(), pool, func(tx pgx.Tx) error {
		return ResolveReview(context.Background(), tx, id, action, "reviewer", now)
	})
}

func seedReview(t *testing.T, pool *pgxpool.Pool, workspace string) (ResolveResult, ResolveResult, SourceRecordInput) {
	t.Helper()
	first := ingest(t, pool, recordInput(workspace, "first", RecordFacts{LegalName: "Acme LLC", State: "TX"}))
	in := recordInput(workspace, "second", RecordFacts{LegalName: "Other Corp", DBA: "ACME", State: "tx"})
	in.Citations = []quality.Citation{stateCitation(quality.ScrapedWeb)}
	return first, ingest(t, pool, in), in
}
