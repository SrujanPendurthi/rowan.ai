package proxy_test

import (
	"context"
	"encoding/json"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	"rowan.ai/rowan/services/core/internal/db"
	"rowan.ai/rowan/services/core/internal/proxy"
	"rowan.ai/rowan/services/core/internal/resolver"
	"rowan.ai/rowan/services/core/internal/testdb"
)

var now = time.Date(2026, 11, 20, 12, 0, 0, 0, time.UTC)

const ref = "P26000012345"

// seed links one fl_sunbiz record to a company and stores an officer_change signal observed at observed.
func seed(t *testing.T, observed time.Time) (*pgxpool.Pool, string) {
	t.Helper()
	pool := testdb.New(t)
	ctx := context.Background()
	var ws string
	if err := pool.QueryRow(ctx, `INSERT INTO workspaces(name,tier) VALUES ('p','solo') RETURNING id::text`).Scan(&ws); err != nil {
		t.Fatal(err)
	}
	var company string
	err := db.InTx(ctx, pool, func(tx pgx.Tx) error {
		r, err := resolver.Resolve(ctx, tx, resolver.SourceRecordInput{WorkspaceID: ws, SourceSystem: "fl_sunbiz",
			ExternalRef: ref, Facts: resolver.RecordFacts{LegalName: "Acme HVAC LLC"}, FetchedAt: now}, now)
		company = r.CanonicalID
		return err
	})
	if err != nil {
		t.Fatal(err)
	}
	cit := `{"field_name":"officer_change","field_value":"\"x\"","origin_type":"raw_source","data_quality_status":"verified_filing","export_policy":"display_only","source_system":"fl_sunbiz","source_ref":"` + ref + `","retrieved_at":"2026-01-01T00:00:00Z"}`
	if _, err := pool.Exec(ctx, `INSERT INTO registry_signals(source_system,external_ref,signal_type,observed_at,citation,dedupe_key)
		VALUES ('fl_sunbiz',$1,'officer_change',$2,$3,'k')`, ref, observed, cit); err != nil {
		t.Fatal(err)
	}
	return pool, company
}

func recompute(t *testing.T, pool *pgxpool.Pool, company string, at time.Time) proxy.Summary {
	t.Helper()
	var s proxy.Summary
	err := db.InTx(context.Background(), pool, func(tx pgx.Tx) error {
		var err error
		s, err = proxy.Recompute(context.Background(), tx, company, at)
		return err
	})
	if err != nil {
		t.Fatal(err)
	}
	return s
}

func stored(t *testing.T, pool *pgxpool.Pool, company string) proxy.Summary {
	t.Helper()
	var raw []byte
	if err := pool.QueryRow(context.Background(), `SELECT proxy_summary FROM canonical_companies WHERE id=$1`, company).Scan(&raw); err != nil {
		t.Fatal(err)
	}
	var s proxy.Summary
	if err := json.Unmarshal(raw, &s); err != nil {
		t.Fatalf("stored summary %q: %v", raw, err)
	}
	return s
}

func TestRecomputeInWindow(t *testing.T) {
	pool, c := seed(t, now.AddDate(0, -7, 0))
	got := recompute(t, pool, c, now)
	if got.Label != proxy.LabelPostSaleProxy || got.SignalCount != 1 {
		t.Fatalf("got %+v", got)
	}
	if s := stored(t, pool, c); s.Label != got.Label || s.SignalCount != 1 || s.Signals[0].Citation.SourceSystem != "fl_sunbiz" {
		t.Fatalf("stored %+v", s)
	}
}

func TestRecomputeOutOfWindow(t *testing.T) {
	pool, c := seed(t, now.AddDate(0, -11, 0))
	if got := recompute(t, pool, c, now); got.Label != proxy.LabelNone {
		t.Fatalf("got %+v", got)
	}
	if s := stored(t, pool, c); s.Label != proxy.LabelNone {
		t.Fatalf("stored %+v", s)
	}
}

func TestRecomputeIgnoresSplitLinks(t *testing.T) {
	pool, c := seed(t, now.AddDate(0, -7, 0))
	if _, err := pool.Exec(context.Background(), `UPDATE entity_merge_links SET status='split'`); err != nil {
		t.Fatal(err)
	}
	if got := recompute(t, pool, c, now); got.SignalCount != 0 || got.Label != proxy.LabelNone {
		t.Fatalf("got %+v", got)
	}
}

func TestRecomputeMissingCompanyNoOp(t *testing.T) {
	pool := testdb.New(t)
	recompute(t, pool, "00000000-0000-0000-0000-000000000000", now)
}

func TestRecomputeAllSlidesWindow(t *testing.T) {
	pool, c := seed(t, now.AddDate(0, -5, 0))
	ctx := context.Background()
	n, err := proxy.RecomputeAll(ctx, pool, now)
	if err != nil || n != 1 {
		t.Fatalf("n=%d err=%v", n, err)
	}
	if s := stored(t, pool, c); s.Label != proxy.LabelNone {
		t.Fatalf("stored %+v", s)
	}
	if _, err := proxy.RecomputeAll(ctx, pool, now.AddDate(0, 2, 0)); err != nil {
		t.Fatal(err)
	}
	if s := stored(t, pool, c); s.Label != proxy.LabelPostSaleProxy {
		t.Fatalf("stored %+v", s)
	}
}

func TestRecomputeForRefs(t *testing.T) {
	pool, c := seed(t, now.AddDate(0, -7, 0))
	ctx := context.Background()
	if n, err := proxy.RecomputeForRefs(ctx, pool, "fl_sunbiz", []string{"other"}, now); err != nil || n != 0 {
		t.Fatalf("n=%d err=%v", n, err)
	}
	if n, err := proxy.RecomputeForRefs(ctx, pool, "fl_sunbiz", []string{ref}, now); err != nil || n != 1 {
		t.Fatalf("n=%d err=%v", n, err)
	}
	if s := stored(t, pool, c); s.Label != proxy.LabelPostSaleProxy {
		t.Fatalf("stored %+v", s)
	}
}
