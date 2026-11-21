package resolver

import (
	"context"
	"encoding/json"
	"testing"

	"github.com/jackc/pgx/v5/pgxpool"
	"rowan.ai/rowan/services/core/internal/quality"
	"rowan.ai/rowan/services/core/internal/testdb"
)

const sunbizCitation = `{"field_name":"officer_change","field_value":"\"x\"","origin_type":"raw_source","data_quality_status":"verified_filing","export_policy":"display_only","source_system":"fl_sunbiz","source_ref":"P1","retrieved_at":"2026-01-01T00:00:00Z"}`

func insertSunbizSignal(t *testing.T, pool *pgxpool.Pool) {
	t.Helper()
	execSQL(t, pool, `INSERT INTO registry_signals(source_system,external_ref,signal_type,observed_at,citation,dedupe_key)
		VALUES ('fl_sunbiz','P1','officer_change',$1,$2,'k')`, resolverNow.AddDate(0, -7, 0), sunbizCitation)
}

func proxyLabel(t *testing.T, pool *pgxpool.Pool, id string) string {
	t.Helper()
	var raw []byte
	if err := pool.QueryRow(context.Background(), `SELECT proxy_summary FROM canonical_companies WHERE id=$1`, id).Scan(&raw); err != nil {
		t.Fatal(err)
	}
	var s struct{ Label string }
	if err := json.Unmarshal(raw, &s); err != nil {
		t.Fatalf("proxy_summary %q: %v", raw, err)
	}
	return s.Label
}

func TestResolveRecomputesProxy(t *testing.T) {
	pool := testdb.New(t)
	w := seedWorkspace(t, pool)
	insertSunbizSignal(t, pool)
	in := recordInput(w, "P1", RecordFacts{LegalName: "Acme HVAC LLC"})
	in.SourceSystem = "fl_sunbiz"
	r := ingest(t, pool, in)
	if got := proxyLabel(t, pool, r.CanonicalID); got != "post_sale_proxy" {
		t.Fatalf("label=%q", got)
	}
}

func TestSplitRecomputesProxy(t *testing.T) {
	pool := testdb.New(t)
	w := seedWorkspace(t, pool)
	insertSunbizSignal(t, pool)
	sb := recordInput(w, "P1", RecordFacts{LegalName: "Acme HVAC LLC", IDs: map[string]string{"ein": "1"}})
	sb.SourceSystem = "fl_sunbiz"
	a := ingest(t, pool, sb)
	dbpr := recordInput(w, "D1", RecordFacts{LegalName: "Acme HVAC LLC", IDs: map[string]string{"ein": "1"}})
	dbpr.SourceSystem = "fl_dbpr"
	b := ingest(t, pool, dbpr)
	if a.CanonicalID != b.CanonicalID {
		t.Fatalf("expected one canonical: %+v %+v", a, b)
	}
	if got := proxyLabel(t, pool, a.CanonicalID); got != "post_sale_proxy" {
		t.Fatalf("before split label=%q", got)
	}
	newID, err := split(t, pool, a.CanonicalID, []string{a.SourceRecordID})
	if err != nil {
		t.Fatal(err)
	}
	if got := proxyLabel(t, pool, a.CanonicalID); got != "none" {
		t.Fatalf("original label=%q", got)
	}
	if got := proxyLabel(t, pool, newID); got != "post_sale_proxy" {
		t.Fatalf("new label=%q", got)
	}
}

func TestResolveReviewMergeRecomputesProxy(t *testing.T) {
	pool := testdb.New(t)
	w := seedWorkspace(t, pool)
	first := ingest(t, pool, recordInput(w, "first", RecordFacts{LegalName: "Acme LLC", State: "TX"}))
	execSQL(t, pool, `INSERT INTO registry_signals(source_system,external_ref,signal_type,observed_at,citation,dedupe_key)
		VALUES ('filing','second','officer_change',$1,$2,'k')`, resolverNow.AddDate(0, -7, 0), sunbizCitation)
	in := recordInput(w, "second", RecordFacts{LegalName: "Other Corp", DBA: "ACME", State: "tx"})
	in.Citations = []quality.Citation{stateCitation(quality.ScrapedWeb)}
	second := ingest(t, pool, in)
	if second.Decision != Review || second.CanonicalID == first.CanonicalID {
		t.Fatalf("expected review band: %+v", second)
	}
	if got := proxyLabel(t, pool, first.CanonicalID); got != "none" {
		t.Fatalf("target before merge label=%q", got)
	}
	if err := review(t, pool, second.ReviewCandidateID, "merge", resolverNow); err != nil {
		t.Fatal(err)
	}
	if got := proxyLabel(t, pool, first.CanonicalID); got != "post_sale_proxy" {
		t.Fatalf("survivor label=%q", got)
	}
	if n := countSQL(t, pool, `SELECT count(*) FROM canonical_companies WHERE id=$1`, second.CanonicalID); n != 0 {
		t.Fatal("merged canonical not deleted")
	}
}
