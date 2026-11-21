package registry

import (
	"context"
	"fmt"
	"path/filepath"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
	"golang.org/x/time/rate"

	"rowan.ai/rowan/services/core/internal/enrich"
	"rowan.ai/rowan/services/core/internal/ledger"
	"rowan.ai/rowan/services/core/internal/quality"
	"rowan.ai/rowan/services/core/internal/testdb"
)

var _ enrich.Provider = (*Provider)(nil)

var providerNow = time.Date(2026, 11, 20, 12, 0, 0, 0, time.UTC)

func providerPool(t *testing.T) *pgxpool.Pool {
	t.Helper()
	pool := testdb.New(t)
	ctx := context.Background()
	td := func(n string) string { return filepath.Join("testdata", n) }
	if _, err := LoadSAM(ctx, pool, td("sam_public_v2.dat"), "s", providerNow); err != nil {
		t.Fatal(err)
	}
	if _, err := LoadSunbizCorp(ctx, pool, td("cordata_q1.txt"), "s", providerNow); err != nil {
		t.Fatal(err)
	}
	for _, f := range []string{"dbpr_construction.csv", "dbpr_electrical.csv"} {
		if _, err := LoadDBPR(ctx, pool, td(f), "s", providerNow); err != nil {
			t.Fatal(err)
		}
	}
	return pool
}

func bySource(t *testing.T, recs []string) map[string]int {
	t.Helper()
	m := map[string]int{}
	for _, s := range recs {
		m[s]++
	}
	return m
}

func lookupSources(t *testing.T, p *Provider, c enrich.Candidate) map[string]int {
	t.Helper()
	recs, err := p.Lookup(context.Background(), c)
	if err != nil {
		t.Fatal(err)
	}
	var s []string
	for _, r := range recs {
		s = append(s, r.SourceSystem)
	}
	return bySource(t, s)
}

func TestLookupByID(t *testing.T) {
	p := &Provider{Pool: providerPool(t), MaxPerSource: 3}
	recs, err := p.Lookup(context.Background(), enrich.Candidate{Ref: "c", IDs: map[string]string{"UEI": "abcdef123456"}})
	if err != nil || len(recs) != 1 || recs[0].SourceSystem != "sam" {
		t.Fatalf("recs=%v err=%v", recs, err)
	}
	if len(recs[0].Citations) == 0 || len(recs[0].Payload) == 0 {
		t.Fatal("missing citations or payload")
	}
	for _, c := range recs[0].Citations {
		if c.Status != quality.VerifiedFiling || c.ExportPolicy != quality.Allow || c.TableName != "sam_public_v2" || c.SourceRef != "sam_public_v2.dat" {
			t.Fatalf("citation %+v", c)
		}
	}
}

func TestLookupByNameAcrossSources(t *testing.T) {
	p := &Provider{Pool: providerPool(t), MaxPerSource: 3}
	got := lookupSources(t, p, enrich.Candidate{Ref: "c", LegalName: "Acme HVAC, LLC", State: "fl"})
	if got["sam"] != 1 || got["fl_sunbiz"] != 1 || got["fl_dbpr"] != 1 {
		t.Fatalf("got %v", got)
	}
}

func TestLookupStateFilter(t *testing.T) {
	p := &Provider{Pool: providerPool(t), MaxPerSource: 3}
	if got := lookupSources(t, p, enrich.Candidate{Ref: "c", LegalName: "Acme HVAC, LLC", State: "TX"}); len(got) != 0 {
		t.Fatalf("got %v", got)
	}
}

func TestLookupCapsPerSource(t *testing.T) {
	pool := providerPool(t)
	for i := 0; i < 10; i++ {
		if _, err := pool.Exec(context.Background(), `INSERT INTO registry_entities
			(source_system, external_ref, legal_name, names, state, entity, content_hash, file_name, loaded_at)
			VALUES ('fl_dbpr', $1, 'ABC PLUMBING', ARRAY['abc plumbing'], 'FL',
			jsonb_build_object('external_ref', $1::text, 'facts', jsonb_build_object('LegalName','ABC PLUMBING')), '\x00', 'x.csv', now())`,
			fmt.Sprintf("R%02d", 9-i)); err != nil {
			t.Fatal(err)
		}
	}
	recs, err := (&Provider{Pool: pool, MaxPerSource: 3}).Lookup(context.Background(), enrich.Candidate{Ref: "c", LegalName: "ABC Plumbing"})
	if err != nil || len(recs) != 3 {
		t.Fatalf("recs=%d err=%v", len(recs), err)
	}
	for i, want := range []string{"R00", "R01", "R02"} {
		if recs[i].ExternalRef != want {
			t.Fatalf("recs[%d]=%s want %s", i, recs[i].ExternalRef, want)
		}
	}
}

func TestLookupZeroHit(t *testing.T) {
	p := &Provider{Pool: providerPool(t), MaxPerSource: 3}
	for _, c := range []enrich.Candidate{{Ref: "c", LegalName: "Nobody Here Inc"}, {Ref: "d"}} {
		recs, err := p.Lookup(context.Background(), c)
		if err != nil || len(recs) != 0 {
			t.Fatalf("recs=%v err=%v", recs, err)
		}
	}
}

func TestRegistryEnrichAutoMerge(t *testing.T) {
	pool := providerPool(t)
	ctx := context.Background()
	var ws string
	if err := pool.QueryRow(ctx, `INSERT INTO workspaces(name,tier) VALUES ('reg',$1) RETURNING id::text`, string(ledger.Solo)).Scan(&ws); err != nil {
		t.Fatal(err)
	}
	if err := ledger.GrantTopup(ctx, pool, ws, 10, providerNow); err != nil {
		t.Fatal(err)
	}
	svc := &enrich.Service{Pool: pool, Provider: &Provider{Pool: pool, MaxPerSource: 3},
		Limiter: rate.NewLimiter(rate.Inf, 1), CacheTTL: time.Hour, Now: func() time.Time { return providerNow }}
	res, err := svc.Run(ctx, ws, "", "k1", "", []enrich.Candidate{{Ref: "acme", LegalName: "Acme HVAC LLC", State: "FL"}})
	if err != nil {
		t.Fatal(err)
	}
	var companies, links int
	var committed int64
	if err := pool.QueryRow(ctx, `SELECT (SELECT count(*) FROM canonical_companies WHERE workspace_id=$1),
		(SELECT count(*) FROM entity_merge_links l JOIN canonical_companies c ON c.id=l.canonical_company_id
		  WHERE c.workspace_id=$1 AND l.status='active'),
		(SELECT COALESCE(sum(committed_amount),0) FROM credit_reservations WHERE workspace_id=$1)`, ws).Scan(&companies, &links, &committed); err != nil {
		t.Fatal(err)
	}
	if companies != 1 || links != 3 || committed != 1 {
		rows, _ := pool.Query(ctx, `SELECT merge_confidence_score, match_reasons, merge_mode FROM entity_merge_links`)
		for rows.Next() {
			var s float64
			var r []string
			var m string
			_ = rows.Scan(&s, &r, &m)
			t.Logf("link %.3f %v %s", s, r, m)
		}
		t.Fatalf("companies=%d links=%d committed=%d res=%+v", companies, links, committed, res)
	}
	rows, _ := pool.Query(ctx, `SELECT merge_confidence_score, match_reasons FROM entity_merge_links ORDER BY created_at`)
	for rows.Next() {
		var s float64
		var r []string
		_ = rows.Scan(&s, &r)
		t.Logf("link %.3f %v", s, r)
	}
}
