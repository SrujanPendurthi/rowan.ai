package api

import (
	"context"
	"testing"

	"golang.org/x/time/rate"

	"rowan.ai/rowan/services/core/internal/enrich"
	"rowan.ai/rowan/services/core/internal/ledger"
	"rowan.ai/rowan/services/core/internal/registry"
)

func TestConnectorsSourcingLoop(t *testing.T) {
	ctx := context.Background()
	e := newEnv(t)
	td := "../registry/testdata/"
	if _, err := registry.LoadSAM(ctx, e.pool, td+"sam_public_v2.dat", "sha256:sam", testNow); err != nil {
		t.Fatal(err)
	}
	if _, err := registry.LoadSunbizCorp(ctx, e.pool, td+"cordata_q1.txt", "sha256:cor", testNow); err != nil {
		t.Fatal(err)
	}
	if _, err := registry.LoadSunbizEvents(ctx, e.pool, td+"corevent.txt", "sha256:evt", testNow); err != nil {
		t.Fatal(err)
	}
	for _, f := range []string{"dbpr_construction.csv", "dbpr_electrical.csv"} {
		if _, err := registry.LoadDBPR(ctx, e.pool, td+f, "sha256:"+f, testNow); err != nil {
			t.Fatal(err)
		}
	}
	svc := &enrich.Service{Pool: e.pool, Provider: &registry.Provider{Pool: e.pool, MaxPerSource: 3},
		Limiter: rate.NewLimiter(rate.Inf, 1), CacheTTL: 30 * 24 * 3600 * 1e9, Now: e.clock}
	e.h = New(Deps{Pool: e.pool, Enrich: svc, ArtifactDir: e.dir, Now: e.clock})

	ws, _, token := e.tenant(ledger.Solo)
	rec := e.run(token, "conn-1", []cand{{"ref": "c1", "legal_name": "Acme HVAC LLC", "state": "FL"}}, "")
	wantStatus(t, rec, 200)
	res := decode(t, rec)
	if res["credits_committed"] != 1.0 || res["hits"] != 1.0 {
		t.Fatalf("run = %v", res)
	}
	if e.available(ws) != 499 {
		t.Fatalf("available = %d", e.available(ws))
	}
	companies, _ := res["companies"].([]any)
	if len(companies) != 1 {
		t.Fatalf("companies = %v", companies)
	}
	rec = e.do("GET", "/v1/companies/"+companies[0].(string), token, "", nil)
	wantStatus(t, rec, 200)
	company := decode(t, rec)

	systems := map[string]bool{}
	for _, c := range sub[[]any](t, company, "citations") {
		m := c.(map[string]any)
		systems[m["source_system"].(string)] = true
		if m["data_quality_status"] != "verified_filing" {
			t.Fatalf("citation not verified_filing: %v", m)
		}
	}
	for _, s := range []string{"sam", "fl_sunbiz", "fl_dbpr"} {
		if !systems[s] {
			t.Fatalf("no %s citation; got %v", s, systems)
		}
	}
	proxy := sub[map[string]any](t, company, "proxy_summary")
	sigs, _ := proxy["signals"].([]any)
	if proxy["label"] != "post_sale_proxy" || len(sigs) != 1 {
		t.Fatalf("proxy = %v", proxy)
	}
	sig := sigs[0].(map[string]any)
	if sig["type"] != "registered_agent_change" || sig["citation"].(map[string]any)["table_name"] != "corevent" {
		t.Fatalf("signal = %v", sig)
	}
}
