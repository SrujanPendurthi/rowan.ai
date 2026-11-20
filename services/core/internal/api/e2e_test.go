package api

import (
	"fmt"
	"strings"
	"sync"
	"testing"
	"time"

	"rowan.ai/rowan/services/core/internal/ledger"
	"rowan.ai/rowan/services/core/internal/quality"
	"rowan.ai/rowan/services/core/internal/resolver"
)

func ohCitations(rev string, revStatus quality.Status) []quality.Citation {
	return []quality.Citation{
		citation("state", `"OH"`, quality.VerifiedFiling, quality.Allow),
		citation("naics", `"238220"`, quality.VerifiedFiling, quality.Allow),
		citation("revenue_cents", rev, revStatus, quality.DealTeamPlus),
	}
}

func sub[T any](t *testing.T, m map[string]any, key string) T {
	t.Helper()
	v, ok := m[key].(T)
	if !ok {
		t.Fatalf("%s has wrong type in %v", key, m)
	}
	return v
}

// evalStatuses maps display name -> criterion -> "status/data_quality_status".
func evalStatuses(t *testing.T, e *env, token, bb string) map[string]map[string]string {
	t.Helper()
	rec := e.do("GET", "/v1/buy-boxes/"+bb+"/evaluations", token, "", nil)
	wantStatus(t, rec, 200)
	out := map[string]map[string]string{}
	for _, item := range sub[[]any](t, decode(t, rec), "evaluations") {
		m := item.(map[string]any)
		crit := map[string]string{}
		for _, b := range m["criteria_breakdown"].([]any) {
			bm := b.(map[string]any)
			crit[bm["criterion"].(string)] = bm["status"].(string) + "/" + bm["data_quality_status"].(string)
		}
		out[m["display_name"].(string)+"|"+m["id"].(string)] = crit
	}
	return out
}

func findEval(t *testing.T, all map[string]map[string]string, name string) (id string, crit map[string]string) {
	t.Helper()
	for k, v := range all {
		if n, i, _ := strings.Cut(k, "|"); n == name {
			return i, v
		}
	}
	t.Fatalf("no evaluation for %q in %v", name, all)
	return "", nil
}

// PRD §7.1: quote, run, merge/review, evaluate, reject, split, redacted export.
func TestE2ESourcingLoop(t *testing.T) {
	e := newEnv(t)
	ws := e.workspace(ledger.Solo)
	e.fund(ws)
	keyID, token := e.key(ws, 1_000_000, 1000)

	rec := e.do("POST", "/v1/buy-boxes", token, "bb-1", cand{"name": "OH/PA HVAC", "thesis_notes": "n",
		"criteria": cand{"revenue_min_cents": 200000000, "states_in": []string{"OH", "PA"}, "naics_in": []string{"238220"}}})
	wantStatus(t, rec, 201)
	bb := str(t, decode(t, rec), "id")

	cs := []cand{
		{"ref": "cand-1", "legal_name": "Zephyr Plumbing LLC", "state": "OH"},
		{"ref": "cand-2", "legal_name": "Other Corp", "state": "OH"},
		{"ref": "cand-3", "legal_name": "Quartz Dental Group", "state": "OH"},
		{"ref": "cand-4", "legal_name": "Orion Logistics Corp", "state": "OH"},
	}
	uei := map[string]string{"uei": "UEI-ZEPHYR-1"}
	e.provider.Results["cand-1"] = []resolver.SourceRecordInput{
		{SourceSystem: "sam", ExternalRef: "sam-1", FetchedAt: testNow,
			Facts:     resolver.RecordFacts{LegalName: "Zephyr Plumbing LLC", State: "OH", IDs: uei},
			Citations: ohCitations(`250000000`, quality.VerifiedFiling)},
		{SourceSystem: "irs", ExternalRef: "irs-1", FetchedAt: testNow,
			Facts:     resolver.RecordFacts{LegalName: "Zephyr Plumbing, L.L.C.", State: "OH", IDs: uei},
			Citations: []quality.Citation{citation("state", `"OH"`, quality.VerifiedFiling, quality.Allow)}},
	}
	e.provider.Results["cand-2"] = []resolver.SourceRecordInput{{SourceSystem: "sos", ExternalRef: "sos-2", FetchedAt: testNow,
		Facts:     resolver.RecordFacts{LegalName: "Other Corp", DBA: "Zephyr Plumbing LLC", State: "OH"},
		Citations: []quality.Citation{citation("state", `"OH"`, quality.VerifiedFiling, quality.Allow)}}}
	e.provider.Results["cand-3"] = []resolver.SourceRecordInput{{SourceSystem: "vendor", ExternalRef: "v-3", FetchedAt: testNow,
		Facts:     resolver.RecordFacts{LegalName: "Quartz Dental Group", State: "OH"},
		Citations: ohCitations(`300000000`, quality.CommercialEstimate)}}

	// Quote is free.
	rec = e.do("POST", "/v1/enrichment/quote", token, "", cand{"candidates": cs})
	wantStatus(t, rec, 200)
	if q := decode(t, rec); q["credits_required"] != 4.0 || q["billable"] != 4.0 {
		t.Fatalf("quote = %v", q)
	}
	if e.available(ws) != 500 {
		t.Fatal("quote changed credits")
	}
	if n := scalar[int](t, e.pool, `SELECT count(*) FROM credit_reservations`); n != 0 {
		t.Fatalf("quote wrote %d reservations", n)
	}

	// Run: 3 hits committed, zero-hit cand-4 refunded.
	rec = e.run(token, "run-1", cs, bb)
	wantStatus(t, rec, 200)
	res := decode(t, rec)
	if res["credits_committed"] != 3.0 || res["credits_refunded"] != 1.0 || res["hits"] != 3.0 || res["zero_hits"] != 1.0 {
		t.Fatalf("run = %v", res)
	}
	if e.available(ws) != 497 {
		t.Fatalf("available = %d, want 497", e.available(ws))
	}
	rec = e.do("GET", "/v1/credits", token, "", nil)
	wantStatus(t, rec, 200)
	cr := decode(t, rec)
	if cr["total"] != 497.0 || sub[map[string]any](t, cr, "month_to_date")["spent"] != 3.0 {
		t.Fatalf("credits = %v", cr)
	}

	// Evaluations: verified revenue MET; commercial estimate UNVERIFIED_LOW_CONFIDENCE.
	evals := evalStatuses(t, e, token, bb)
	c1, crit1 := findEval(t, evals, "Zephyr Plumbing LLC")
	_, crit3 := findEval(t, evals, "Quartz Dental Group")
	if crit1["revenue_min_cents"] != "MET/verified_filing" || crit1["states_in"] != "MET/verified_filing" || crit1["naics_in"] != "MET/verified_filing" {
		t.Fatalf("cand-1 criteria = %v", crit1)
	}
	if crit3["revenue_min_cents"] != "UNVERIFIED_LOW_CONFIDENCE/commercial_estimate" {
		t.Fatalf("cand-3 criteria = %v", crit3)
	}

	// Review queue holds exactly the DBA candidate; reject it.
	rec = e.do("GET", "/v1/review-queue", token, "", nil)
	wantStatus(t, rec, 200)
	queue := sub[[]any](t, decode(t, rec), "candidates")
	if len(queue) != 1 {
		t.Fatalf("review queue = %v", queue)
	}
	item := queue[0].(map[string]any)
	if s := item["score"].(float64); s < 0.55 || s >= 0.85 {
		t.Fatalf("review score = %v", s)
	}
	rec = e.do("POST", "/v1/review-queue/"+str(t, item, "id")+"/resolve", token, "rej-1", cand{"action": "reject"})
	wantStatus(t, rec, 200)
	if decode(t, rec)["status"] != "rejected" {
		t.Fatalf("resolve = %s", rec.Body.String())
	}
	rec = e.do("GET", "/v1/review-queue", token, "", nil)
	if q := sub[[]any](t, decode(t, rec), "candidates"); len(q) != 0 {
		t.Fatalf("queue not drained: %v", q)
	}

	// Split the record without revenue off cand-1's canonical company.
	rec = e.do("GET", "/v1/companies/"+c1, token, "", nil)
	wantStatus(t, rec, 200)
	company := decode(t, rec)
	links := sub[[]any](t, company, "links")
	if len(links) != 2 {
		t.Fatalf("cand-1 links = %v", links)
	}
	revRecord := ""
	for _, c := range sub[[]any](t, company, "citations") {
		cm := c.(map[string]any)
		if cm["field_name"] == "revenue_cents" {
			revRecord = str(t, cm, "source_record_id")
		}
	}
	var other string
	for _, l := range links {
		if id := str(t, l.(map[string]any), "source_record_id"); id != revRecord {
			other = id
		}
	}
	if revRecord == "" || other == "" {
		t.Fatalf("cannot pick split record: rev=%q other=%q", revRecord, other)
	}
	rec = e.do("POST", "/v1/companies/"+c1+"/split", token, "split-1", cand{"source_record_ids": []string{other}, "reason": "e2e"})
	wantStatus(t, rec, 200)
	newID := str(t, decode(t, rec), "new_canonical_id")
	if newID == c1 {
		t.Fatal("split reused canonical id")
	}
	rec = e.do("GET", "/v1/companies/"+c1, token, "", nil)
	if n := len(sub[[]any](t, decode(t, rec), "links")); n != 1 {
		t.Fatalf("cand-1 links after split = %d", n)
	}

	// Export on solo tier: deal_team_plus revenue redacted, quality column present, audit written.
	rec = e.do("POST", "/v1/exports", token, "exp-1", cand{"buy_box_id": bb, "columns": []string{"revenue_cents", "state"}})
	wantStatus(t, rec, 200)
	if rf := rec.Header().Get("X-Rowan-Redacted-Fields"); !strings.Contains(rf, "revenue_cents") || strings.Contains(rf, "state") {
		t.Fatalf("redacted header = %q", rf)
	}
	csv := rec.Body.String()
	header := strings.SplitN(csv, "\n", 2)[0]
	if !strings.Contains(csv, "[REDACTED]") || strings.Contains(csv, "250000000") ||
		!strings.Contains(header, "revenue_cents__data_quality_status") || !strings.Contains(csv, "verified_filing") {
		t.Fatalf("csv = %s", csv)
	}
	if n := scalar[int](t, e.pool, `SELECT count(*) FROM export_audits WHERE workspace_id=$1 AND user_or_key_id=$2`, ws, "api_key:"+keyID); n != 1 {
		t.Fatalf("export audits = %d", n)
	}
	if err := ledger.CheckInvariants(t.Context(), e.pool, ws); err != nil {
		t.Fatal(err)
	}
}

// PRD §7.2: concurrent retries charge once per key; lots expire FIFO-safely; ledger stays balanced.
func TestE2ELedgerIntegrity(t *testing.T) {
	e := newEnv(t)
	ws, _, token := e.tenant(ledger.Solo)

	const keys, retries = 5, 5
	bodies := make([]cand, keys)
	for k := range keys {
		hit, zero := fmt.Sprintf("k%d-hit", k), fmt.Sprintf("k%d-zero", k)
		e.provider.Results[hit] = []resolver.SourceRecordInput{{SourceSystem: "fake", ExternalRef: hit, FetchedAt: testNow,
			Facts:     resolver.RecordFacts{LegalName: fmt.Sprintf("Hit Company %d LLC", k), State: "OH"},
			Citations: ohCitations(`250000000`, quality.VerifiedFiling)}}
		bodies[k] = cand{"candidates": []cand{
			{"ref": hit, "legal_name": fmt.Sprintf("Hit Company %d LLC", k), "state": "OH"},
			{"ref": zero, "legal_name": fmt.Sprintf("Zero Company %d LLC", k), "state": "OH"},
		}}
	}

	type outcome struct {
		key    int
		status int
		code   string
	}
	results := make(chan outcome, keys*retries)
	var wg sync.WaitGroup
	for k := range keys {
		for range retries {
			wg.Add(1)
			go func() {
				defer wg.Done()
				rec := e.do("POST", "/v1/enrichment/run", token, fmt.Sprintf("ledger-key-%d", k), bodies[k])
				code := ""
				if rec.Code != 200 {
					if m := decode(t, rec); m["error"] != nil {
						code, _ = m["error"].(map[string]any)["code"].(string)
					}
				}
				results <- outcome{k, rec.Code, code}
			}()
		}
	}
	wg.Wait()
	close(results)
	for r := range results {
		if r.status != 200 && !(r.status == 409 && r.code == "request_in_progress") {
			t.Fatalf("key %d: status %d code %q, want 200 or 409 request_in_progress", r.key, r.status, r.code)
		}
	}
	// A settled retry replays the stored result for every key.
	for k := range keys {
		rec := e.do("POST", "/v1/enrichment/run", token, fmt.Sprintf("ledger-key-%d", k), bodies[k])
		wantStatus(t, rec, 200)
		if got := decode(t, rec); got["credits_committed"] != 1.0 || got["credits_refunded"] != 1.0 {
			t.Fatalf("key %d replay = %v", k, got)
		}
	}

	// Exactly one reservation per key, each committed once with the zero-hit refunded.
	if n := scalar[int](t, e.pool, `SELECT count(*) FROM credit_reservations WHERE workspace_id=$1`, ws); n != keys {
		t.Fatalf("reservations = %d, want %d", n, keys)
	}
	if n := scalar[int](t, e.pool, `SELECT count(*) FROM credit_reservations WHERE workspace_id=$1
		AND status='committed' AND amount=2 AND committed_amount=1`, ws); n != keys {
		t.Fatalf("committed reservations = %d, want %d", n, keys)
	}
	if n := scalar[int](t, e.pool, `SELECT count(*) FROM credit_ledger_entries WHERE workspace_id=$1 AND entry_type='refund'`, ws); n == 0 {
		t.Fatal("no refund ledger entries for zero-hit candidates")
	}
	if got := e.available(ws); got != 495 {
		t.Fatalf("available = %d, want 495", got)
	}
	rec := e.do("GET", "/v1/credits", token, "", nil)
	wantStatus(t, rec, 200)
	if cr := decode(t, rec); cr["total"] != 495.0 || sub[map[string]any](t, cr, "month_to_date")["spent"] != 5.0 {
		t.Fatalf("credits after refunds = %v", cr)
	}
	if err := ledger.CheckInvariants(t.Context(), e.pool, ws); err != nil {
		t.Fatal(err)
	}

	// Solo lots last 60 days: advance 61 and sweep.
	e.advance(61 * 24 * time.Hour)
	expired, err := ledger.ExpireDue(t.Context(), e.pool, e.clock())
	if err != nil {
		t.Fatal(err)
	}
	if expired != 495 {
		t.Fatalf("expired = %d, want 495", expired)
	}
	if n := scalar[int64](t, e.pool, `SELECT COALESCE(sum(balance_remaining),0)::bigint FROM credit_buckets WHERE workspace_id=$1`, ws); n != 0 {
		t.Fatalf("lot balance after expiry = %d", n)
	}
	avail, err := ledger.Available(t.Context(), e.pool, ws, e.clock())
	if err != nil || avail != 0 {
		t.Fatalf("available after expiry = %d, %v", avail, err)
	}
	rec = e.do("GET", "/v1/credits", token, "", nil)
	wantStatus(t, rec, 200)
	if cr := decode(t, rec); cr["total"] != 0.0 {
		t.Fatalf("credits after expiry = %v", cr)
	}
	if err := ledger.CheckInvariants(t.Context(), e.pool, ws); err != nil {
		t.Fatal(err)
	}
}

// PRD §7.3: agent flow under a spend cap with structured citations.
func TestE2EAgentParity(t *testing.T) {
	e := newEnv(t)
	ws := e.workspace(ledger.Solo)
	e.fund(ws)
	_, token := e.key(ws, 3, 1000)

	all := candidates(4)
	e.provide(all)
	rec := e.do("POST", "/v1/enrichment/quote", token, "", cand{"candidates": all[:3]})
	wantStatus(t, rec, 200)
	if q := decode(t, rec); q["credits_required"] != 3.0 || q["shortfall"] != 0.0 {
		t.Fatalf("quote = %v", q)
	}

	rec = e.run(token, "agent-run-1", all[:3], "")
	wantStatus(t, rec, 200)
	run := decode(t, rec)
	if run["credits_committed"] != 3.0 {
		t.Fatalf("run = %v", run)
	}
	companies := sub[[]any](t, run, "companies")
	if len(companies) != 3 {
		t.Fatalf("companies = %v", companies)
	}

	wantError(t, e.run(token, "agent-run-2", all[3:], ""), 403, "spend_cap_exceeded")
	if got := e.available(ws); got != 497 {
		t.Fatalf("available = %d, want 497 (capped run must not charge)", got)
	}

	rec = e.do("GET", "/v1/companies/"+companies[0].(string), token, "", nil)
	wantStatus(t, rec, 200)
	cites := sub[[]any](t, decode(t, rec), "citations")
	if len(cites) == 0 {
		t.Fatal("company has no citations")
	}
	for _, c := range cites {
		cm := c.(map[string]any)
		for _, f := range []string{"data_quality_status", "origin_type", "source_system", "retrieved_at"} {
			if s, _ := cm[f].(string); s == "" {
				t.Fatalf("citation missing %s: %v", f, cm)
			}
		}
	}
	if err := ledger.CheckInvariants(t.Context(), e.pool, ws); err != nil {
		t.Fatal(err)
	}
}
