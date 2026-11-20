package api

import (
	"context"
	"encoding/json"
	"strings"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"

	"rowan.ai/rowan/services/core/internal/db"
	"rowan.ai/rowan/services/core/internal/enrich"
	"rowan.ai/rowan/services/core/internal/ledger"
	"rowan.ai/rowan/services/core/internal/quality"
	"rowan.ai/rowan/services/core/internal/resolver"
)

// runCompany enriches one candidate and returns its canonical company id.
func (e *env) runCompany(token, idem string) string {
	e.t.Helper()
	cs := candidates(1)
	e.provide(cs)
	rec := e.run(token, idem, cs, "")
	wantStatus(e.t, rec, 200)
	companies, _ := decode(e.t, rec)["companies"].([]any)
	if len(companies) != 1 {
		e.t.Fatalf("companies = %v", companies)
	}
	return companies[0].(string)
}

func (e *env) ingest(ws string, in resolver.SourceRecordInput) resolver.ResolveResult {
	e.t.Helper()
	in.WorkspaceID, in.FetchedAt = ws, testNow
	var res resolver.ResolveResult
	err := db.InTx(context.Background(), e.pool, func(tx pgx.Tx) error {
		var err error
		res, err = resolver.Resolve(context.Background(), tx, in, testNow)
		return err
	})
	if err != nil {
		e.t.Fatal(err)
	}
	return res
}

func TestCompanyResponseHasProvenance(t *testing.T) {
	e := newEnv(t)
	_, _, token := e.tenant(ledger.Solo)
	id := e.runCompany(token, "k1")
	rec := e.do("GET", "/v1/companies/"+id, token, "", nil)
	wantStatus(t, rec, 200)
	body := decode(t, rec)
	if body["id"] != id || body["display_name"] != "Zephyr Plumbing LLC" || body["pending_review"] != false {
		t.Fatalf("company = %v", body)
	}
	if v, ok := body["proxy_summary"]; !ok || v != nil {
		t.Fatalf("proxy_summary = %v, %v", v, ok)
	}
	citations, _ := body["citations"].([]any)
	if len(citations) < 2 {
		t.Fatalf("citations = %v", citations)
	}
	for _, c := range citations {
		m := c.(map[string]any)
		for _, k := range []string{"data_quality_status", "origin_type", "export_policy", "source_system", "source_ref", "retrieved_at", "field_name"} {
			if s, _ := m[k].(string); s == "" {
				t.Fatalf("citation %v lacks %s", m, k)
			}
		}
		if _, ok := m["field_value"]; !ok {
			t.Fatalf("citation %v lacks field_value", m)
		}
	}
	links, _ := body["links"].([]any)
	if len(links) != 1 {
		t.Fatalf("links = %v", links)
	}
	link := links[0].(map[string]any)
	reasons, _ := link["match_reasons"].([]any)
	if link["source_record_id"] == "" || link["merge_mode"] != "seed" || link["status"] != "active" ||
		link["merge_confidence_score"] == nil || reasons == nil {
		t.Fatalf("link = %v", link)
	}
}

func TestCrossWorkspace404(t *testing.T) {
	e := newEnv(t)
	_, _, tok1 := e.tenant(ledger.Solo)
	_, _, tok2 := e.tenant(ledger.Solo)
	company := e.runCompany(tok1, "k1")
	bb := e.buyBox(tok1, "kb")
	wantStatus(t, e.do("GET", "/v1/companies/"+company, tok1, "", nil), 200)

	wantError(t, e.do("GET", "/v1/companies/"+company, tok2, "", nil), 404, "not_found")
	wantError(t, e.do("GET", "/v1/buy-boxes/"+bb, tok2, "", nil), 404, "not_found")
	wantError(t, e.do("GET", "/v1/buy-boxes/"+bb+"/evaluations", tok2, "", nil), 404, "not_found")
	wantError(t, e.do("POST", "/v1/discovery/search", tok2, "", cand{"buy_box_id": bb}), 404, "not_found")
	wantError(t, e.do("POST", "/v1/companies/"+company+"/split", tok2, "s1",
		cand{"source_record_ids": []string{company}, "reason": "x"}), 404, "not_found")
	wantError(t, e.do("POST", "/v1/exports", tok2, "x1", cand{"buy_box_id": bb, "columns": []string{"state"}}), 404, "not_found")
	// Malformed ids are not found, not server errors.
	wantError(t, e.do("GET", "/v1/companies/not-a-uuid", tok1, "", nil), 404, "not_found")
}

func TestBuyBoxCreateAndGet(t *testing.T) {
	e := newEnv(t)
	_, _, token := e.tenant(ledger.Solo)
	body := cand{"name": "TX trades", "thesis_notes": "notes", "criteria": cand{"states_in": []string{"TX"}}}
	rec := e.do("POST", "/v1/buy-boxes", token, "k1", body)
	wantStatus(t, rec, 201)
	id := str(t, decode(t, rec), "id")
	replay := e.do("POST", "/v1/buy-boxes", token, "k1", body)
	if replay.Header().Get("Idempotent-Replay") != "true" || replay.Body.String() != rec.Body.String() {
		t.Fatal("create not idempotent")
	}
	if n := scalar[int](t, e.pool, `SELECT count(*) FROM buy_boxes`); n != 1 {
		t.Fatalf("buy boxes = %d", n)
	}
	got := e.do("GET", "/v1/buy-boxes/"+id, token, "", nil)
	wantStatus(t, got, 200)
	gb := decode(t, got)
	crit, _ := gb["criteria"].(map[string]any)
	if gb["name"] != "TX trades" || gb["thesis_notes"] != "notes" || crit["states_in"] == nil {
		t.Fatalf("buy box = %v", gb)
	}
}

func TestBuyBoxRejectsInvalidCriteria(t *testing.T) {
	e := newEnv(t)
	_, _, token := e.tenant(ledger.Solo)
	for i, criteria := range []string{`{"naics_in":[""]}`, `{"revenue_min_cents":-5}`, `{}`, `{"nope":1}`,
		`{"revenue_min_cents":9,"revenue_max_cents":1}`, `null`, ``} {
		raw := `{"name":"x","criteria":` + criteria + `}`
		if criteria == "" {
			raw = `{"name":"x"}`
		}
		wantError(t, e.do("POST", "/v1/buy-boxes", token, "k"+string(rune('a'+i)), raw), 400, "invalid_request")
	}
	wantError(t, e.do("POST", "/v1/buy-boxes", token, "kz", `{"name":"  ","criteria":{"states_in":["TX"]}}`), 400, "invalid_request")
}

func TestBuyBoxEvaluatesExistingCompaniesAndDiscovery(t *testing.T) {
	e := newEnv(t)
	_, _, token := e.tenant(ledger.Solo)
	company := e.runCompany(token, "k1")
	bb := e.buyBox(token, "kb") // created after the company exists

	rec := e.do("POST", "/v1/discovery/search", token, "", cand{"buy_box_id": bb})
	wantStatus(t, rec, 200)
	results, _ := decode(t, rec)["results"].([]any)
	if len(results) != 1 {
		t.Fatalf("results = %v", results)
	}
	r := results[0].(map[string]any)
	if r["id"] != company || r["display_name"] != "Zephyr Plumbing LLC" || r["fit"] != "partial_unverified" || r["overall_score"] != 0.5 {
		t.Fatalf("result = %v", r)
	}
	breakdown, _ := r["criteria_breakdown"].([]any)
	statuses := map[string]string{}
	for _, b := range breakdown {
		m := b.(map[string]any)
		statuses[m["criterion"].(string)] = m["status"].(string) + "/" + m["data_quality_status"].(string)
	}
	if statuses["states_in"] != "MET/verified_filing" || statuses["revenue_min_cents"] != "UNVERIFIED_LOW_CONFIDENCE/commercial_estimate" {
		t.Fatalf("breakdown = %v", statuses)
	}

	filter := func(fit ...string) int {
		rec := e.do("POST", "/v1/discovery/search", token, "", cand{"buy_box_id": bb, "fit": fit})
		wantStatus(t, rec, 200)
		list, _ := decode(t, rec)["results"].([]any)
		return len(list)
	}
	if filter("match") != 0 || filter("partial_unverified", "fail") != 1 {
		t.Fatal("fit filter wrong")
	}
	wantError(t, e.do("POST", "/v1/discovery/search", token, "", cand{"buy_box_id": bb, "fit": []string{"bogus"}}), 400, "invalid_request")
	wantError(t, e.do("POST", "/v1/discovery/search", token, "", cand{"buy_box_id": "nope"}), 400, "invalid_request")

	ev := e.do("GET", "/v1/buy-boxes/"+bb+"/evaluations", token, "", nil)
	wantStatus(t, ev, 200)
	list, _ := decode(t, ev)["evaluations"].([]any)
	if len(list) != 1 || list[0].(map[string]any)["criteria_breakdown"] == nil {
		t.Fatalf("evaluations = %v", list)
	}
}

func TestDiscoveryOrderedByScore(t *testing.T) {
	e := newEnv(t)
	ws, _, token := e.tenant(ledger.Solo)
	bb := e.buyBox(token, "kb")
	low := e.ingest(ws, resolver.SourceRecordInput{SourceSystem: "x", ExternalRef: "low",
		Facts: resolver.RecordFacts{LegalName: "Low Co", State: "TX"}})
	high := e.ingest(ws, resolver.SourceRecordInput{SourceSystem: "x", ExternalRef: "high",
		Facts: resolver.RecordFacts{LegalName: "High Co", State: "TX"},
		Citations: []quality.Citation{citation("state", `"TX"`, quality.VerifiedFiling, quality.Allow),
			citation("revenue_cents", `300000000`, quality.VerifiedFiling, quality.Allow)}})
	rec := e.do("POST", "/v1/discovery/search", token, "", cand{"buy_box_id": bb})
	wantStatus(t, rec, 200)
	results, _ := decode(t, rec)["results"].([]any)
	if len(results) != 2 || results[0].(map[string]any)["id"] != high.CanonicalID || results[1].(map[string]any)["id"] != low.CanonicalID {
		t.Fatalf("order = %v", results)
	}
	if results[0].(map[string]any)["fit"] != "match" {
		t.Fatalf("high = %v", results[0])
	}
}

func TestQuoteIsFree(t *testing.T) {
	e := newEnv(t)
	ws, _, token := e.tenant(ledger.Solo)
	before := scalar[string](t, e.pool, `SELECT (SELECT count(*) FROM credit_ledger_entries)::text || (SELECT count(*) FROM credit_reservations)::text`)
	rec := e.do("POST", "/v1/enrichment/quote", token, "", cand{"candidates": candidates(3)})
	wantStatus(t, rec, 200)
	q := decode(t, rec)
	if q["candidate_count"] != float64(3) || q["billable"] != float64(3) || q["already_enriched"] != float64(0) ||
		q["credits_required"] != float64(3) || q["credits_available"] != float64(500) || q["shortfall"] != float64(0) {
		t.Fatalf("quote = %v", q)
	}
	after := scalar[string](t, e.pool, `SELECT (SELECT count(*) FROM credit_ledger_entries)::text || (SELECT count(*) FROM credit_reservations)::text`)
	if before != after || e.available(ws) != 500 {
		t.Fatal("quote wrote to the ledger")
	}
	wantError(t, e.do("POST", "/v1/enrichment/quote", token, "", cand{"candidates": []cand{{"ref": ""}}}), 400, "invalid_request")
}

func TestReviewQueueResolve(t *testing.T) {
	e := newEnv(t)
	ws, keyID, token := e.tenant(ledger.Solo)
	e.ingest(ws, resolver.SourceRecordInput{SourceSystem: "filing", ExternalRef: "first",
		Facts: resolver.RecordFacts{LegalName: "Acme LLC", State: "TX"}})
	e.ingest(ws, resolver.SourceRecordInput{SourceSystem: "filing", ExternalRef: "second",
		Facts: resolver.RecordFacts{LegalName: "Other Corp", DBA: "ACME", State: "tx"}})

	rec := e.do("GET", "/v1/review-queue", token, "", nil)
	wantStatus(t, rec, 200)
	queue, _ := decode(t, rec)["candidates"].([]any)
	if len(queue) != 1 {
		t.Fatalf("queue = %v", queue)
	}
	item := queue[0].(map[string]any)
	id := str(t, item, "id")
	if item["score"] == nil || item["match_reasons"] == nil || item["canonical_company_id"] == "" || item["source_record_id"] == "" {
		t.Fatalf("item = %v", item)
	}

	wantError(t, e.do("POST", "/v1/review-queue/"+id+"/resolve", token, "r0", cand{"action": "bogus"}), 400, "invalid_request")
	wantError(t, e.do("POST", "/v1/review-queue/00000000-0000-0000-0000-000000000000/resolve", token, "r00", cand{"action": "merge"}), 404, "not_found")
	res := e.do("POST", "/v1/review-queue/"+id+"/resolve", token, "r1", cand{"action": "merge"})
	wantStatus(t, res, 200)
	if decode(t, res)["status"] != "merged" {
		t.Fatalf("resolve = %s", res.Body.String())
	}
	replay := e.do("POST", "/v1/review-queue/"+id+"/resolve", token, "r1", cand{"action": "merge"})
	if replay.Header().Get("Idempotent-Replay") != "true" || replay.Body.String() != res.Body.String() {
		t.Fatal("resolve not replayed")
	}
	if by := scalar[string](t, e.pool, `SELECT resolved_by FROM possible_match_candidates WHERE id=$1`, id); by != "api_key:"+keyID {
		t.Fatalf("resolved_by = %q", by)
	}
	wantError(t, e.do("POST", "/v1/review-queue/"+id+"/resolve", token, "r2", cand{"action": "reject"}), 409, "already_resolved")
	rec = e.do("GET", "/v1/review-queue", token, "", nil)
	if q, _ := decode(t, rec)["candidates"].([]any); len(q) != 0 {
		t.Fatalf("queue not drained: %v", q)
	}
}

func TestReviewResolveDoNotMerge409AndCrossWorkspace(t *testing.T) {
	e := newEnv(t)
	ws, _, token := e.tenant(ledger.Solo)
	_, _, other := e.tenant(ledger.Solo)
	e.ingest(ws, resolver.SourceRecordInput{SourceSystem: "filing", ExternalRef: "first",
		Facts: resolver.RecordFacts{LegalName: "Acme LLC", State: "TX"}})
	e.ingest(ws, resolver.SourceRecordInput{SourceSystem: "filing", ExternalRef: "second",
		Facts: resolver.RecordFacts{LegalName: "Other Corp", DBA: "ACME", State: "tx"}})
	id := scalar[string](t, e.pool, `SELECT id::text FROM possible_match_candidates`)
	wantError(t, e.do("POST", "/v1/review-queue/"+id+"/resolve", other, "r1", cand{"action": "merge"}), 404, "not_found")
	if st := scalar[string](t, e.pool, `SELECT status FROM possible_match_candidates WHERE id=$1`, id); st != "pending" {
		t.Fatalf("cross-workspace call changed status to %s", st)
	}
	if _, err := e.pool.Exec(t.Context(), `INSERT INTO do_not_merge_pairs(record_a_id,record_b_id,reason,created_by)
		SELECT LEAST(a.id,b.id),GREATEST(a.id,b.id),'t','t' FROM source_records a, source_records b
		WHERE a.external_ref='second' AND b.external_ref='first'`); err != nil {
		t.Fatal(err)
	}
	wantError(t, e.do("POST", "/v1/review-queue/"+id+"/resolve", token, "r2", cand{"action": "merge"}), 409, "do_not_merge")
}

func TestSplit(t *testing.T) {
	e := newEnv(t)
	ws, keyID, token := e.tenant(ledger.Solo)
	a := e.ingest(ws, resolver.SourceRecordInput{SourceSystem: "x", ExternalRef: "A",
		Facts: resolver.RecordFacts{LegalName: "Acme A", IDs: map[string]string{"ein": "1"}}})
	c := e.ingest(ws, resolver.SourceRecordInput{SourceSystem: "x", ExternalRef: "C",
		Facts: resolver.RecordFacts{LegalName: "Cee Corp", IDs: map[string]string{"ein": "1"}}})
	if a.CanonicalID != c.CanonicalID {
		t.Fatal("seed did not merge")
	}
	path := "/v1/companies/" + a.CanonicalID + "/split"
	wantError(t, e.do("POST", path, token, "s0", cand{"source_record_ids": []string{}, "reason": "x"}), 422, "invalid_split")
	wantError(t, e.do("POST", path, token, "s00", cand{"source_record_ids": []string{a.SourceRecordID, c.SourceRecordID}, "reason": "x"}), 422, "invalid_split")
	rec := e.do("POST", path, token, "s1", cand{"source_record_ids": []string{c.SourceRecordID}, "reason": "wrong merge"})
	wantStatus(t, rec, 200)
	newID := str(t, decode(t, rec), "new_canonical_id")
	if newID == a.CanonicalID {
		t.Fatal("split returned old canonical")
	}
	replay := e.do("POST", path, token, "s1", cand{"source_record_ids": []string{c.SourceRecordID}, "reason": "wrong merge"})
	if replay.Header().Get("Idempotent-Replay") != "true" || replay.Body.String() != rec.Body.String() {
		t.Fatal("split not replayed")
	}
	if n := scalar[int](t, e.pool, `SELECT count(*) FROM do_not_merge_pairs WHERE created_by=$1 AND reason='wrong merge'`, "api_key:"+keyID); n != 1 {
		t.Fatalf("do-not-merge pairs by actor = %d", n)
	}
	if got := scalar[string](t, e.pool, `SELECT canonical_company_id::text FROM entity_merge_links WHERE source_record_id=$1`, c.SourceRecordID); got != newID {
		t.Fatalf("record moved to %s", got)
	}
}

func TestExportRedactsAndAudits(t *testing.T) {
	e := newEnv(t)
	_, keyID, token := e.tenant(ledger.Solo)
	e.runCompany(token, "k1")
	bb := e.buyBox(token, "kb")
	req := cand{"buy_box_id": bb, "columns": []string{"state", "revenue_cents"}}

	wantError(t, e.do("POST", "/v1/exports", token, "x0", cand{"buy_box_id": bb, "columns": []string{"ssn"}}), 400, "invalid_request")
	wantError(t, e.do("POST", "/v1/exports", token, "x00", cand{"buy_box_id": "nope", "columns": []string{"state"}}), 400, "invalid_request")
	rec := e.do("POST", "/v1/exports", token, "x1", req)
	wantStatus(t, rec, 200)
	if ct := rec.Header().Get("Content-Type"); !strings.HasPrefix(ct, "text/csv") {
		t.Fatalf("content-type = %q", ct)
	}
	if rf := rec.Header().Get("X-Rowan-Redacted-Fields"); rf != "revenue_cents" {
		t.Fatalf("redacted header = %q", rf)
	}
	csv := rec.Body.String()
	if !strings.Contains(csv, "[REDACTED]") || strings.Contains(csv, "250000000") || !strings.Contains(csv, "verified_filing") {
		t.Fatalf("csv = %s", csv)
	}
	if by := scalar[string](t, e.pool, `SELECT user_or_key_id FROM export_audits`); by != "api_key:"+keyID {
		t.Fatalf("audit actor = %q", by)
	}
	replay := e.do("POST", "/v1/exports", token, "x1", req)
	if replay.Header().Get("Idempotent-Replay") != "true" || replay.Body.String() != csv ||
		!strings.HasPrefix(replay.Header().Get("Content-Type"), "text/csv") || replay.Header().Get("X-Rowan-Redacted-Fields") != "revenue_cents" {
		t.Fatalf("replay lost csv headers: %v", replay.Header())
	}
	if n := scalar[int](t, e.pool, `SELECT count(*) FROM export_audits`); n != 1 {
		t.Fatalf("audits = %d", n)
	}
}

func TestExportFundTierSeesGatedColumn(t *testing.T) {
	e := newEnv(t)
	_, _, token := e.tenant(ledger.Fund)
	e.runCompany(token, "k1")
	bb := e.buyBox(token, "kb")
	rec := e.do("POST", "/v1/exports", token, "x1", cand{"buy_box_id": bb, "columns": []string{"revenue_cents"}})
	wantStatus(t, rec, 200)
	if rec.Header().Get("X-Rowan-Redacted-Fields") != "" || !strings.Contains(rec.Body.String(), "250000000") {
		t.Fatalf("fund export redacted: %v %s", rec.Header(), rec.Body.String())
	}
}

func TestCredits(t *testing.T) {
	e := newEnv(t)
	ws := e.workspace(ledger.Solo)
	e.fund(ws)
	if err := ledger.GrantTopup(t.Context(), e.pool, ws, 40, testNow); err != nil {
		t.Fatal(err)
	}
	_, token := e.key(ws, 100, 100)
	cs := candidates(3)
	e.provide(cs[:2]) // c3 is a zero-hit
	wantStatus(t, e.run(token, "k1", cs, ""), 200)
	rec := e.do("GET", "/v1/credits", token, "", nil)
	wantStatus(t, rec, 200)
	body := decode(t, rec)
	avail, _ := body["available"].(map[string]any)
	mtd, _ := body["month_to_date"].(map[string]any)
	if avail["included"] != float64(498) || avail["topup"] != float64(40) || avail["rollover"] != float64(0) ||
		body["total"] != float64(538) || mtd["spent"] != float64(2) || mtd["cap"] != float64(100) {
		t.Fatalf("credits = %v", body)
	}
	// Another key's spend does not count against this key.
	_, otherTok := e.key(ws, 50, 100)
	mtd2, _ := decode(t, e.do("GET", "/v1/credits", otherTok, "", nil))["month_to_date"].(map[string]any)
	if mtd2["spent"] != float64(0) || mtd2["cap"] != float64(50) {
		t.Fatalf("other key = %v", mtd2)
	}
}

func TestIdempotencyInProgressAndStaleTakeover(t *testing.T) {
	e := newEnv(t)
	ws, _, token := e.tenant(ledger.Solo)
	cs := candidates(1)
	e.provide(cs)
	first := e.run(token, "k1", cs, "")
	wantStatus(t, first, 200)
	// Simulate a crash after the work finished but before the response was stored.
	if _, err := e.pool.Exec(t.Context(), `UPDATE idempotency_records SET state='in_progress', status_code=NULL,
		response_body=NULL, response_headers=NULL, created_at=$2 WHERE workspace_id=$1`, ws, testNow); err != nil {
		t.Fatal(err)
	}
	wantError(t, e.run(token, "k1", cs, ""), 409, "request_in_progress")
	e.advance(16 * time.Minute)
	retry := e.run(token, "k1", cs, "")
	wantStatus(t, retry, 200)
	if decode(t, retry)["run_id"] != decode(t, first)["run_id"] {
		t.Fatal("takeover re-ran instead of recovering the stored run")
	}
	if retry.Header().Get("Idempotent-Replay") != "" {
		t.Fatal("takeover flagged as replay")
	}
	replay := e.run(token, "k1", cs, "")
	if replay.Header().Get("Idempotent-Replay") != "true" {
		t.Fatal("takeover result not stored")
	}
}

func TestServerErrorsAreNotStoredOrLeaked(t *testing.T) {
	e := newEnv(t)
	_, keyID, token := e.tenant(ledger.Solo)
	_ = keyID
	nilEnrich := New(Deps{Pool: e.pool, Enrich: nil, ArtifactDir: e.dir, Now: e.clock})
	e.h = nilEnrich // Run dereferences the nil service: panic => 500.
	rec := e.run(token, "k1", candidates(1), "")
	body := wantError(t, rec, 500, "internal")
	if strings.Contains(rec.Body.String(), "nil pointer") || body["quote"] != nil {
		t.Fatalf("500 leaked detail: %s", rec.Body.String())
	}
	if n := scalar[int](t, e.pool, `SELECT count(*) FROM idempotency_records`); n != 0 {
		t.Fatalf("5xx row kept: %d", n)
	}
}

func TestMapError(t *testing.T) {
	cases := []struct {
		err    error
		status int
		code   string
	}{
		{ledger.ErrSpendCapExceeded, 403, "spend_cap_exceeded"},
		{resolver.ErrInvalidSplit, 422, "invalid_split"},
		{resolver.ErrAlreadyResolved, 409, "already_resolved"},
		{resolver.ErrDoNotMerge, 409, "do_not_merge"},
		{enrich.ErrRunInProgress, 409, "request_in_progress"},
		{ledger.ErrIdempotencyConflict, 409, "idempotency_key_reuse"},
		{enrich.ErrInvalidRequest, 400, "invalid_request"},
		{resolver.ErrInvalidRequest, 400, "invalid_request"},
		{ledger.ErrInvalidRequest, 400, "invalid_request"},
		{errNotFound, 404, "not_found"},
		{context.DeadlineExceeded, 500, "internal"},
	}
	for _, tc := range cases {
		resp := mapError(wrapf(tc.err))
		var body struct {
			Error struct{ Code, Message string }
		}
		if err := json.Unmarshal(resp.body, &body); err != nil {
			t.Fatal(err)
		}
		if resp.status != tc.status || body.Error.Code != tc.code || body.Error.Message == "" {
			t.Fatalf("%v => %d %s", tc.err, resp.status, resp.body)
		}
		if tc.status == 500 && strings.Contains(string(resp.body), "deadline") {
			t.Fatal("500 leaked error text")
		}
	}
}

func TestIdempotencyConcurrentSameKeyChargesOnce(t *testing.T) {
	e := newEnv(t)
	ws, _, token := e.tenant(ledger.Solo)
	cs := candidates(2)
	e.provide(cs)
	e.provider.OnLookup = func(context.Context, enrich.Candidate) { time.Sleep(40 * time.Millisecond) }
	codes := make(chan int, 6)
	for range cap(codes) {
		go func() {
			rec := e.run(token, "same", cs, "")
			codes <- rec.Code
		}()
	}
	ok := 0
	for range cap(codes) {
		switch code := <-codes; code {
		case 200:
			ok++
		case 409: // request_in_progress while the owner works
		default:
			t.Fatalf("unexpected status %d", code)
		}
	}
	if ok == 0 {
		t.Fatal("no request succeeded")
	}
	if e.provider.Calls.Load() != 2 || e.available(ws) != 498 {
		t.Fatalf("calls=%d available=%d, want 2 lookups and one charge", e.provider.Calls.Load(), e.available(ws))
	}
}
