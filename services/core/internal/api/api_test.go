package api

import (
	"fmt"
	"net/http/httptest"
	"strconv"
	"strings"
	"testing"
	"time"

	"golang.org/x/time/rate"

	"rowan.ai/rowan/services/core/internal/enrich"
	"rowan.ai/rowan/services/core/internal/ledger"
)

func TestAuthRequired(t *testing.T) {
	e := newEnv(t)
	ws, _, token := e.tenant(ledger.Solo)
	revokedID, revoked := e.key(ws, 100, 100)
	if _, err := e.pool.Exec(t.Context(), `UPDATE api_keys SET revoked_at=now() WHERE id=$1`, revokedID); err != nil {
		t.Fatal(err)
	}
	for _, tok := range []string{"", "rk_live_nope", revoked} {
		wantError(t, e.do("GET", "/v1/credits", tok, "", nil), 401, "unauthorized")
	}
	// A non-Bearer scheme is rejected even with a valid token.
	req := httptest.NewRequest("GET", "/v1/credits", nil)
	req.Header.Set("Authorization", "Basic "+token)
	rec := httptest.NewRecorder()
	e.h.ServeHTTP(rec, req)
	wantError(t, rec, 401, "unauthorized")
	wantStatus(t, e.do("GET", "/v1/credits", token, "", nil), 200)
}

func TestTokenFormat(t *testing.T) {
	a, err := NewToken()
	if err != nil {
		t.Fatal(err)
	}
	b, _ := NewToken()
	if !strings.HasPrefix(a, "rk_live_") || len(a) != len("rk_live_")+52 || a == b {
		t.Fatalf("bad tokens %q %q", a, b)
	}
	if string(HashKey(a)) == string(HashKey(b)) || len(HashKey(a)) != 32 {
		t.Fatal("bad hash")
	}
}

func TestRateLimit429(t *testing.T) {
	e := newEnv(t)
	ws := e.workspace(ledger.Solo)
	_, limited := e.key(ws, 100, 2)
	_, other := e.key(ws, 100, 2)
	for range 2 {
		wantStatus(t, e.do("GET", "/v1/credits", limited, "", nil), 200)
	}
	rec := e.do("GET", "/v1/credits", limited, "", nil)
	wantError(t, rec, 429, "rate_limited")
	secs, err := strconv.Atoi(rec.Header().Get("Retry-After"))
	if err != nil || secs < 1 {
		t.Fatalf("Retry-After = %q", rec.Header().Get("Retry-After"))
	}
	// Limits are per key.
	wantStatus(t, e.do("GET", "/v1/credits", other, "", nil), 200)
	// Tokens refill with the injected clock: 2/min is one token per 30s.
	e.advance(31 * time.Second)
	wantStatus(t, e.do("GET", "/v1/credits", limited, "", nil), 200)
}

func TestRateLimitBeforeIdempotency(t *testing.T) {
	e := newEnv(t)
	ws := e.workspace(ledger.Solo)
	_, token := e.key(ws, 100, 1)
	wantStatus(t, e.do("GET", "/v1/credits", token, "", nil), 200)
	// Rate exceeded and no Idempotency-Key: 429 wins over 400.
	wantError(t, e.run(token, "", candidates(1), ""), 429, "rate_limited")
}

func TestIdempotencyKeyRequired(t *testing.T) {
	e := newEnv(t)
	_, _, token := e.tenant(ledger.Solo)
	wantError(t, e.run(token, "", candidates(1), ""), 400, "idempotency_key_required")
	wantError(t, e.do("POST", "/v1/buy-boxes", token, "", cand{}), 400, "idempotency_key_required")
	wantError(t, e.run(token, strings.Repeat("k", 256), candidates(1), ""), 400, "invalid_request")
	e.provide(candidates(1))
	wantStatus(t, e.run(token, strings.Repeat("k", 255), candidates(1), ""), 200)
}

func TestIdempotencyReplay(t *testing.T) {
	e := newEnv(t)
	ws, _, token := e.tenant(ledger.Solo)
	cs := candidates(2)
	e.provide(cs)
	first := e.run(token, "k1", cs, "")
	wantStatus(t, first, 200)
	if first.Header().Get("Idempotent-Replay") != "" {
		t.Fatal("first response flagged as replay")
	}
	if got := decode(t, first)["credits_committed"]; got != float64(2) {
		t.Fatalf("credits_committed = %v", got)
	}
	calls := e.provider.Calls.Load()
	second := e.run(token, "k1", cs, "")
	wantStatus(t, second, 200)
	if second.Header().Get("Idempotent-Replay") != "true" || second.Body.String() != first.Body.String() {
		t.Fatalf("replay differs: %q vs %q (%v)", second.Body.String(), first.Body.String(), second.Header())
	}
	if e.provider.Calls.Load() != calls {
		t.Fatal("replay called provider")
	}
	if got := e.available(ws); got != 498 {
		t.Fatalf("available = %d, want 498 (one charge)", got)
	}
	if err := ledger.CheckInvariants(t.Context(), e.pool, ws); err != nil {
		t.Fatal(err)
	}
}

func TestEnrichRunPartialReturns200(t *testing.T) {
	e := newEnv(t)
	ws, _, token := e.tenant(ledger.Solo)
	cs := candidates(3)
	e.provide(cs)
	// Run detaches request cancellation. A second token beyond runTimeout stops it after one hit.
	svc := &enrich.Service{Pool: e.pool, Provider: e.provider, Limiter: rate.NewLimiter(rate.Every(time.Hour), 1),
		CacheTTL: 30 * 24 * time.Hour, Now: e.clock}
	e.h = New(Deps{Pool: e.pool, Enrich: svc, ArtifactDir: e.dir, Now: e.clock})
	first := e.run(token, "partial", cs, "")
	wantStatus(t, first, 200)
	body := decode(t, first)
	runID := str(t, body, "run_id")
	if body["status"] != "partial" || body["unprocessed"] != float64(2) || body["hits"] != float64(1) ||
		body["credits_committed"] != float64(1) || body["credits_refunded"] != float64(2) {
		t.Fatalf("partial result = %v", body)
	}
	if status := scalar[string](t, e.pool, `SELECT status FROM enrichment_runs WHERE id=$1`, runID); status != "partial" {
		t.Fatalf("stored status = %q, want partial", status)
	}
	second := e.run(token, "partial", cs, "")
	wantStatus(t, second, 200)
	if second.Header().Get("Idempotent-Replay") != "true" || second.Body.String() != first.Body.String() {
		t.Fatalf("replay differs: %q vs %q (%v)", second.Body.String(), first.Body.String(), second.Header())
	}
	if calls := e.provider.Calls.Load(); calls != 1 {
		t.Fatalf("provider calls = %d, want 1", calls)
	}
	if got := e.available(ws); got != 499 {
		t.Fatalf("available = %d, want 499 (one charge)", got)
	}
	if err := ledger.CheckInvariants(t.Context(), e.pool, ws); err != nil {
		t.Fatal(err)
	}
}

func TestIdempotencyConflict(t *testing.T) {
	e := newEnv(t)
	_, _, token := e.tenant(ledger.Solo)
	e.provide(candidates(2))
	wantStatus(t, e.run(token, "k1", candidates(1), ""), 200)
	wantError(t, e.run(token, "k1", candidates(2), ""), 409, "idempotency_key_reuse")
	// The same key on a different path also conflicts.
	wantError(t, e.do("POST", "/v1/buy-boxes", token, "k1", cand{"name": "x"}), 409, "idempotency_key_reuse")
}

func TestIdempotencyScopedPerWorkspace(t *testing.T) {
	e := newEnv(t)
	ws1, _, tok1 := e.tenant(ledger.Solo)
	ws2, _, tok2 := e.tenant(ledger.Solo)
	e.provide(candidates(2))
	r1 := e.run(tok1, "shared", candidates(1), "")
	r2 := e.run(tok2, "shared", candidates(2), "")
	wantStatus(t, r1, 200)
	wantStatus(t, r2, 200)
	if decode(t, r1)["run_id"] == decode(t, r2)["run_id"] {
		t.Fatal("workspaces share a run")
	}
	if e.available(ws1) != 499 || e.available(ws2) != 498 {
		t.Fatalf("charges wrong: %d %d", e.available(ws1), e.available(ws2))
	}
}

func TestRunInsufficient402(t *testing.T) {
	e := newEnv(t)
	ws := e.workspace(ledger.Solo) // unfunded
	_, token := e.key(ws, 1_000_000, 100)
	e.provide(candidates(2))
	body := wantError(t, e.run(token, "k1", candidates(2), ""), 402, "insufficient_credits")
	quote, _ := body["quote"].(map[string]any)
	if quote["shortfall"] != float64(2) || quote["credits_required"] != float64(2) ||
		quote["overage_estimate_cents"] != float64(120) || quote["credits_available"] != float64(0) {
		t.Fatalf("quote = %v", quote)
	}
	if n := scalar[int](t, e.pool, `SELECT count(*) FROM credit_reservations`); n != 0 {
		t.Fatalf("reservations = %d", n)
	}
}

func TestSpendCap403(t *testing.T) {
	e := newEnv(t)
	ws := e.workspace(ledger.Solo)
	e.fund(ws)
	_, token := e.key(ws, 1, 100)
	e.provide(candidates(2))
	wantError(t, e.run(token, "k1", candidates(2), ""), 403, "spend_cap_exceeded")
	if e.available(ws) != 500 {
		t.Fatal("credits moved on rejected run")
	}
}

func TestZeroHitRefund(t *testing.T) {
	e := newEnv(t)
	ws, _, token := e.tenant(ledger.Solo)
	cs := candidates(2)
	e.provide(cs[:1]) // c2 is a zero-hit
	rec := e.run(token, "k1", cs, "")
	wantStatus(t, rec, 200)
	body := decode(t, rec)
	if body["hits"] != float64(1) || body["zero_hits"] != float64(1) || body["credits_refunded"] != float64(1) {
		t.Fatalf("body = %v", body)
	}
	if e.available(ws) != 499 {
		t.Fatalf("available = %d", e.available(ws))
	}
}

func TestUnknownFieldRejected(t *testing.T) {
	e := newEnv(t)
	_, _, token := e.tenant(ledger.Solo)
	wantError(t, e.do("POST", "/v1/buy-boxes", token, "k1", `{"name":"x","criteria":{"states_in":["TX"]},"bogus":1}`), 400, "invalid_request")
	wantError(t, e.do("POST", "/v1/enrichment/run", token, "k2", `{"candidates":[],"bogus":1}`), 400, "invalid_request")
	wantError(t, e.do("POST", "/v1/enrichment/quote", token, "", `{"candidates":[{"ref":"a","bogus":1}]}`), 400, "invalid_request")
	wantError(t, e.do("POST", "/v1/enrichment/quote", token, "", `{not json`), 400, "invalid_request")
	big := `{"candidates":[],"pad":"` + strings.Repeat("x", 1<<20) + `"}`
	wantError(t, e.do("POST", "/v1/enrichment/quote", token, "", big), 400, "invalid_request")
}

func TestIdempotencyDoesNotStoreInsufficientCredits(t *testing.T) {
	e := newEnv(t)
	ws := e.workspace(ledger.Solo) // unfunded
	_, token := e.key(ws, 1_000_000, 100)
	cs := candidates(2)
	e.provide(cs)
	wantError(t, e.run(token, "k1", cs, ""), 402, "insufficient_credits")
	if n := scalar[int](t, e.pool, `SELECT count(*) FROM idempotency_records`); n != 0 {
		t.Fatalf("402 was stored: %d records", n)
	}
	if err := ledger.GrantTopup(t.Context(), e.pool, ws, 10, testNow); err != nil {
		t.Fatal(err)
	}
	rec := e.run(token, "k1", cs, "")
	wantStatus(t, rec, 200)
	if rec.Header().Get("Idempotent-Replay") != "" {
		t.Fatal("stale 402 replayed")
	}
	if got := e.available(ws); got != 8 {
		t.Fatalf("available = %d, want 8 (charged once)", got)
	}
	again := e.run(token, "k1", cs, "")
	if again.Header().Get("Idempotent-Replay") != "true" || e.available(ws) != 8 {
		t.Fatalf("retry after success must replay without charge")
	}
}

func TestIdempotencyDoesNotStoreSpendCap(t *testing.T) {
	e := newEnv(t)
	ws := e.workspace(ledger.Solo)
	e.fund(ws)
	keyID, token := e.key(ws, 1, 100)
	cs := candidates(2)
	e.provide(cs)
	wantError(t, e.run(token, "k1", cs, ""), 403, "spend_cap_exceeded")
	if n := scalar[int](t, e.pool, `SELECT count(*) FROM idempotency_records`); n != 0 {
		t.Fatalf("403 was stored: %d records", n)
	}
	if _, err := e.pool.Exec(t.Context(), `UPDATE api_keys SET monthly_spend_cap=1000 WHERE id=$1`, keyID); err != nil {
		t.Fatal(err)
	}
	wantStatus(t, e.run(token, "k1", cs, ""), 200)
	if got := e.available(ws); got != 498 {
		t.Fatalf("available = %d, want 498", got)
	}
}

func TestRunCandidateCap400(t *testing.T) {
	e := newEnv(t)
	_, _, token := e.tenant(ledger.Solo)
	big := make([]cand, 501)
	for i := range big {
		big[i] = cand{"ref": fmt.Sprintf("r%d", i), "legal_name": fmt.Sprintf("Co %d", i)}
	}
	body := wantError(t, e.run(token, "k1", big, ""), 400, "invalid_request")
	if msg := body["error"].(map[string]any)["message"].(string); !strings.Contains(msg, "500") {
		t.Fatalf("message %q does not name the cap", msg)
	}
	wantError(t, e.do("POST", "/v1/enrichment/quote", token, "", cand{"candidates": big}), 400, "invalid_request")
}
