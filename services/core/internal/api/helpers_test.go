package api

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"sync"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
	"golang.org/x/time/rate"

	"rowan.ai/rowan/services/core/internal/enrich"
	"rowan.ai/rowan/services/core/internal/ledger"
	"rowan.ai/rowan/services/core/internal/quality"
	"rowan.ai/rowan/services/core/internal/resolver"
	"rowan.ai/rowan/services/core/internal/testdb"
)

var testNow = time.Date(2026, 11, 20, 12, 0, 0, 0, time.UTC)

type env struct {
	t        *testing.T
	pool     *pgxpool.Pool
	h        http.Handler
	provider *enrich.FakeProvider
	dir      string

	mu  sync.Mutex
	now time.Time
}

func (e *env) clock() time.Time {
	e.mu.Lock()
	defer e.mu.Unlock()
	return e.now
}

func (e *env) advance(d time.Duration) {
	e.mu.Lock()
	defer e.mu.Unlock()
	e.now = e.now.Add(d)
}

func newEnv(t *testing.T) *env {
	t.Helper()
	pool := testdb.New(t)
	e := &env{t: t, pool: pool, provider: &enrich.FakeProvider{Results: map[string][]resolver.SourceRecordInput{}},
		dir: t.TempDir(), now: testNow}
	svc := &enrich.Service{Pool: pool, Provider: e.provider, Limiter: rate.NewLimiter(rate.Inf, 1),
		CacheTTL: 30 * 24 * time.Hour, Now: e.clock}
	e.h = New(Deps{Pool: pool, Enrich: svc, ArtifactDir: e.dir, Now: e.clock})
	return e
}

func scalar[T any](t *testing.T, pool *pgxpool.Pool, query string, args ...any) T {
	t.Helper()
	var v T
	if err := pool.QueryRow(context.Background(), query, args...).Scan(&v); err != nil {
		t.Fatal(err)
	}
	return v
}

func (e *env) workspace(tier ledger.Tier) string {
	e.t.Helper()
	return scalar[string](e.t, e.pool, `INSERT INTO workspaces(name,tier) VALUES ('api',$1) RETURNING id::text`, string(tier))
}

// key creates an API key and returns its id and plaintext token.
func (e *env) key(ws string, spendCap int64, perMinute int) (id, token string) {
	e.t.Helper()
	token, err := NewToken()
	if err != nil {
		e.t.Fatal(err)
	}
	id = scalar[string](e.t, e.pool, `INSERT INTO api_keys(workspace_id,key_hash,label,monthly_spend_cap,rate_per_minute)
		VALUES ($1,$2,'k',$3,$4) RETURNING id::text`, ws, HashKey(token), spendCap, perMinute)
	return id, token
}

func (e *env) fund(ws string) {
	e.t.Helper()
	if err := ledger.GrantPeriod(context.Background(), e.pool, ws, testNow); err != nil {
		e.t.Fatal(err)
	}
}

// tenant is a funded workspace with one generous key.
func (e *env) tenant(tier ledger.Tier) (ws, keyID, token string) {
	e.t.Helper()
	ws = e.workspace(tier)
	e.fund(ws)
	keyID, token = e.key(ws, 1_000_000, 1000)
	return ws, keyID, token
}

func (e *env) do(method, path, token, idem string, body any) *httptest.ResponseRecorder {
	e.t.Helper()
	var reader *bytes.Reader
	switch b := body.(type) {
	case nil:
		reader = bytes.NewReader(nil)
	case string:
		reader = bytes.NewReader([]byte(b))
	case []byte:
		reader = bytes.NewReader(b)
	default:
		raw, err := json.Marshal(b)
		if err != nil {
			e.t.Fatal(err)
		}
		reader = bytes.NewReader(raw)
	}
	req := httptest.NewRequest(method, path, reader)
	if token != "" {
		req.Header.Set("Authorization", "Bearer "+token)
	}
	if idem != "" {
		req.Header.Set("Idempotency-Key", idem)
	}
	rec := httptest.NewRecorder()
	e.h.ServeHTTP(rec, req)
	return rec
}

func decode(t *testing.T, rec *httptest.ResponseRecorder) map[string]any {
	t.Helper()
	var m map[string]any
	if err := json.Unmarshal(rec.Body.Bytes(), &m); err != nil {
		t.Fatalf("decode %q: %v", rec.Body.String(), err)
	}
	return m
}

func wantStatus(t *testing.T, rec *httptest.ResponseRecorder, status int) {
	t.Helper()
	if rec.Code != status {
		t.Fatalf("status = %d, want %d; body %s", rec.Code, status, rec.Body.String())
	}
}

func wantError(t *testing.T, rec *httptest.ResponseRecorder, status int, code string) map[string]any {
	t.Helper()
	wantStatus(t, rec, status)
	body := decode(t, rec)
	errObj, _ := body["error"].(map[string]any)
	if errObj["code"] != code || errObj["message"] == "" || errObj["message"] == nil {
		t.Fatalf("error = %v, want code %q with message", body["error"], code)
	}
	return body
}

type cand = map[string]any

func candidates(n int) []cand {
	names := []string{"Zephyr Plumbing LLC", "Quartz Dental Group", "Maple Roofing Inc", "Orion Logistics Corp"}
	out := make([]cand, n)
	for i := range out {
		out[i] = cand{"ref": fmt.Sprintf("c%d", i+1), "legal_name": names[i], "state": "TX"}
	}
	return out
}

func citation(field, value string, status quality.Status, policy quality.ExportPolicy) quality.Citation {
	return quality.Citation{FieldName: field, Value: json.RawMessage(value), Origin: quality.RawSource, Status: status,
		ExportPolicy: policy, SourceSystem: "fake", SourceRef: "snap-" + field, TableName: "filings", RowKey: "r1",
		PageOrSpan: "p1", RetrievedAt: testNow}
}

// provide registers one hit record (state verified, revenue estimate) per candidate ref.
func (e *env) provide(cs []cand) {
	for _, c := range cs {
		ref, name := c["ref"].(string), c["legal_name"].(string)
		e.provider.Results[ref] = []resolver.SourceRecordInput{{SourceSystem: "fake", ExternalRef: ref,
			Facts: resolver.RecordFacts{LegalName: name, State: "TX"}, FetchedAt: testNow,
			Citations: []quality.Citation{
				citation("state", `"TX"`, quality.VerifiedFiling, quality.Allow),
				citation("revenue_cents", `250000000`, quality.CommercialEstimate, quality.DealTeamPlus),
			}}}
	}
}

func str(t *testing.T, m map[string]any, key string) string {
	t.Helper()
	s, ok := m[key].(string)
	if !ok || s == "" {
		t.Fatalf("%s missing in %v", key, m)
	}
	return s
}

func (e *env) buyBox(token, idem string) string {
	e.t.Helper()
	rec := e.do("POST", "/v1/buy-boxes", token, idem, cand{"name": "TX trades", "thesis_notes": "n",
		"criteria": cand{"states_in": []string{"TX"}, "revenue_min_cents": 200000000}})
	wantStatus(e.t, rec, 201)
	return str(e.t, decode(e.t, rec), "id")
}

func (e *env) run(token, idem string, cs []cand, buyBox string) *httptest.ResponseRecorder {
	body := cand{"candidates": cs}
	if buyBox != "" {
		body["buy_box_id"] = buyBox
	}
	return e.do("POST", "/v1/enrichment/run", token, idem, body)
}

func (e *env) available(ws string) int64 {
	e.t.Helper()
	n, err := ledger.Available(context.Background(), e.pool, ws, testNow)
	if err != nil {
		e.t.Fatal(err)
	}
	return n
}

func wrapf(err error) error { return fmt.Errorf("op: %w", err) }
