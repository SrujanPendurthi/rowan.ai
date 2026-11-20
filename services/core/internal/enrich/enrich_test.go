package enrich

import (
	"context"
	"errors"
	"fmt"
	"reflect"
	"sort"
	"sync"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
	"golang.org/x/time/rate"

	"rowan.ai/rowan/services/core/internal/ledger"
	"rowan.ai/rowan/services/core/internal/resolver"
	"rowan.ai/rowan/services/core/internal/testdb"
)

var testNow = time.Date(2026, 11, 20, 12, 0, 0, 0, time.UTC)

var names = []string{"Zephyr Plumbing LLC", "Quartz Dental Group", "Maple Roofing Inc",
	"Orion Logistics Corp", "Birch Bakery LLC", "Falcon Machining Co", "Harbor Freight Brokers"}

func scalar[T any](t *testing.T, pool *pgxpool.Pool, query string, args ...any) T {
	t.Helper()
	var v T
	if err := pool.QueryRow(context.Background(), query, args...).Scan(&v); err != nil {
		t.Fatal(err)
	}
	return v
}

func seedWorkspace(t *testing.T, pool *pgxpool.Pool, tier ledger.Tier) string {
	t.Helper()
	return scalar[string](t, pool, `INSERT INTO workspaces(name,tier) VALUES ('enrich',$1) RETURNING id::text`, string(tier))
}

func seedKey(t *testing.T, pool *pgxpool.Pool, workspace string) string {
	t.Helper()
	return scalar[string](t, pool, `INSERT INTO api_keys(workspace_id,key_hash,label,monthly_spend_cap)
		VALUES ($1,decode(replace(gen_random_uuid()::text,'-',''),'hex'),'k',1000000) RETURNING id::text`, workspace)
}

func fund(t *testing.T, pool *pgxpool.Pool, workspace string, credits int64) {
	t.Helper()
	if err := ledger.GrantTopup(context.Background(), pool, workspace, credits, testNow); err != nil {
		t.Fatal(err)
	}
}

func cands(n int) []Candidate {
	cs := make([]Candidate, n)
	for i := range cs {
		cs[i] = Candidate{Ref: fmt.Sprintf("c%d", i+1), LegalName: names[i], State: "TX"}
	}
	return cs
}

func record(c Candidate) resolver.SourceRecordInput {
	return resolver.SourceRecordInput{SourceSystem: "fake", ExternalRef: c.Ref,
		Facts: resolver.RecordFacts{LegalName: c.LegalName, State: c.State}, FetchedAt: testNow}
}

// hitProvider returns one record for every candidate except those in zero/errs.
func hitProvider(cs []Candidate, zero []string, errs map[string]error) *FakeProvider {
	p := &FakeProvider{Results: map[string][]resolver.SourceRecordInput{}, Errs: errs}
	skip := map[string]bool{}
	for _, z := range zero {
		skip[z] = true
	}
	for _, c := range cs {
		if !skip[c.Ref] {
			p.Results[c.Ref] = []resolver.SourceRecordInput{record(c)}
		}
	}
	return p
}

func newService(pool *pgxpool.Pool, p Provider) *Service {
	return &Service{Pool: pool, Provider: p, Limiter: rate.NewLimiter(rate.Inf, 1),
		CacheTTL: 30 * 24 * time.Hour, Now: func() time.Time { return testNow }}
}

func assertInvariants(t *testing.T, pool *pgxpool.Pool, workspace string) {
	t.Helper()
	if err := ledger.CheckInvariants(context.Background(), pool, workspace); err != nil {
		t.Fatal(err)
	}
}

func snapshot(t *testing.T, pool *pgxpool.Pool) string {
	t.Helper()
	return scalar[string](t, pool, `SELECT (
		(SELECT COALESCE(jsonb_agg(to_jsonb(b) ORDER BY id),'[]') FROM credit_buckets b)::text ||
		(SELECT COALESCE(jsonb_agg(to_jsonb(e) ORDER BY id),'[]') FROM credit_ledger_entries e)::text ||
		(SELECT COALESCE(jsonb_agg(to_jsonb(r) ORDER BY id),'[]') FROM credit_reservations r)::text)`)
}

func seedCache(t *testing.T, pool *pgxpool.Pool, ws string, c Candidate, fetched time.Time) {
	t.Helper()
	if _, err := pool.Exec(context.Background(), `INSERT INTO source_fetch_cache(provider,cache_key,content_hash,payload,zero_hit,fetched_at)
		VALUES ('fake',$1,'\x00',NULL,true,$2)`, cacheKey(ws, c), fetched); err != nil {
		t.Fatal(err)
	}
}

func mustRun(t *testing.T, s *Service, ws, key, apiKey string, cs []Candidate) RunResult {
	t.Helper()
	res, err := s.Run(context.Background(), ws, apiKey, key, "", cs)
	if err != nil {
		t.Fatalf("Run: %v", err)
	}
	return res
}

func TestRunIdempotentReplay(t *testing.T) {
	pool := testdb.New(t)
	ws := seedWorkspace(t, pool, ledger.Solo)
	fund(t, pool, ws, 10)
	cs := cands(3)
	p := hitProvider(cs, []string{"c3"}, nil)
	s := newService(pool, p)
	first := mustRun(t, s, ws, "same", "", cs)
	calls := p.Calls.Load()
	entries := scalar[int](t, pool, `SELECT count(*) FROM credit_ledger_entries`)
	second := mustRun(t, s, ws, "same", "", cs)
	if !reflect.DeepEqual(first, second) {
		t.Fatalf("replay differs:\n%+v\n%+v", first, second)
	}
	if p.Calls.Load() != calls {
		t.Fatal("replay called provider")
	}
	if n := scalar[int](t, pool, `SELECT count(*) FROM credit_ledger_entries`); n != entries {
		t.Fatalf("replay wrote ledger entries: %d -> %d", entries, n)
	}
	if got := scalar[int64](t, pool, `SELECT sum(committed_amount) FROM credit_reservations WHERE workspace_id=$1`, ws); got != 2 {
		t.Fatalf("charged %d, want 2", got)
	}
	assertInvariants(t, pool, ws)
}

func TestRunKeyReuseDifferentPayloadConflicts(t *testing.T) {
	pool := testdb.New(t)
	ws := seedWorkspace(t, pool, ledger.Solo)
	fund(t, pool, ws, 10)
	s := newService(pool, hitProvider(cands(3), nil, nil))
	mustRun(t, s, ws, "same", "", cands(2))
	_, err := s.Run(context.Background(), ws, "", "same", "", cands(3))
	if !errors.Is(err, ledger.ErrIdempotencyConflict) {
		t.Fatalf("err = %v, want ErrIdempotencyConflict", err)
	}
	assertInvariants(t, pool, ws)
}

func TestRunConcurrentSameKey(t *testing.T) {
	pool := testdb.New(t)
	ws := seedWorkspace(t, pool, ledger.Solo)
	fund(t, pool, ws, 10)
	cs := cands(3)
	p := hitProvider(cs, []string{"c3"}, nil)
	s := newService(pool, p)
	type out struct {
		res RunResult
		err error
	}
	outs := make([]out, 10)
	var wg sync.WaitGroup
	for i := range outs {
		wg.Add(1)
		go func() {
			defer wg.Done()
			outs[i].res, outs[i].err = s.Run(context.Background(), ws, "", "race", "", cs)
		}()
	}
	wg.Wait()
	var want *RunResult
	for i, o := range outs {
		switch {
		case errors.Is(o.err, ErrRunInProgress):
		case o.err != nil:
			t.Fatalf("goroutine %d: %v", i, o.err)
		case want == nil:
			want = &outs[i].res
		case !reflect.DeepEqual(*want, o.res):
			t.Fatalf("results differ:\n%+v\n%+v", *want, o.res)
		}
	}
	if want == nil {
		t.Fatal("no goroutine returned a result")
	}
	if got := p.Calls.Load(); got != 3 {
		t.Fatalf("provider calls = %d, want 3", got)
	}
	if got := scalar[int64](t, pool, `SELECT sum(committed_amount) FROM credit_reservations WHERE workspace_id=$1`, ws); got != 2 {
		t.Fatalf("charged %d, want 2", got)
	}
	if n := scalar[int](t, pool, `SELECT count(*) FROM enrichment_runs WHERE workspace_id=$1`, ws); n != 1 {
		t.Fatalf("run rows = %d", n)
	}
	assertInvariants(t, pool, ws)
}

func TestRunInsufficientCredits(t *testing.T) {
	pool := testdb.New(t)
	ws := seedWorkspace(t, pool, ledger.Solo)
	fund(t, pool, ws, 1)
	cs := cands(3)
	p := hitProvider(cs, nil, nil)
	s := newService(pool, p)
	_, err := s.Run(context.Background(), ws, "", "k", "", cs)
	if !errors.Is(err, ledger.ErrInsufficientCredits) {
		t.Fatalf("err = %v, want ErrInsufficientCredits", err)
	}
	if p.Calls.Load() != 0 {
		t.Fatal("provider called")
	}
	if n := scalar[int](t, pool, `SELECT count(*) FROM enrichment_runs`); n != 0 {
		t.Fatalf("run row left behind: %d", n)
	}
	// Same key can be retried after funding.
	fund(t, pool, ws, 10)
	mustRun(t, s, ws, "k", "", cs)
	assertInvariants(t, pool, ws)
}

func TestRunCachedCandidateFree(t *testing.T) {
	pool := testdb.New(t)
	ws := seedWorkspace(t, pool, ledger.Solo)
	fund(t, pool, ws, 10)
	cs := cands(3)
	p := hitProvider(cs, []string{"c3"}, nil)
	s := newService(pool, p)
	mustRun(t, s, ws, "k1", "", cs)
	calls := p.Calls.Load()
	reservations := scalar[int](t, pool, `SELECT count(*) FROM credit_reservations`)
	res := mustRun(t, s, ws, "k2", "", cs)
	if p.Calls.Load() != calls {
		t.Fatal("cached candidates called provider")
	}
	if res.Cached != 3 || res.Hits != 0 || res.ReservationID != "" || res.CreditsCommitted != 0 || res.CreditsRefunded != 0 {
		t.Fatalf("RunResult = %+v", res)
	}
	if len(res.Companies) != 2 {
		t.Fatalf("Companies = %v, want the 2 cached hits' companies", res.Companies)
	}
	if n := scalar[int](t, pool, `SELECT count(*) FROM credit_reservations`); n != reservations {
		t.Fatal("billable==0 run created a reservation")
	}
	if st := scalar[string](t, pool, `SELECT status FROM enrichment_runs WHERE id=$1`, res.RunID); st != "completed" {
		t.Fatalf("run status = %s", st)
	}
	assertInvariants(t, pool, ws)
}

func TestCacheIsolatedPerWorkspace(t *testing.T) {
	pool := testdb.New(t)
	a, b := seedWorkspace(t, pool, ledger.Solo), seedWorkspace(t, pool, ledger.Solo)
	fund(t, pool, a, 5)
	fund(t, pool, b, 5)
	cs := cands(1)
	p := hitProvider(cs, nil, nil)
	s := newService(pool, p)
	mustRun(t, s, a, "k", "", cs)
	q, err := s.Quote(context.Background(), b, cs)
	if err != nil || q.AlreadyEnriched != 0 || q.Billable != 1 {
		t.Fatalf("Quote B = %+v, %v", q, err)
	}
	calls := p.Calls.Load()
	res := mustRun(t, s, b, "k", "", cs)
	if p.Calls.Load() != calls+1 || res.Cached != 0 || res.Hits != 1 || res.CreditsCommitted != 1 {
		t.Fatalf("RunResult B = %+v calls %d->%d", res, calls, p.Calls.Load())
	}
	// Workspace A's cached row is still free for A.
	if again := mustRun(t, s, a, "k2", "", cs); again.Cached != 1 || again.ReservationID != "" {
		t.Fatalf("RunResult A again = %+v", again)
	}
	assertInvariants(t, pool, a)
	assertInvariants(t, pool, b)
}

func TestCacheKeyedByIdentity(t *testing.T) {
	pool := testdb.New(t)
	ws := seedWorkspace(t, pool, ledger.Solo)
	fund(t, pool, ws, 5)
	cs := cands(1)
	p := hitProvider(cs, nil, nil)
	s := newService(pool, p)
	mustRun(t, s, ws, "k1", "", cs)
	same := []Candidate{{Ref: "c1", LegalName: "  ZEPHYR plumbing, L.L.C. ", State: " tx "}}
	if q, err := s.Quote(context.Background(), ws, same); err != nil || q.AlreadyEnriched != 1 {
		t.Fatalf("normalized identity Quote = %+v, %v", q, err)
	}
	other := []Candidate{{Ref: "c1", LegalName: "Totally Different Holdings", State: "TX"}}
	q, err := s.Quote(context.Background(), ws, other)
	if err != nil || q.AlreadyEnriched != 0 || q.Billable != 1 {
		t.Fatalf("different identity Quote = %+v, %v", q, err)
	}
	p.Results["c1"] = []resolver.SourceRecordInput{record(other[0])}
	calls := p.Calls.Load()
	res := mustRun(t, s, ws, "k2", "", other)
	if p.Calls.Load() != calls+1 || res.Cached != 0 || res.CreditsCommitted != 1 {
		t.Fatalf("RunResult = %+v", res)
	}
	assertInvariants(t, pool, ws)
}

func TestAbandonedRunFinalizedOnRetry(t *testing.T) {
	pool := testdb.New(t)
	ws := seedWorkspace(t, pool, ledger.Solo)
	fund(t, pool, ws, 10)
	cs := cands(3)
	p := hitProvider(cs, nil, nil)
	s := newService(pool, p)
	r, err := ledger.Reserve(context.Background(), pool, ledger.ReserveRequest{WorkspaceID: ws, IdempotencyKey: "k", Amount: 3}, testNow)
	if err != nil {
		t.Fatal(err)
	}
	hash := requestHash("", cs)
	runID := scalar[string](t, pool, `INSERT INTO enrichment_runs(workspace_id,idempotency_key,candidate_count,status,reservation_id,request_hash,created_at)
		VALUES ($1,'k',3,'running',$2,$3,now()-interval '20 minutes') RETURNING id::text`, ws, r.ID, hash)
	// A fresh stuck row (not yet abandoned) still reports in-progress.
	if _, err := pool.Exec(context.Background(), `UPDATE enrichment_runs SET created_at=now() WHERE id=$1`, runID); err != nil {
		t.Fatal(err)
	}
	if _, err := s.Run(context.Background(), ws, "", "k", "", cs); !errors.Is(err, ErrRunInProgress) {
		t.Fatalf("fresh running row: err = %v, want ErrRunInProgress", err)
	}
	if _, err := pool.Exec(context.Background(), `UPDATE enrichment_runs SET created_at=now()-interval '20 minutes' WHERE id=$1`, runID); err != nil {
		t.Fatal(err)
	}
	res, err := s.Run(context.Background(), ws, "", "k", "", cs)
	if err != nil {
		t.Fatalf("retry: %v", err)
	}
	if res.RunID != runID || res.ReservationID != r.ID || res.CreditsCommitted != 0 || res.CreditsRefunded != 3 || p.Calls.Load() != 0 {
		t.Fatalf("RunResult = %+v calls=%d", res, p.Calls.Load())
	}
	if st := scalar[string](t, pool, `SELECT status FROM enrichment_runs WHERE id=$1`, runID); st != "failed" {
		t.Fatalf("run status = %s, want failed", st)
	}
	if st := scalar[string](t, pool, `SELECT status FROM credit_reservations WHERE id=$1`, r.ID); st != "refunded" {
		t.Fatalf("reservation status = %s", st)
	}
	again, err := s.Run(context.Background(), ws, "", "k", "", cs)
	if err != nil || !reflect.DeepEqual(again, res) {
		t.Fatalf("replay = %+v, %v", again, err)
	}
	assertInvariants(t, pool, ws)
}

func TestRunKeyReuseSameCountDifferentPayloadConflicts(t *testing.T) {
	pool := testdb.New(t)
	ws := seedWorkspace(t, pool, ledger.Solo)
	fund(t, pool, ws, 10)
	cs := cands(2)
	p := hitProvider(cands(4), nil, nil)
	s := newService(pool, p)
	mustRun(t, s, ws, "same", "", cs)
	calls := p.Calls.Load()
	ctx := context.Background()
	if _, err := s.Run(ctx, ws, "", "same", "", cands(4)[2:]); !errors.Is(err, ledger.ErrIdempotencyConflict) {
		t.Fatalf("different refs: err = %v, want ErrIdempotencyConflict", err)
	}
	box := scalar[string](t, pool, `INSERT INTO buy_boxes(workspace_id,name,criteria) VALUES ($1,'b','{"states_in":["TX"]}') RETURNING id::text`, ws)
	if _, err := s.Run(ctx, ws, "", "same", box, cs); !errors.Is(err, ledger.ErrIdempotencyConflict) {
		t.Fatalf("different buy box: err = %v, want ErrIdempotencyConflict", err)
	}
	// Reordered identical payload still replays.
	if res, err := s.Run(ctx, ws, "", "same", "", []Candidate{cs[1], cs[0]}); err != nil || res.Hits != 2 {
		t.Fatalf("reordered replay = %+v, %v", res, err)
	}
	if p.Calls.Load() != calls {
		t.Fatal("conflict or replay called provider")
	}
	assertInvariants(t, pool, ws)
}

// A limiter that cannot grant a second token within runTimeout stops the loop early.
func TestRunLimiterStopSettlesPartial(t *testing.T) {
	pool := testdb.New(t)
	ws := seedWorkspace(t, pool, ledger.Solo)
	fund(t, pool, ws, 10)
	cs := cands(4)
	p := hitProvider(cs, nil, nil)
	svc := newService(pool, p)
	svc.Limiter = rate.NewLimiter(rate.Every(time.Hour), 1)
	res, err := svc.Run(context.Background(), ws, "", "k", "", cs)
	if err == nil {
		t.Fatal("want limiter error")
	}
	if res.Hits != 1 || res.Unprocessed != 3 || res.Status != "partial" || res.CreditsCommitted != 1 || res.CreditsRefunded != 3 || p.Calls.Load() != 1 {
		t.Fatalf("RunResult = %+v calls=%d", res, p.Calls.Load())
	}
	if sum := res.Hits + res.ZeroHits + res.Failed + res.Cached + res.Unprocessed; sum != len(cs) {
		t.Fatalf("invariant: counters sum to %d, want %d", sum, len(cs))
	}
	if n := scalar[int](t, pool, `SELECT count(*) FROM credit_reservations WHERE status='reserved'`); n != 0 {
		t.Fatalf("%d reservations still reserved", n)
	}
	if st := scalar[string](t, pool, `SELECT status FROM enrichment_runs WHERE id=$1`, res.RunID); st != "partial" {
		t.Fatalf("run status = %s", st)
	}
	if n := scalar[int](t, pool, `SELECT count(*) FROM source_fetch_cache WHERE cache_key=$1 AND NOT zero_hit`, cacheKey(ws, cs[0])); n != 1 {
		t.Fatal("delivered hit not persisted")
	}
	// Replay returns the stored partial result without new lookups.
	again, err := svc.Run(context.Background(), ws, "", "k", "", cs)
	if err != nil || again.Status != "partial" || again.Unprocessed != 3 || p.Calls.Load() != 1 {
		t.Fatalf("replay = %+v, %v", again, err)
	}
	assertInvariants(t, pool, ws)
}

// A client disconnect (canceled request ctx) must not abort a run that holds a reservation.
func TestRunSurvivesClientCancel(t *testing.T) {
	pool := testdb.New(t)
	ws := seedWorkspace(t, pool, ledger.Solo)
	fund(t, pool, ws, 10)
	cs := cands(4)
	p := hitProvider(cs, nil, nil)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	p.OnLookup = func(_ context.Context, c Candidate) {
		if c.Ref == "c1" {
			cancel()
		}
	}
	res, err := newService(pool, p).Run(ctx, ws, "", "k", "", cs)
	if err != nil || res.Status != "completed" || res.Hits != 4 || res.Unprocessed != 0 || res.CreditsCommitted != 4 {
		t.Fatalf("RunResult = %+v, %v", res, err)
	}
	assertInvariants(t, pool, ws)
}

func TestRunCandidateCap(t *testing.T) {
	pool := testdb.New(t)
	ws := seedWorkspace(t, pool, ledger.Solo)
	fund(t, pool, ws, 10)
	big := make([]Candidate, MaxCandidates+1)
	for i := range big {
		big[i] = Candidate{Ref: fmt.Sprintf("r%d", i), LegalName: fmt.Sprintf("Co %d", i)}
	}
	p := &FakeProvider{}
	s := newService(pool, p)
	if _, err := s.Run(context.Background(), ws, "", "k", "", big); !errors.Is(err, ErrTooManyCandidates) || !errors.Is(err, ErrInvalidRequest) {
		t.Fatalf("Run err = %v", err)
	}
	if _, err := s.Quote(context.Background(), ws, big); !errors.Is(err, ErrTooManyCandidates) {
		t.Fatalf("Quote err = %v", err)
	}
	// Duplicate refs collapse before the cap applies.
	dups := append(append([]Candidate{}, big[:MaxCandidates]...), big[0])
	if _, err := s.Quote(context.Background(), ws, dups); err != nil {
		t.Fatalf("500 distinct + dup: %v", err)
	}
	if p.Calls.Load() != 0 || scalar[int](t, pool, `SELECT count(*) FROM enrichment_runs`) != 0 {
		t.Fatal("rejected request had side effects")
	}
}

// Two refs with the same identity cost one lookup and one credit; both are reported.
func TestRunBillableDedupeByIdentity(t *testing.T) {
	pool := testdb.New(t)
	ws := seedWorkspace(t, pool, ledger.Solo)
	fund(t, pool, ws, 10)
	a := Candidate{Ref: "a", LegalName: "Zephyr Plumbing LLC", State: "TX"}
	b := Candidate{Ref: "b", LegalName: "zephyr plumbing llc", State: "tx"}
	c := Candidate{Ref: "c", LegalName: "Quartz Dental Group", State: "TX"}
	p := hitProvider([]Candidate{a, c}, nil, nil)
	s := newService(pool, p)
	q, err := s.Quote(context.Background(), ws, []Candidate{a, b, c})
	if err != nil || q.Billable != 2 || q.CreditsRequired != 2 {
		t.Fatalf("quote = %+v, %v", q, err)
	}
	res := mustRun(t, s, ws, "k", "", []Candidate{a, b, c})
	if p.Calls.Load() != 2 || res.CreditsCommitted != 2 || res.CreditsRefunded != 0 || res.Hits != 3 || res.Unprocessed != 0 {
		t.Fatalf("RunResult = %+v calls=%d", res, p.Calls.Load())
	}
	if len(res.Companies) != 2 {
		t.Fatalf("companies = %v", res.Companies)
	}
	assertInvariants(t, pool, ws)
}

func TestRunStaleReservationReleasedMidRun(t *testing.T) {
	pool := testdb.New(t)
	ws := seedWorkspace(t, pool, ledger.Solo)
	fund(t, pool, ws, 10)
	cs := cands(3)
	p := hitProvider(cs, nil, nil)
	p.OnLookup = func(ctx context.Context, c Candidate) {
		if c.Ref != "c2" {
			return
		}
		id := scalar[string](t, pool, `SELECT id::text FROM credit_reservations WHERE workspace_id=$1`, ws)
		if _, err := ledger.Refund(ctx, pool, id, testNow); err != nil {
			t.Error(err)
		}
	}
	res := mustRun(t, newService(pool, p), ws, "k", "", cs)
	if res.Hits != 3 || res.CreditsCommitted != 0 || res.CreditsRefunded != 3 {
		t.Fatalf("RunResult = %+v", res)
	}
	if n := scalar[int](t, pool, `SELECT count(*) FROM source_fetch_cache WHERE NOT zero_hit`); n != 3 {
		t.Fatalf("hits not delivered: %d cache rows", n)
	}
	assertInvariants(t, pool, ws)
}

func TestRunValidation(t *testing.T) {
	pool := testdb.New(t)
	ws, other := seedWorkspace(t, pool, ledger.Solo), seedWorkspace(t, pool, ledger.Solo)
	fund(t, pool, ws, 10)
	foreignBox := scalar[string](t, pool, `INSERT INTO buy_boxes(workspace_id,name,criteria) VALUES ($1,'b','{"states_in":["TX"]}') RETURNING id::text`, other)
	ownBox := scalar[string](t, pool, `INSERT INTO buy_boxes(workspace_id,name,criteria) VALUES ($1,'b','{"states_in":["TX"]}') RETURNING id::text`, ws)
	p := hitProvider(cands(2), nil, nil)
	s := newService(pool, p)
	ctx := context.Background()
	if _, err := s.Run(ctx, ws, "", "k1", "", []Candidate{{Ref: ""}}); !errors.Is(err, ErrInvalidRequest) {
		t.Fatalf("empty ref: %v", err)
	}
	if _, err := s.Run(ctx, ws, "", "", "", cands(1)); !errors.Is(err, ErrInvalidRequest) {
		t.Fatalf("empty key: %v", err)
	}
	if _, err := s.Run(ctx, ws, "", "k2", foreignBox, cands(1)); !errors.Is(err, ErrInvalidRequest) {
		t.Fatalf("foreign buy box: %v", err)
	}
	if _, err := s.Run(ctx, ws, "", "k3", "not-a-uuid", cands(1)); !errors.Is(err, ErrInvalidRequest) {
		t.Fatalf("malformed buy box: %v", err)
	}
	if _, err := s.Run(ctx, "00000000-0000-0000-0000-000000000000", "", "k4", "", cands(1)); !errors.Is(err, ErrInvalidRequest) {
		t.Fatalf("unknown workspace: %v", err)
	}
	if _, err := s.Quote(ctx, ws, []Candidate{{Ref: ""}}); !errors.Is(err, ErrInvalidRequest) {
		t.Fatalf("quote empty ref: %v", err)
	}
	if p.Calls.Load() != 0 || scalar[int](t, pool, `SELECT count(*) FROM enrichment_runs`) != 0 {
		t.Fatal("invalid request had side effects")
	}
	dup := append(cands(2), Candidate{Ref: "c1", LegalName: "Dup"})
	res, err := s.Run(ctx, ws, "", "k5", ownBox, dup)
	if err != nil || res.Hits != 2 || res.CreditsCommitted != 2 {
		t.Fatalf("dedupe run = %+v, %v", res, err)
	}
	if n := scalar[int](t, pool, `SELECT count(*) FROM enrichment_runs WHERE buy_box_id=$1`, ownBox); n != 1 {
		t.Fatal("buy box not recorded")
	}
	assertInvariants(t, pool, ws)
	assertInvariants(t, pool, other)
}

func TestQuoteIsFree(t *testing.T) {
	pool := testdb.New(t)
	ws := seedWorkspace(t, pool, ledger.Solo)
	fund(t, pool, ws, 10)
	cs := cands(3)
	seedCache(t, pool, ws, cs[1], testNow.Add(-time.Hour))
	before := snapshot(t, pool)
	q, err := newService(pool, &FakeProvider{}).Quote(context.Background(), ws, cs)
	if err != nil {
		t.Fatal(err)
	}
	want := Quote{CandidateCount: 3, AlreadyEnriched: 1, Billable: 2, CreditsRequired: 2, CreditsAvailable: 10}
	if q != want {
		t.Fatalf("Quote = %+v, want %+v", q, want)
	}
	if after := snapshot(t, pool); after != before {
		t.Fatal("quote mutated ledger tables")
	}
	assertInvariants(t, pool, ws)
}

func TestQuoteStaleCacheIsBillable(t *testing.T) {
	pool := testdb.New(t)
	ws := seedWorkspace(t, pool, ledger.Solo)
	seedCache(t, pool, ws, cands(1)[0], testNow.Add(-30*24*time.Hour)) // exactly TTL old: not fresh
	q, err := newService(pool, &FakeProvider{}).Quote(context.Background(), ws, cands(1))
	if err != nil || q.AlreadyEnriched != 0 || q.Billable != 1 {
		t.Fatalf("Quote = %+v, %v", q, err)
	}
	assertInvariants(t, pool, ws)
}

func TestQuoteShortfallOverage(t *testing.T) {
	pool := testdb.New(t)
	ws := seedWorkspace(t, pool, ledger.Solo)
	fund(t, pool, ws, 2)
	q, err := newService(pool, &FakeProvider{}).Quote(context.Background(), ws, cands(5))
	if err != nil {
		t.Fatal(err)
	}
	if q.Billable != 5 || q.CreditsAvailable != 2 || q.Shortfall != 3 || q.OverageEstimateCents != 180 {
		t.Fatalf("Quote = %+v", q)
	}
	assertInvariants(t, pool, ws)
}

func TestRunCommitsHitsRefundsZeroHits(t *testing.T) {
	pool := testdb.New(t)
	ws := seedWorkspace(t, pool, ledger.Solo)
	fund(t, pool, ws, 10)
	cs := cands(3)
	p := hitProvider(cs, []string{"c3"}, nil)
	res := mustRun(t, newService(pool, p), ws, "k1", seedKey(t, pool, ws), cs)
	if res.Hits != 2 || res.ZeroHits != 1 || res.Failed != 0 || res.Cached != 0 ||
		res.CreditsCommitted != 2 || res.CreditsRefunded != 1 || len(res.Companies) < 2 {
		t.Fatalf("RunResult = %+v", res)
	}
	if !sort.StringsAreSorted(res.Companies) {
		t.Fatalf("Companies unsorted: %v", res.Companies)
	}
	if got := scalar[int64](t, pool, `SELECT sum(balance_remaining) FROM credit_buckets WHERE workspace_id=$1`, ws); got != 8 {
		t.Fatalf("balance = %d, want 8", got)
	}
	if st := scalar[string](t, pool, `SELECT status FROM enrichment_runs WHERE id=$1`, res.RunID); st != "completed" {
		t.Fatalf("run status = %s", st)
	}
	if n := scalar[int](t, pool, `SELECT count(*) FROM source_fetch_cache WHERE zero_hit AND cache_key=$1 AND payload IS NULL`, cacheKey(ws, cs[2])); n != 1 {
		t.Fatalf("zero-hit cache rows = %d", n)
	}
	if n := scalar[int](t, pool, `SELECT count(*) FROM source_fetch_cache WHERE NOT zero_hit AND payload IS NOT NULL`); n != 2 {
		t.Fatalf("hit cache rows = %d", n)
	}
	assertInvariants(t, pool, ws)
}

func TestRunRefundsFailures(t *testing.T) {
	pool := testdb.New(t)
	ws := seedWorkspace(t, pool, ledger.Solo)
	fund(t, pool, ws, 10)
	cs := cands(3)
	p := hitProvider(cs, nil, map[string]error{"c2": errors.New("boom")})
	res := mustRun(t, newService(pool, p), ws, "k1", "", cs)
	if res.Hits != 2 || res.Failed != 1 || res.CreditsCommitted != 2 || res.CreditsRefunded != 1 {
		t.Fatalf("RunResult = %+v", res)
	}
	if st := scalar[string](t, pool, `SELECT status FROM enrichment_runs WHERE id=$1`, res.RunID); st != "completed" {
		t.Fatalf("run status = %s", st)
	}
	if n := scalar[int](t, pool, `SELECT count(*) FROM source_fetch_cache WHERE cache_key=$1`, cacheKey(ws, cs[1])); n != 0 {
		t.Fatal("failed lookup wrote cache")
	}
	assertInvariants(t, pool, ws)
}

func TestRunAllFailedRefundsAll(t *testing.T) {
	pool := testdb.New(t)
	ws := seedWorkspace(t, pool, ledger.Solo)
	fund(t, pool, ws, 10)
	cs := cands(2)
	p := &FakeProvider{Errs: map[string]error{"c1": errors.New("x"), "c2": errors.New("y")}}
	res := mustRun(t, newService(pool, p), ws, "k1", "", cs)
	if res.Failed != 2 || res.CreditsCommitted != 0 || res.CreditsRefunded != 2 {
		t.Fatalf("RunResult = %+v", res)
	}
	if st := scalar[string](t, pool, `SELECT status FROM enrichment_runs WHERE id=$1`, res.RunID); st != "failed" {
		t.Fatalf("run status = %s", st)
	}
	if st := scalar[string](t, pool, `SELECT status FROM credit_reservations WHERE id=$1`, res.ReservationID); st != "refunded" {
		t.Fatalf("reservation status = %s", st)
	}
	assertInvariants(t, pool, ws)
}

// If recovery finalized the row while the run was live, finish returns the stored result.
func TestFinishDoesNotOverwriteFinalizedRun(t *testing.T) {
	pool := testdb.New(t)
	ws := seedWorkspace(t, pool, ledger.Solo)
	fund(t, pool, ws, 10)
	cs := cands(2)
	p := hitProvider(cs, nil, nil)
	p.OnLookup = func(ctx context.Context, c Candidate) {
		if c.Ref != "c1" {
			return
		}
		if _, err := pool.Exec(ctx, `UPDATE enrichment_runs SET status='failed', result='{"run_id":"x","status":"failed","companies":[]}'`); err != nil {
			t.Error(err)
		}
	}
	res, err := newService(pool, p).Run(context.Background(), ws, "", "k", "", cs)
	if err != nil || res.Status != "failed" || res.RunID != "x" {
		t.Fatalf("res = %+v, %v", res, err)
	}
	if st := scalar[string](t, pool, `SELECT status FROM enrichment_runs`); st != "failed" {
		t.Fatalf("status = %s", st)
	}
	assertInvariants(t, pool, ws)
}
