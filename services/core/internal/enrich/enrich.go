// Package enrich quotes and runs metered enrichment lookups against the credit ledger.
package enrich

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"sort"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgxpool"
	"golang.org/x/time/rate"

	"rowan.ai/rowan/services/core/internal/ledger"
	"rowan.ai/rowan/services/core/internal/resolver"
)

var (
	ErrInvalidRequest = errors.New("invalid enrichment request")
	// ErrRunInProgress means another call with the same idempotency key has not finished.
	ErrRunInProgress = errors.New("enrichment run in progress")
	// ErrTooManyCandidates is an ErrInvalidRequest: one request may not exceed MaxCandidates distinct refs.
	ErrTooManyCandidates = fmt.Errorf("too many candidates (max %d): %w", MaxCandidates, ErrInvalidRequest)
)

// MaxCandidates caps distinct candidate refs per Quote or Run so one request stays inside runTimeout.
const MaxCandidates = 500

// runTimeout bounds one Run.
// ponytail: must stay under the 15m ledger.ReleaseStale window, or the sweeper can refund a live reservation.
// Settle then reports ErrIdempotencyConflict and Run delivers the hits free instead of charging.
const runTimeout = 10 * time.Minute

type Candidate struct {
	Ref, LegalName, State string
	IDs                   map[string]string
}

// Provider looks up one candidate. An empty slice with nil error is a zero-hit.
type Provider interface {
	Name() string
	Lookup(ctx context.Context, c Candidate) ([]resolver.SourceRecordInput, error)
}

type Quote struct {
	CandidateCount, AlreadyEnriched, Billable int
	CreditsRequired, CreditsAvailable         int64
	Shortfall, OverageEstimateCents           int64
}

type Service struct {
	Pool     *pgxpool.Pool
	Provider Provider
	Limiter  *rate.Limiter
	CacheTTL time.Duration
	Now      func() time.Time
}

type RunResult struct {
	RunID         string `json:"run_id"`
	ReservationID string `json:"reservation_id"`
	Hits          int    `json:"hits"`
	ZeroHits      int    `json:"zero_hits"`
	Failed        int    `json:"failed"`
	Cached        int    `json:"cached"`
	// Unprocessed counts candidates never attempted because the run stopped early.
	// Hits+ZeroHits+Failed+Cached+Unprocessed always equals the deduped candidate count.
	Unprocessed int `json:"unprocessed"`
	// Status is "completed", "partial" (stopped early), or "failed" (every lookup failed).
	Status           string   `json:"status"`
	CreditsCommitted int64    `json:"credits_committed"`
	CreditsRefunded  int64    `json:"credits_refunded"`
	Companies        []string `json:"companies"`
}

func (s *Service) now() time.Time {
	if s.Now != nil {
		return s.Now().UTC()
	}
	return time.Now().UTC()
}

// classify maps malformed-UUID input to ErrInvalidRequest.
func classify(op string, err error) error {
	var pgErr *pgconn.PgError
	if errors.Is(err, pgx.ErrNoRows) || (errors.As(err, &pgErr) && pgErr.Code == "22P02") {
		return fmt.Errorf("enrich: %s: %w: %w", op, ErrInvalidRequest, err)
	}
	return fmt.Errorf("enrich: %s: %w", op, err)
}

// dedupe keeps the first candidate per Ref and rejects empty refs.
func dedupe(cs []Candidate) ([]Candidate, error) {
	seen := make(map[string]bool, len(cs))
	out := make([]Candidate, 0, len(cs))
	for _, c := range cs {
		if c.Ref == "" {
			return nil, fmt.Errorf("enrich: empty candidate ref: %w", ErrInvalidRequest)
		}
		if !seen[c.Ref] {
			seen[c.Ref] = true
			out = append(out, c)
		}
	}
	if len(out) > MaxCandidates {
		return nil, fmt.Errorf("enrich: %d candidates: %w", len(out), ErrTooManyCandidates)
	}
	return out, nil
}

func (s *Service) tier(ctx context.Context, workspaceID string) (ledger.Tier, error) {
	var tier ledger.Tier
	if err := s.Pool.QueryRow(ctx, `SELECT tier FROM workspaces WHERE id=$1`, workspaceID).Scan(&tier); err != nil {
		return "", classify("workspace", err)
	}
	return tier, nil
}

// Quote is read-only: it never writes credit tables, cache, or runs.
func (s *Service) Quote(ctx context.Context, workspaceID string, cs []Candidate) (Quote, error) {
	cs, err := dedupe(cs)
	if err != nil {
		return Quote{}, err
	}
	tier, err := s.tier(ctx, workspaceID)
	if err != nil {
		return Quote{}, err
	}
	now := s.now()
	cached, err := s.freshCache(ctx, cacheKeys(workspaceID, cs), now)
	if err != nil {
		return Quote{}, err
	}
	billable, already := 0, 0
	for _, g := range groupByIdentity(workspaceID, cs) {
		if _, ok := cached[g.key]; ok {
			already += g.n
		} else {
			billable++
		}
	}
	available, err := ledger.Available(ctx, s.Pool, workspaceID, now)
	if err != nil {
		return Quote{}, err
	}
	_, _, overage := ledger.TierSpec(tier)
	q := Quote{CandidateCount: len(cs), AlreadyEnriched: already, Billable: billable, CreditsAvailable: available}
	q.CreditsRequired = int64(q.Billable)
	q.Shortfall = max(0, q.CreditsRequired-available)
	q.OverageEstimateCents = q.Shortfall * overage
	return q, nil
}

// abandonedAfter is how long a 'running' row may sit before a retry treats its run as dead.
// ponytail: equals the ledger.ReleaseStale window; the 10m runTimeout guarantees a live run is never this old.
const abandonedAfter = 15 * time.Minute

// claimRun inserts the run row. A non-nil stored result is an idempotent replay.
func (s *Service) claimRun(ctx context.Context, workspaceID, idemKey, buyBoxID string, cs []Candidate) (string, *RunResult, error) {
	hash := requestHash(buyBoxID, cs)
	for range 3 {
		var id string
		err := s.Pool.QueryRow(ctx, `INSERT INTO enrichment_runs(workspace_id,idempotency_key,buy_box_id,candidate_count,status,request_hash)
			VALUES ($1,$2,NULLIF($3,'')::uuid,$4,'running',$5)
			ON CONFLICT (workspace_id, idempotency_key) DO NOTHING RETURNING id::text`,
			workspaceID, idemKey, buyBoxID, len(cs), hash).Scan(&id)
		if err == nil {
			return id, nil, nil
		}
		if !errors.Is(err, pgx.ErrNoRows) {
			return "", nil, classify("claim run", err)
		}
		var existing int
		var raw, storedHash []byte
		var runID, reservationID string
		var abandoned bool
		err = s.Pool.QueryRow(ctx, `SELECT id::text, candidate_count, request_hash, result, COALESCE(reservation_id::text,''),
			created_at < now() - make_interval(secs => $3)
			FROM enrichment_runs WHERE workspace_id=$1 AND idempotency_key=$2`,
			workspaceID, idemKey, abandonedAfter.Seconds()).Scan(&runID, &existing, &storedHash, &raw, &reservationID, &abandoned)
		if errors.Is(err, pgx.ErrNoRows) {
			continue // the winner failed to reserve and deleted its row; claim again
		}
		if err != nil {
			return "", nil, fmt.Errorf("enrich: load run: %w", err)
		}
		if existing != len(cs) || (storedHash != nil && !bytes.Equal(storedHash, hash)) {
			return "", nil, fmt.Errorf("enrich: run key reused with different request: %w", ledger.ErrIdempotencyConflict)
		}
		if raw == nil {
			if !abandoned {
				return "", nil, ErrRunInProgress
			}
			res, err := s.recoverAbandoned(ctx, runID, reservationID)
			if err != nil {
				return "", nil, err
			}
			return "", &res, nil
		}
		var stored RunResult
		if err := json.Unmarshal(raw, &stored); err != nil {
			return "", nil, fmt.Errorf("enrich: decode stored run: %w", err)
		}
		return "", &stored, nil
	}
	return "", nil, ErrRunInProgress
}

// recoverAbandoned refunds a dead run's reservation and finalizes its row as failed.
func (s *Service) recoverAbandoned(ctx context.Context, runID, reservationID string) (RunResult, error) {
	ctx = context.WithoutCancel(ctx)
	res := RunResult{RunID: runID, ReservationID: reservationID, Companies: []string{}, Status: "failed"}
	if reservationID != "" {
		_, err := ledger.Settle(ctx, s.Pool, reservationID, 0, s.now())
		if err != nil && !errors.Is(err, ledger.ErrIdempotencyConflict) {
			return RunResult{}, fmt.Errorf("enrich: refund abandoned run: %w", err)
		}
		// Conflict means the reservation was already committed: report what was charged.
		var amount, committed int64
		if err := s.Pool.QueryRow(ctx, `SELECT amount, committed_amount FROM credit_reservations WHERE id=$1`, reservationID).Scan(&amount, &committed); err != nil {
			return RunResult{}, fmt.Errorf("enrich: load abandoned reservation: %w", err)
		}
		res.CreditsCommitted, res.CreditsRefunded = committed, amount-committed
	}
	raw, err := json.Marshal(res)
	if err != nil {
		return RunResult{}, fmt.Errorf("enrich: marshal result: %w", err)
	}
	if _, err := s.Pool.Exec(ctx, `UPDATE enrichment_runs SET status='failed', result=$2, finished_at=$3 WHERE id=$1 AND result IS NULL`,
		runID, raw, s.now()); err != nil {
		return RunResult{}, fmt.Errorf("enrich: finalize abandoned run: %w", err)
	}
	return res, nil
}

// dropRun deletes a run row that never reserved credits.
func (s *Service) dropRun(ctx context.Context, runID string) error {
	if _, err := s.Pool.Exec(context.WithoutCancel(ctx), `DELETE FROM enrichment_runs WHERE id=$1 AND reservation_id IS NULL`, runID); err != nil {
		return fmt.Errorf("enrich: drop run: %w", err)
	}
	return nil
}

// Run reserves one credit per uncached candidate, enriches, commits hits, and refunds the rest.
// Reusing idemKey replays the stored result without calling the provider or the ledger.
func (s *Service) Run(ctx context.Context, workspaceID, apiKeyID, idemKey, buyBoxID string, cs []Candidate) (RunResult, error) {
	if idemKey == "" {
		return RunResult{}, fmt.Errorf("enrich: idempotency key required: %w", ErrInvalidRequest)
	}
	cs, err := dedupe(cs)
	if err != nil {
		return RunResult{}, err
	}
	if _, err := s.tier(ctx, workspaceID); err != nil {
		return RunResult{}, err
	}
	if buyBoxID != "" {
		var ok bool
		err := s.Pool.QueryRow(ctx, `SELECT EXISTS (SELECT 1 FROM buy_boxes WHERE id=$1 AND workspace_id=$2)`, buyBoxID, workspaceID).Scan(&ok)
		if err != nil {
			return RunResult{}, classify("buy box", err)
		}
		if !ok {
			return RunResult{}, fmt.Errorf("enrich: buy box not in workspace: %w", ErrInvalidRequest)
		}
	}
	// A client disconnect must not abort a run that already holds a reservation; only runTimeout bounds it.
	// ponytail: a disconnected client learns the outcome by replaying the same Idempotency-Key.
	ctx, cancel := context.WithTimeout(context.WithoutCancel(ctx), runTimeout)
	defer cancel()
	runID, stored, err := s.claimRun(ctx, workspaceID, idemKey, buyBoxID, cs)
	if err != nil {
		return RunResult{}, err
	}
	if stored != nil {
		return *stored, nil
	}
	return s.execute(ctx, workspaceID, apiKeyID, idemKey, runID, cs)
}

// group is one distinct lookup: the first candidate plus how many refs share its identity.
type group struct {
	key string
	c   Candidate
	n   int
}

// groupByIdentity collapses candidates with the same cacheKey so each identity is looked up and billed once.
func groupByIdentity(workspaceID string, cs []Candidate) []group {
	idx := map[string]int{}
	var out []group
	for _, c := range cs {
		k := cacheKey(workspaceID, c)
		if i, ok := idx[k]; ok {
			out[i].n++
			continue
		}
		idx[k] = len(out)
		out = append(out, group{key: k, c: c, n: 1})
	}
	return out
}

func (s *Service) execute(ctx context.Context, workspaceID, apiKeyID, idemKey, runID string, cs []Candidate) (RunResult, error) {
	now := s.now()
	groups := groupByIdentity(workspaceID, cs)
	keys := make([]string, len(groups))
	for i, g := range groups {
		keys[i] = g.key
	}
	cached, err := s.freshCache(ctx, keys, now)
	if err != nil {
		return RunResult{}, errors.Join(err, s.dropRun(ctx, runID))
	}
	var billable, hitCached []group
	res := RunResult{RunID: runID, Companies: []string{}}
	for _, g := range groups {
		row, ok := cached[g.key]
		switch {
		case !ok:
			billable = append(billable, g)
		case row.zeroHit:
			res.Cached += g.n
		default:
			hitCached = append(hitCached, g)
		}
	}
	if len(billable) > 0 {
		r, err := ledger.Reserve(ctx, s.Pool, ledger.ReserveRequest{WorkspaceID: workspaceID, APIKeyID: apiKeyID,
			IdempotencyKey: idemKey, Amount: int64(len(billable))}, now)
		if err == nil && r.Status != "reserved" {
			err = fmt.Errorf("enrich: reservation for key already settled: %w", ledger.ErrIdempotencyConflict)
		}
		if err != nil {
			return RunResult{}, errors.Join(err, s.dropRun(ctx, runID))
		}
		res.ReservationID = r.ID
		// Persist the link at once so a crash leaves a recoverable row.
		if _, err := s.Pool.Exec(ctx, `UPDATE enrichment_runs SET reservation_id=$2 WHERE id=$1`, runID, r.ID); err != nil {
			_, refundErr := ledger.Settle(context.WithoutCancel(ctx), s.Pool, r.ID, 0, now)
			return RunResult{}, errors.Join(fmt.Errorf("enrich: link reservation: %w", err), refundErr, s.dropRun(ctx, runID))
		}
	}
	companies := map[string]struct{}{}
	add := func(ids []string) {
		for _, id := range ids {
			companies[id] = struct{}{}
		}
	}
	var runErr error
	// Lookup-level counters drive the ledger; res counters are per ref.
	lookupHits, lookupFailed, cachedHits := 0, 0, 0
	for _, g := range hitCached {
		if runErr = ctx.Err(); runErr != nil {
			break
		}
		ids, err := s.resolveCached(ctx, workspaceID, cached[g.key].payload, now)
		if err != nil {
			res.Failed += g.n
			continue
		}
		add(ids)
		res.Cached += g.n
		cachedHits++
	}
	for _, g := range billable {
		if runErr != nil {
			break
		}
		if runErr = ctx.Err(); runErr != nil {
			break
		}
		if s.Limiter != nil {
			if err := s.Limiter.Wait(ctx); err != nil {
				runErr = err
				break
			}
		}
		recs, err := s.Provider.Lookup(ctx, g.c)
		if err != nil {
			if runErr = ctx.Err(); runErr != nil {
				break
			}
			res.Failed += g.n
			lookupFailed++
			continue
		}
		// A delivered answer is persisted even if ctx was canceled during the lookup.
		ids, err := s.storeLookup(context.WithoutCancel(ctx), workspaceID, g.key, recs, s.now())
		switch {
		case err != nil:
			res.Failed += g.n
			lookupFailed++
		case len(recs) == 0:
			res.ZeroHits += g.n
		default:
			res.Hits += g.n
			lookupHits++
			add(ids)
		}
	}
	res.Unprocessed = len(cs) - (res.Hits + res.ZeroHits + res.Failed + res.Cached)
	return s.finish(ctx, res, companies, len(billable), lookupHits, lookupFailed, cachedHits, runErr)
}

// finish settles credits and finalizes the run row; it runs to completion even when ctx is done.
func (s *Service) finish(ctx context.Context, res RunResult, companies map[string]struct{}, billable, lookupHits, billableFailed, cachedHits int, runErr error) (RunResult, error) {
	ctx = context.WithoutCancel(ctx)
	if res.ReservationID != "" {
		settled, err := ledger.Settle(ctx, s.Pool, res.ReservationID, int64(lookupHits), s.now())
		switch {
		case err == nil:
			res.CreditsCommitted = settled.CommittedAmount
		case errors.Is(err, ledger.ErrIdempotencyConflict):
			// ReleaseStale already refunded the reservation: hits are delivered free.
			res.CreditsCommitted = 0
		default:
			return res, fmt.Errorf("enrich: settle: %w", err)
		}
		res.CreditsRefunded = int64(billable) - res.CreditsCommitted
	}
	for id := range companies {
		res.Companies = append(res.Companies, id)
	}
	sort.Strings(res.Companies)
	res.Status = "completed"
	switch {
	case res.Unprocessed > 0 || runErr != nil:
		res.Status = "partial"
	case billable > 0 && billableFailed == billable && cachedHits == 0:
		res.Status = "failed"
	}
	raw, err := json.Marshal(res)
	if err != nil {
		return res, fmt.Errorf("enrich: marshal result: %w", err)
	}
	var id string
	err = s.Pool.QueryRow(ctx, `UPDATE enrichment_runs SET reservation_id=NULLIF($2,'')::uuid, status=$3,
		hit_count=$4, zero_hit_count=$5, failed_count=$6, result=$7, finished_at=$8 WHERE id=$1 AND result IS NULL RETURNING id::text`,
		res.RunID, res.ReservationID, res.Status, res.Hits, res.ZeroHits, res.Failed, raw, s.now()).Scan(&id)
	if errors.Is(err, pgx.ErrNoRows) {
		// Another path (abandoned-run recovery) already finalized the row: its result is authoritative.
		var stored RunResult
		var rawStored []byte
		if err := s.Pool.QueryRow(ctx, `SELECT result FROM enrichment_runs WHERE id=$1`, res.RunID).Scan(&rawStored); err != nil {
			return res, fmt.Errorf("enrich: load finalized run: %w", err)
		}
		if err := json.Unmarshal(rawStored, &stored); err != nil {
			return res, fmt.Errorf("enrich: decode finalized run: %w", err)
		}
		return stored, nil
	}
	if err != nil {
		return res, fmt.Errorf("enrich: finalize run: %w", err)
	}
	// A persisted result is authoritative; an interrupted run is represented by its status.
	return res, nil
}
