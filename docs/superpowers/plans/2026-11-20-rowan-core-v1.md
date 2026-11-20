# Rowan Core Service (v1) Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Build the Go core service + PostgreSQL schema that owns every Rowan v1 domain invariant (credit ledger, entity resolution, Buy-Box evaluation, post-sale proxy, export gating) and exposes the agent API, with validation tests proving the PRD §7 success criteria.

**Architecture:** One Go module at repo root (`rowan.ai/rowan`). `services/core` holds a stdlib `net/http` API server and focused `internal/*` packages; all state in PostgreSQL via `pgx/v5`, every mutation in explicit transactions. Enrichment runs synchronously inside the request behind a `Provider` seam (fake in tests, real connectors in Plan 4). Export gating is enforced in core (single enforcement point); the TS BFF in Plan 2 passes through.

**Tech Stack:** Go 1.26, PostgreSQL 17 (Docker), `github.com/jackc/pgx/v5`, `golang.org/x/time/rate`. No ORM, no migration framework, no router library.

**Spec:** `PRD.md`, `Architecture.md`, `AGENTS.md` (repo root). Executors read all three.

## Program Roadmap (separate plans)

| Plan | Scope | Depends on |
| --- | --- | --- |
| **1 (this)** | Go core: schema, ledger, resolver, evaluator, proxy, enrichment run, export, agent API, e2e validation | — |
| 2 | `apps/bff` (TS): user auth/sessions, API-key management, typed client to core, pass-through export | 1 |
| 3 | `apps/web` (TS): Buy-Box builder, quote/run views, lead explorer, citation inspector, review queue, split UI, export center | 2 |
| 4 | Real connectors: SAM.gov, bulk registries, licensing boards, polite fetcher (`robots.txt`, ETag cache, per-source limits), commercial adapters, post-sale signal ingestion | 1 |
| 5 | Billing: Stripe subscriptions, monthly grant scheduler, overage purchase flow | 1 |

## Global Constraints

- Merge thresholds: auto-merge `score >= 0.85`; review `0.55 <= score < 0.85`; distinct `score < 0.55`.
- `data_quality_status` ∈ {`verified_filing`, `commercial_estimate`, `scraped_web`, `missing`}; `origin_type` ∈ {`raw_source`, `derived_enrichment`}. Never dropped from citations, evaluations, API responses, or exports.
- Criterion status ∈ {`MET`, `NOT_MET`, `UNVERIFIED_LOW_CONFIDENCE`}. Only `verified_filing` data may yield `MET`/`NOT_MET`.
- Tiers: `solo` 500 credits / 60-day lots / 60¢ overage; `deal_team` 2500 / 90-day / 45¢; `fund` 7500 / 90-day / 35¢.
- FIFO consumption order: `rollover` → `included` → `topup`; within a type, earliest `expires_at` first, then `id`.
- Discovery, search, and quotes cost `0` credits and never write `credit_buckets` or `credit_ledger_entries`.
- `Idempotency-Key` required on every billable/mutating endpoint; same key + same payload replays; same key + different payload → `409`.
- Credits are `bigint`; USD amounts are integer cents. No floats for money.
- Post-sale proxy output always labeled `post_sale_proxy` unless a `verified_filing` transfer signal exists (`confirmed_transfer`).
- Outputs are "thesis match + cited facts"; no field, label, or copy may imply financial/legal/tax/investment advice.
- Go: `context.Context` first param on every I/O function; errors wrapped with `fmt.Errorf("op: %w", err)`; parameterized SQL only.

## Decisions & Assumptions (flag to user; not in spec)

1. 1 credit per billable candidate lookup. Already-enriched candidates (cache hit within 30 days) cost 0.
2. Top-up lots expire 365 days after purchase.
3. At a new period grant, prior `included` lots with balance become `rollover` (keep original `expires_at`).
4. Spend cap = per API key, per UTC calendar month, counting `reserved + committed` credits.
5. Insufficient credits → run rejected `402` with shortfall + overage estimate. No auto-overage charge (Plan 5).
6. Refunds return to the exact lots they were reserved from, even if since expired; the next expiry sweep zeroes them with an `expire` entry.
7. Reservations older than 15 minutes still `reserved` are refunded by `ReleaseStale` (crash recovery).
8. Review-band records become their own canonical company flagged `pending_review`, plus a `possible_match_candidates` row.
9. Exports cost 0 credits in v1 but are always audited.
10. `rowan/` currently lives untracked inside the parent `Projects/Current` git repo; Task 1 runs `git init` here so Rowan has its own history.

## Review Focus

1. **Concurrent runs with different keys draining one workspace** → total reserved never exceeds available; losers get `ErrInsufficientCredits`, no negative balances. (Task 4, `TestReserveConcurrentNoOverdraft`)
2. **Crash between reserve and commit** → stale reservation refunded, ledger conserves. (Task 5, `TestReleaseStaleRefunds`)
3. **Refund lands in a lot that expired mid-run** → next sweep expires it; conservation invariant holds. (Task 5, `TestRefundIntoExpiredLotThenSweep`)
4. **Split request naming every record, or a record not linked to that canonical** → `ErrInvalidSplit`, transaction rolled back, nothing changed. (Task 9, `TestSplitRejectsInvalidSelection`)
5. **Same `Idempotency-Key` used by two workspaces** → keys are workspace-scoped; neither sees the other's response. (Task 13, `TestIdempotencyScopedPerWorkspace`)

---

## File Structure

```text
go.mod                                  module rowan.ai/rowan
docker-compose.yml                      postgres:17-alpine on 54329
Makefile                                db-up, test, test-short, run
migrations/embed.go                     package migrations; //go:embed *.sql
migrations/0001_init.sql                full v1 schema
services/core/cmd/core/main.go          API server entrypoint
services/core/cmd/rowanctl/main.go      seed: workspace, api key, grant credits
services/core/internal/db/              Open, Migrate, InTx
services/core/internal/testdb/          New(t) fresh migrated DB per test
services/core/internal/quality/         status/origin enums, Citation, BestCitation
services/core/internal/ledger/          tiers, Grant, Reserve, Commit, Refund, ExpireDue, ReleaseStale, CheckInvariants
services/core/internal/resolver/        normalize, Score, Classify, Resolve, ResolveReview, Split
services/core/internal/evaluate/        BuyBox spec, Evaluate (pure), EvaluateCompany (persist)
services/core/internal/proxy/           Analyze post-sale signals
services/core/internal/enrich/          Provider, Quote, Run, fetch cache, per-source limiter
services/core/internal/export/          policy gating, CSV writer, audit
services/core/internal/api/             server, auth, rate limit, idempotency middleware, handlers
services/core/internal/api/e2e_test.go  PRD §7 validation loop
```

---

### Task 1: Repo scaffold, DB runner, test DB helper

**Files:**
- Create: `go.mod`, `docker-compose.yml`, `Makefile`, `.gitignore`, `migrations/embed.go`, `migrations/0000_bootstrap.sql` (`SELECT 1;` placeholder, replaced in Task 2), `services/core/internal/db/db.go`, `services/core/internal/testdb/testdb.go`
- Test: `services/core/internal/db/db_test.go`

**Interfaces:**
- Produces:
  - `migrations.FS embed.FS`
  - `db.Open(ctx context.Context, url string) (*pgxpool.Pool, error)`
  - `db.Migrate(ctx context.Context, pool *pgxpool.Pool) error` — applies `migrations/*.sql` in lexical order, records each in `schema_migrations(version text primary key, applied_at timestamptz)`, each file in its own tx, skips applied.
  - `db.InTx(ctx context.Context, pool *pgxpool.Pool, fn func(pgx.Tx) error) error` — commit on nil, rollback otherwise.
  - `testdb.New(t *testing.T) *pgxpool.Pool` — reads `ROWAN_TEST_DATABASE_URL`; if unset, `t.Skip` when `testing.Short()`, else `t.Fatal("ROWAN_TEST_DATABASE_URL unset; run make db-up")`. Creates `rowan_test_<random hex>`, migrates, drops on `t.Cleanup`.

- [ ] **Step 1:** `git init` in `rowan/`; `go mod init rowan.ai/rowan`; add pgx/v5. `docker-compose.yml` service `db`: `postgres:17-alpine`, port `54329:5432`, user/pass/db `rowan`. Makefile: `db-up` (`docker compose up -d --wait db`), `test` (`ROWAN_TEST_DATABASE_URL=postgres://rowan:rowan@localhost:54329/rowan?sslmode=disable go test ./... -race -count=1`), `test-short` (`go test ./... -short`).
- [ ] **Step 2: Write failing tests** in `db_test.go`:
  - `TestMigrateIdempotent`: `testdb.New`, call `db.Migrate` again → nil error; `SELECT count(*) FROM schema_migrations` equals number of `.sql` files in `migrations.FS`.
  - `TestInTxRollsBackOnError`: create temp table, `InTx` inserting a row then returning `errors.New("boom")` → error wraps `boom`, row count 0.
- [ ] **Step 3:** `make db-up && make test` → FAIL (undefined `db.Migrate`).
- [ ] **Step 4:** Implement `db.go`, `testdb.go`, `migrations/embed.go`.
- [ ] **Step 5:** `make test` → PASS. `make test-short` → PASS (skips).
- [ ] **Step 6: Commit** `chore: scaffold go module, postgres compose, migration runner`

---

### Task 2: v1 schema

**Files:**
- Delete: `migrations/0000_bootstrap.sql`
- Create: `migrations/0001_init.sql`
- Test: `services/core/internal/db/schema_test.go`

**Interfaces:**
- Produces: tables below. Every later task uses these exact names.

Enums as `CHECK` constraints (not PG enum types — cheaper to alter). DDL decisions:

```sql
workspaces(id uuid pk default gen_random_uuid(), name text not null,
  tier text not null check (tier in ('solo','deal_team','fund')), created_at timestamptz not null default now())
api_keys(id uuid pk, workspace_id uuid not null fk, key_hash bytea not null unique,  -- sha256 of token
  label text not null, monthly_spend_cap bigint not null check (monthly_spend_cap >= 0),
  rate_per_minute int not null default 120, revoked_at timestamptz, created_at timestamptz not null)
buy_boxes(id uuid pk, workspace_id fk, name text not null, criteria jsonb not null, thesis_notes text not null default '', created_at, updated_at)
source_records(id uuid pk, workspace_id fk, source_system text not null, external_ref text not null,
  payload jsonb not null, snapshot_ref text, content_hash bytea not null, fetched_at timestamptz not null,
  unique(workspace_id, source_system, external_ref))
canonical_companies(id uuid pk, workspace_id fk, display_name text not null,
  pending_review bool not null default false, proxy_summary jsonb, created_at, updated_at)
entity_merge_links(id uuid pk, canonical_company_id fk, source_record_id fk unique,
  merge_confidence_score numeric(4,3) not null check (between 0 and 1), match_reasons text[] not null,
  merge_mode text not null check (merge_mode in ('auto','manual','seed')),
  status text not null check (status in ('active','split')), created_at)
possible_match_candidates(id uuid pk, workspace_id fk, canonical_company_id fk, source_record_id fk,
  score numeric(4,3) not null check (score >= 0.55 and score < 0.85), match_reasons text[] not null,
  status text not null check (status in ('pending','merged','rejected')), resolved_by text, resolved_at timestamptz,
  unique(canonical_company_id, source_record_id))
do_not_merge_pairs(record_a_id uuid fk, record_b_id uuid fk, reason text not null, created_by text not null,
  created_at, primary key(record_a_id, record_b_id), check (record_a_id < record_b_id))
field_citations(id uuid pk, canonical_company_id fk, source_record_id fk, field_name text not null,
  field_value jsonb not null, origin_type text check (...), data_quality_status text check (...),
  export_policy text not null check (export_policy in ('allow','deal_team_plus','fund_only','display_only')),
  source_system text not null, source_ref text not null, table_name text, row_key text, page_or_span text,
  retrieved_at timestamptz not null)
buy_box_evaluations(buy_box_id fk, canonical_company_id fk, overall_score numeric(4,3) not null,
  fit text not null check (fit in ('match','partial_unverified','fail')), criteria_breakdown jsonb not null,
  evaluated_at timestamptz not null, primary key(buy_box_id, canonical_company_id))
credit_buckets(id bigserial pk, workspace_id fk, bucket_type text check (in ('rollover','included','topup')),
  granted bigint not null check (granted > 0), balance_remaining bigint not null check (balance_remaining >= 0),
  granted_at timestamptz not null, expires_at timestamptz not null, period_start date)
credit_reservations(id uuid pk, workspace_id fk, api_key_id uuid fk null, idempotency_key text not null,
  amount bigint not null check (amount > 0), status text check (in ('reserved','committed','refunded')),
  committed_amount bigint not null default 0, created_at, settled_at, unique(workspace_id, idempotency_key))
credit_ledger_entries(id bigserial pk, workspace_id fk, bucket_id bigint fk, reservation_id uuid fk null,
  entry_type text check (in ('grant','reserve','commit','refund','expire')),
  amount bigint not null check (amount > 0), created_at timestamptz not null default now())
idempotency_records(workspace_id fk, key text, request_hash bytea not null,
  state text check (in ('in_progress','done')), status_code int, response_body bytea,
  created_at, primary key(workspace_id, key))
enrichment_runs(id uuid pk, workspace_id fk, buy_box_id fk null, reservation_id fk, candidate_count int,
  hit_count int, zero_hit_count int, failed_count int, status text check (in ('running','completed','failed')), created_at, finished_at)
source_fetch_cache(provider text, cache_key text, content_hash bytea, payload jsonb null, zero_hit bool,
  fetched_at timestamptz, primary key(provider, cache_key))
export_audits(id uuid pk, workspace_id fk, user_or_key_id text not null, buy_box_id fk null, row_count int not null,
  requested_columns text[] not null, redacted_fields text[] not null, credits_charged bigint not null default 0,
  artifact_ref text not null, created_at)
```

Indexes: `credit_buckets(workspace_id, expires_at) where balance_remaining > 0`; `field_citations(canonical_company_id, field_name)`; `possible_match_candidates(workspace_id) where status='pending'`.

- [ ] **Step 1: Write failing tests** in `schema_test.go`:
  - `TestDoNotMergePairOrdered`: insert with `record_a_id > record_b_id` → error containing `check`.
  - `TestQualityStatusChecked`: insert citation with `data_quality_status='guess'` → check violation.
  - `TestReviewScoreBand`: insert candidate with score `0.85` → violation; `0.55` → ok; `0.849` → ok.
  - `TestBucketBalanceNonNegative`: update balance to `-1` → violation.
- [ ] **Step 2:** `make test` → FAIL (relation does not exist).
- [ ] **Step 3:** Write `0001_init.sql`.
- [ ] **Step 4:** `make test` → PASS.
- [ ] **Step 5: Commit** `feat(db): v1 schema with domain check constraints`

---

### Task 3: Quality & citation types

**Files:**
- Create: `services/core/internal/quality/quality.go`
- Test: `services/core/internal/quality/quality_test.go`

**Interfaces:**
- Produces:
  - `type Status string` consts `VerifiedFiling="verified_filing"`, `CommercialEstimate`, `ScrapedWeb`, `Missing`; `type Origin string` consts `RawSource`, `DerivedEnrichment`; `type ExportPolicy string` consts `Allow`, `DealTeamPlus`, `FundOnly`, `DisplayOnly`.
  - `type Citation struct { FieldName string; Value json.RawMessage; Origin Origin; Status Status; ExportPolicy ExportPolicy; SourceSystem, SourceRef string; TableName, RowKey, PageOrSpan string; RetrievedAt time.Time; SourceRecordID uuid-string }` with JSON tags matching column names.
  - `(Citation) Validate() error` — rejects unknown enum values, empty `SourceSystem`/`SourceRef`, zero `RetrievedAt` unless `Status==Missing`.
  - `BestCitation(cs []Citation) (Citation, bool)` — rank `verified_filing` > `commercial_estimate` > `scraped_web` > `missing`; tie → latest `RetrievedAt`.
  - `(Status) Deterministic() bool` — true only for `VerifiedFiling`.

- [ ] **Step 1: Write failing tests**: `TestValidateRejectsUnknownStatus`, `TestValidateRequiresSource`, `TestBestCitationPrefersVerified` (scraped newer vs verified older → verified), `TestBestCitationTieBreaksNewest`, `TestDeterministicOnlyVerified` (table over 4 statuses → only `verified_filing` true).
- [ ] **Step 2:** `go test ./services/core/internal/quality/` → FAIL.
- [ ] **Step 3:** Implement.
- [ ] **Step 4:** → PASS.
- [ ] **Step 5: Commit** `feat(quality): citation and data_quality_status types`

---

### Task 4: Ledger — tiers, grants, FIFO reserve, spend caps

**Files:**
- Create: `services/core/internal/ledger/tiers.go`, `services/core/internal/ledger/ledger.go`
- Test: `services/core/internal/ledger/ledger_test.go`

**Interfaces:**
- Consumes: `db.InTx`, `testdb.New`.
- Produces:
  - `type Tier string` (`Solo`, `DealTeam`, `Fund`); `TierSpec(t Tier) (included int64, lotDays int, overageCents int64)` → `(500,60,60)`, `(2500,90,45)`, `(7500,90,35)`.
  - `GrantPeriod(ctx, pool, workspaceID string, periodStart time.Time) error` — in one tx: flip workspace's `included` lots with balance > 0 to `rollover`; insert `included` lot `granted = included`, `expires_at = periodStart + lotDays`; `grant` ledger entry. Idempotent per `(workspace, period_start)` (no-op if a lot for that period exists).
  - `GrantTopup(ctx, pool, workspaceID string, credits int64, at time.Time) error` — `topup` lot, expires `at + 365d`.
  - `type ReserveRequest struct { WorkspaceID, APIKeyID, IdempotencyKey string; Amount int64 }`
  - `type Reservation struct { ID string; Amount, CommittedAmount int64; Status string }`
  - `Reserve(ctx, pool, req ReserveRequest, now time.Time) (Reservation, error)`
  - `Available(ctx, pool, workspaceID string, now time.Time) (int64, error)` — sum unexpired balances.
  - Errors: `ErrInsufficientCredits`, `ErrSpendCapExceeded`, `ErrIdempotencyConflict` (same key, different amount).

Reserve algorithm (one tx): (1) existing reservation for `(workspace, key)` → same amount returns it, different amount → `ErrIdempotencyConflict`. (2) If `APIKeyID != ""`: `SELECT ... FROM api_keys WHERE id=$1 FOR UPDATE`; month-to-date `sum(amount)` of reservations by that key with status `reserved` + `sum(committed_amount)` with status `committed`; `+ req.Amount > monthly_spend_cap` → `ErrSpendCapExceeded`. (3) `SELECT id, balance_remaining FROM credit_buckets WHERE workspace_id=$1 AND expires_at > $now AND balance_remaining > 0 ORDER BY CASE bucket_type WHEN 'rollover' THEN 0 WHEN 'included' THEN 1 ELSE 2 END, expires_at, id FOR UPDATE`; insufficient total → `ErrInsufficientCredits`. (4) Deduct greedily; one `reserve` entry per touched bucket. Insert reservation via `INSERT ... ON CONFLICT (workspace_id, idempotency_key) DO NOTHING`; zero rows → rollback and re-read (concurrent same-key winner).

- [ ] **Step 1: Write failing tests**:
  - `TestTierSpec` table: exact tuples above.
  - `TestGrantPeriodRollsIncludedToRollover`: grant Jan (solo), reserve+commit 100, grant Feb → Jan lot is `rollover` balance 400, Feb lot `included` 500, Jan `expires_at` unchanged.
  - `TestGrantPeriodIdempotent`: grant same period twice → one lot.
  - `TestReserveFIFOOrder`: lots rollover(exp D+10, 50), included(exp D+60, 500), topup(exp D+365, 100); reserve 120 → rollover 0, included 430, topup 100.
  - `TestReserveSkipsExpired`: expired lot with balance ignored; `Available` excludes it.
  - `TestReserveInsufficientNoPartial`: available 100, reserve 101 → `ErrInsufficientCredits`, balances unchanged, no entries.
  - `TestReserveIdempotentReplay`: same key twice → same `ID`, balance deducted once.
  - `TestReserveIdempotencyConflict`: same key, different amount → `ErrIdempotencyConflict`.
  - `TestReserveSameKeyConcurrent`: 50 goroutines, same key, amount 10 → all return same ID; balance down by exactly 10.
  - `TestReserveConcurrentNoOverdraft`: balance 100, 30 goroutines distinct keys amount 10 → exactly 10 succeed, 20 `ErrInsufficientCredits`, balance 0.
  - `TestSpendCapExceeded`: cap 50, reserve 40 ok, reserve 20 → `ErrSpendCapExceeded`; next month (`now` + 1 month) reserve 20 ok.
- [ ] **Step 2:** `make test` → FAIL.
- [ ] **Step 3:** Implement.
- [ ] **Step 4:** `make test` → PASS (with `-race`).
- [ ] **Step 5: Commit** `feat(ledger): FIFO reserve with idempotency and key spend caps`

---

### Task 5: Ledger — commit, refund, expiry, stale release, invariants

**Files:**
- Modify: `services/core/internal/ledger/ledger.go`
- Create: `services/core/internal/ledger/invariants.go`
- Test: `services/core/internal/ledger/settle_test.go`

**Interfaces:**
- Consumes: Task 4 `Reserve`, `Reservation`.
- Produces:
  - `Settle(ctx, pool, reservationID string, used int64, now time.Time) (Reservation, error)` — `0 <= used <= amount`; status `reserved` → `committed` with `committed_amount=used`, writes `commit` entries for `used` and `refund` entries returning `amount-used` to source lots in reverse consumption order (last-touched lot first). Already `committed`/`refunded` with same `used` → return as-is; different `used` → `ErrIdempotencyConflict`. `used == 0` → status `refunded`.
  - `Refund(ctx, pool, reservationID string, now time.Time) (Reservation, error)` — `Settle(..., 0, ...)`.
  - `ExpireDue(ctx, pool, now time.Time) (expired int64, err error)` — lots with `expires_at <= now AND balance_remaining > 0` → balance 0 + `expire` entry.
  - `ReleaseStale(ctx, pool, olderThan time.Duration, now time.Time) (released int, err error)` — refunds `reserved` reservations with `created_at < now - olderThan`.
  - `CheckInvariants(ctx, pool, workspaceID string) error` — per lot: `granted − Σreserve + Σrefund − Σexpire = balance_remaining`; per reservation: `Σreserve entries = amount`; committed: `Σcommit = committed_amount` and `Σrefund = amount − committed_amount`. Returns descriptive error on first violation.

- [ ] **Step 1: Write failing tests** (every test ends with `CheckInvariants` == nil):
  - `TestSettlePartialRefundsRemainder`: reserve 10 across two lots (rollover 4, included 6), settle used 7 → refunded 3 returned to included lot first; balances rollover 0, included 3.
  - `TestSettleZeroIsRefund`: used 0 → status `refunded`, balances restored.
  - `TestSettleIdempotent`: settle 7 twice → same result, one set of entries; settle 5 after → `ErrIdempotencyConflict`.
  - `TestSettleRejectsOverUse`: used 11 of 10 → error, nothing written.
  - `TestExpireDue`: lot expires at T; `ExpireDue(T)` → expired = balance, entry written; rerun → 0.
  - `TestRefundIntoExpiredLotThenSweep`: reserve from lot expiring T+1m, at T+2m refund → balance restored on expired lot; `Available` excludes it; `ExpireDue` zeroes it.
  - `TestReleaseStaleRefunds`: reservation created 20m ago → released 1, status `refunded`; fresh reservation untouched.
  - `TestConcurrentSettleSameReservation`: 20 goroutines settle used 5 → one set of commit/refund entries.
- [ ] **Step 2:** `make test` → FAIL.
- [ ] **Step 3:** Implement. `Settle` locks reservation row `FOR UPDATE` first, then touched buckets ordered by `id` (consistent lock order with Reserve to avoid deadlock: Reserve also locks buckets — ensure its `FOR UPDATE` query sorts lock acquisition the same way by wrapping in `ORDER BY id` subselect, then applies FIFO order in Go).
- [ ] **Step 4:** `make test` → PASS.
- [ ] **Step 5: Commit** `feat(ledger): settle, refund, expiry sweep, stale release, invariant check`

---

### Task 6: Resolver — normalization, scoring, classification (pure)

**Files:**
- Create: `services/core/internal/resolver/score.go`
- Test: `services/core/internal/resolver/score_test.go`

**Interfaces:**
- Produces:
  - `type RecordFacts struct { LegalName, DBA, Address1, Zip, State, Domain, Phone, Principal, RegisteredAgent string; POBox bool; IDs map[string]string }` — `IDs` keys: `uei`, `cage`, `license:<state>`, `filing:<state>`.
  - `NormalizeName(s string) string` — lowercase, strip punctuation, drop trailing suffix tokens `{inc, incorporated, llc, l l c, corp, corporation, co, company, ltd, limited, lp, llp, pllc}`, collapse whitespace. Also `normalizeDomain` (strip scheme, `www.`, path), `normalizePhone` (last 10 digits), `normalizeAddress` (lowercase, strip `suite|ste|unit|#` and what follows).
  - `Score(a, b RecordFacts) (score float64, reasons []string)` — max over rules; reasons list every matching rule.
  - `type Decision string` (`AutoMerge`, `Review`, `Distinct`); `Classify(score float64) Decision`.

Rule table (decided here; `n()` = NormalizeName, all comparisons on normalized non-empty values):

| Rule | Score | Reason |
| --- | --- | --- |
| Any shared `IDs` key with equal value | 0.97 | `hard_id:<key>` |
| `n(legal)` equal ∧ zip equal ∧ (domain ∨ phone equal) | 0.92 | `name_zip_contact` |
| `n(legal)` equal ∧ principal equal ∧ state equal | 0.88 | `name_principal_state` |
| `n(legal)` equal ∧ state equal | 0.70 | `name_state` |
| `n(dba)` of one = `n(legal)` of other ∧ state equal | 0.65 | `dba_legal_state` |
| address equal (suite stripped) ∧ zip equal | 0.60 | `address_suite_mismatch` |
| (registered agent equal ∨ both POBox with same address) ∧ state equal | 0.58 | `shared_agent_or_pobox` |
| Conflict: both have same `IDs` key with different values | cap at 0.30 | `hard_id_conflict` |

Classify uses exact comparisons: `>= 0.85` auto, `>= 0.55` review, else distinct. Round score to 3 decimals before classifying (matches `numeric(4,3)`).

- [ ] **Step 1: Write failing tests**:
  - `TestNormalizeName` table: `"Acme Plumbing, Inc."`→`"acme plumbing"`; `"ACME PLUMBING LLC"`→`"acme plumbing"`; `"Co-Op Supply Co"`→`"coop supply"`.
  - `TestClassifyBoundaries` table: `0.85`→AutoMerge, `0.849`→Review, `0.8499`→AutoMerge (rounds to 0.850; documents rounding), `0.55`→Review, `0.549`→Distinct, `1.0`→AutoMerge, `0`→Distinct.
  - `TestScoreHardID`: same UEI, different names → 0.97, reasons contains `hard_id:uei`.
  - `TestScoreHardIDConflictCaps`: same name/zip/domain but different UEI → ≤ 0.30, `hard_id_conflict`.
  - `TestScoreNameZipDomain` → 0.92; `TestScoreNamePrincipalState` → 0.88.
  - `TestScoreDBA` → 0.65 → Review; `TestScoreSuiteMismatch` → 0.60 → Review; `TestScoreSharedAgent` → 0.58 → Review.
  - `TestScoreNameOnlyDifferentState` → 0 → Distinct.
- [ ] **Step 2:** `go test ./services/core/internal/resolver/` → FAIL.
- [ ] **Step 3:** Implement.
- [ ] **Step 4:** → PASS.
- [ ] **Step 5: Commit** `feat(resolver): normalization, rule scoring, threshold classification`

---

### Task 7: Buy-Box evaluator

**Files:**
- Create: `services/core/internal/evaluate/buybox.go`, `services/core/internal/evaluate/evaluate.go`, `services/core/internal/evaluate/store.go`
- Test: `services/core/internal/evaluate/evaluate_test.go`, `services/core/internal/evaluate/store_test.go`

**Interfaces:**
- Consumes: `quality.Citation`, `quality.BestCitation`, `quality.Status.Deterministic`.
- Produces:
  - `type Criteria struct { NAICSIn, ExcludeNAICS, StatesIn, ExcludeStates []string; RevenueMinCents, RevenueMaxCents, HeadcountMin, HeadcountMax, ContractValueMinCents *int64 }` (JSON snake_case); `ParseCriteria(raw json.RawMessage) (Criteria, error)` — rejects unknown fields (`DisallowUnknownFields`), min > max, empty criteria.
  - Field mapping (citation `field_name`): `naics`, `state`, `revenue_cents`, `headcount`, `contract_value_cents`.
  - `type CriterionResult struct { Criterion string; Status string; Observed json.RawMessage; DataQualityStatus quality.Status; Citation *quality.Citation }`
  - `type Evaluation struct { OverallScore float64; Fit string; Criteria []CriterionResult }`
  - `Evaluate(c Criteria, citations []quality.Citation) Evaluation` (pure).
  - `EvaluateCompany(ctx, tx pgx.Tx, buyBoxID, canonicalID string, now time.Time) (Evaluation, error)` — loads criteria + citations, upserts `buy_box_evaluations`.
  - `ReevaluateCompany(ctx, tx pgx.Tx, canonicalID string, now time.Time) error` — all buy-boxes in the company's workspace.

Rules: per criterion pick `BestCitation` for its field. No citation or `missing` → `UNVERIFIED_LOW_CONFIDENCE` with `DataQualityStatus=missing`. Non-deterministic status → `UNVERIFIED_LOW_CONFIDENCE` (still report `Observed` + citation). Deterministic → `MET`/`NOT_MET`. `OverallScore = MET / total` rounded to 3 decimals. `Fit`: any `NOT_MET` → `fail`; all `MET` → `match`; else `partial_unverified`.

- [ ] **Step 1: Write failing tests** (`evaluate_test.go`):
  - `TestRevenueVerifiedMet`: min 200000000, verified 250000000 → `MET`.
  - `TestRevenueVerifiedNotMet` → `NOT_MET`, Fit `fail`.
  - `TestRevenueCommercialEstimateUnverified`: commercial_estimate 900000000 (clearly above) → `UNVERIFIED_LOW_CONFIDENCE`, Observed and citation present, `DataQualityStatus=commercial_estimate`.
  - `TestScrapedExclusionUnverified`: ExcludeStates [TX], scraped_web state TX → `UNVERIFIED_LOW_CONFIDENCE` (not `NOT_MET`).
  - `TestMissingFieldUnverified`: headcount criterion, no citation → `UNVERIFIED_LOW_CONFIDENCE`, `missing`.
  - `TestVerifiedBeatsEstimate`: verified 150000000 + estimate 300000000, min 200000000 → `NOT_MET`.
  - `TestOverallScoreAndFit`: 2 MET + 1 UNVERIFIED → score 0.667, `partial_unverified`.
  - `TestParseCriteriaRejectsUnknownAndInverted`.
  - `store_test.go` `TestEvaluateCompanyPersists`: seed workspace, buy box, canonical, citations → row in `buy_box_evaluations` whose `criteria_breakdown` JSON round-trips with `data_quality_status` and citation on every criterion.
- [ ] **Step 2:** FAIL.
- [ ] **Step 3:** Implement.
- [ ] **Step 4:** `make test` → PASS.
- [ ] **Step 5: Commit** `feat(evaluate): buy-box evaluation with UNVERIFIED_LOW_CONFIDENCE gating`

---

### Task 8: Resolver persistence — ingest, review queue, do-not-merge

**Files:**
- Create: `services/core/internal/resolver/store.go`
- Test: `services/core/internal/resolver/store_test.go`

**Interfaces:**
- Consumes: `Score`, `Classify`, `RecordFacts`; `quality.Citation`; `evaluate.ReevaluateCompany`.
- Produces:
  - `type SourceRecordInput struct { WorkspaceID, SourceSystem, ExternalRef string; Facts RecordFacts; Payload json.RawMessage; Citations []quality.Citation; FetchedAt time.Time }`
  - `type ResolveResult struct { SourceRecordID, CanonicalID string; Decision Decision; Score float64; Reasons []string; ReviewCandidateID string }`
  - `Resolve(ctx, tx pgx.Tx, in SourceRecordInput, now time.Time) (ResolveResult, error)` — upsert `source_records` (same `(workspace, source_system, external_ref)` + same `content_hash` → no-op returning existing link). Blocking candidates: canonicals in workspace sharing any ID value, or normalized legal name + state (facts stored in `payload->'facts'`). For each candidate canonical: skip if any `do_not_merge_pairs` row pairs the new record with any active record in it; score = max over its active records. Best ≥ 0.85 → link `auto`, attach citations, `ReevaluateCompany`. Best in review band → new canonical `pending_review=true`, link `seed`, citations attached, `possible_match_candidates` row targeting best canonical. Else → new canonical, link `seed`.
  - `ResolveReview(ctx, tx pgx.Tx, candidateID, action, actor string, now time.Time) error` — `action ∈ {"merge","reject"}`; `merge`: move the record's links + citations into target, delete emptied canonical, status `merged`, re-evaluate target; `reject`: insert `do_not_merge_pairs` between the record and every active record in target (ordered pair), status `rejected`, clear `pending_review` if no other pending rows. Non-`pending` candidate → `ErrAlreadyResolved` (idempotent replay of same action returns nil).
  - `OrderedPair(a, b string) (string, string)`.

- [ ] **Step 1: Write failing tests**:
  - `TestResolveAutoMergeOnHardID`: two records same UEI → same canonical, link score 0.970, `merge_mode='auto'`, reasons `{hard_id:uei}`.
  - `TestResolveReviewBand`: DBA match → two canonicals, second `pending_review`, one pending candidate score 0.650.
  - `TestResolveDistinct`: unrelated → two canonicals, no candidate row.
  - `TestResolveRespectsDoNotMerge`: insert pair, then ingest record that would score 0.97 against it → new canonical, not merged.
  - `TestResolveSameContentNoop`: same record twice → one `source_records` row, same canonical.
  - `TestResolveReviewMerge`: merge → candidate `merged`, emptied canonical deleted, citations re-pointed, evaluation row for target refreshed.
  - `TestResolveReviewReject`: reject → `do_not_merge_pairs` row exists (ordered), `pending_review` false; re-ingesting changed content of that record does not re-queue against same target.
  - `TestResolveCitationsKeepQuality`: every attached `field_citations` row has non-null `data_quality_status`, `origin_type`, `source_system`, `retrieved_at`.
- [ ] **Step 2:** FAIL. **Step 3:** Implement. **Step 4:** `make test` → PASS.
- [ ] **Step 5: Commit** `feat(resolver): persisted resolution, review queue, do-not-merge enforcement`

---

### Task 9: 1-click Unmerge / Split

**Files:**
- Create: `services/core/internal/resolver/split.go`
- Test: `services/core/internal/resolver/split_test.go`

**Interfaces:**
- Consumes: Task 8 tables, `OrderedPair`, `evaluate.ReevaluateCompany`.
- Produces:
  - `Split(ctx, tx pgx.Tx, canonicalID string, sourceRecordIDs []string, actor, reason string, now time.Time) (newCanonicalID string, err error)`
  - `ErrInvalidSplit`

Behavior (single tx, caller owns tx): lock canonical row `FOR UPDATE`; validate every id is an `active` link of this canonical, no duplicates, and at least one active record remains — else `ErrInvalidSplit`. Create new canonical (display name from best `legal_name` citation of moved records); re-point links (keep score/reasons, mode `manual`), re-point `field_citations` by `source_record_id`; insert `do_not_merge_pairs` for every (moved, remaining) pair with `ON CONFLICT DO NOTHING`; `ReevaluateCompany` both canonicals; recompute `display_name` of the original.

- [ ] **Step 1: Write failing tests**:
  - `TestSplitMovesRecordsAndCitations`: canonical with records A,B,C; split [C] → new canonical holds C's link + citations only; original keeps A,B.
  - `TestSplitWritesDoNotMergePairs`: pairs (A,C),(B,C) exist, ordered.
  - `TestSplitPreventsRemerge`: after split, re-ingest C with new content (still hard-ID match to A) → stays in new canonical.
  - `TestSplitReevaluates`: buy-box revenue criterion verified only via C's citation → original's evaluation flips to `UNVERIFIED_LOW_CONFIDENCE`, new canonical gets `MET`.
  - `TestSplitRejectsInvalidSelection` table: all records; unknown id; record of another canonical; duplicate ids; empty list → `ErrInvalidSplit`, and row counts of `canonical_companies`, `entity_merge_links`, `do_not_merge_pairs` unchanged.
- [ ] **Step 2:** FAIL. **Step 3:** Implement. **Step 4:** `make test` → PASS.
- [ ] **Step 5: Commit** `feat(resolver): transactional split with do-not-merge and re-evaluation`

---

### Task 10: Post-sale proxy analyzer

**Files:**
- Create: `services/core/internal/proxy/proxy.go`
- Test: `services/core/internal/proxy/proxy_test.go`

**Interfaces:**
- Consumes: `quality.Citation`.
- Produces:
  - `type SignalType string` consts `UCCFiling`, `UCCAmendment`, `OfficerChange`, `RegisteredAgentChange`, `EntityChange`, `NewHoldco`, `DomainShift`, `LeadershipChange`, `BrokerListingRemoved`, `DisclosedTransaction`.
  - `type Signal struct { Type SignalType; ObservedAt time.Time; Citation quality.Citation }`
  - `type Summary struct { Label string; InWindow bool; WindowStart, WindowEnd time.Time; SignalCount int; Signals []Signal }` — `Label ∈ {"post_sale_proxy","confirmed_transfer","none"}`.
  - `Analyze(signals []Signal, now time.Time) Summary`

Rules: window = `[now.AddDate(0,-9,0), now.AddDate(0,-6,0)]` inclusive. In-window signals counted. `confirmed_transfer` iff some in-window signal is `DisclosedTransaction` with `Citation.Status == verified_filing`. Else ≥1 in-window signal → `post_sale_proxy`. Else `none`. Every returned signal keeps its citation.

- [ ] **Step 1: Write failing tests**: `TestWindowBoundsInclusive` (exactly 6mo and 9mo ago → in; 5mo29d and 9mo1d → out), `TestProxyLabelDefault` (UCC + officer change → `post_sale_proxy`), `TestDisclosedTransactionUnverifiedStaysProxy` (scraped_web disclosed → `post_sale_proxy`), `TestConfirmedTransferRequiresVerified`, `TestSignalsKeepCitations`.
- [ ] **Step 2:** FAIL. **Step 3:** Implement. **Step 4:** PASS.
- [ ] **Step 5: Commit** `feat(proxy): 6-9 month post-sale proxy with explicit labeling`

Wiring proxy into `canonical_companies.proxy_summary` happens in Plan 4 when signal connectors exist; `GET /v1/companies/{id}` returns the stored summary (null until then).

---

### Task 11: Enrichment — quote and idempotent run

**Files:**
- Create: `services/core/internal/enrich/enrich.go`, `services/core/internal/enrich/cache.go`, `services/core/internal/enrich/fake.go`
- Test: `services/core/internal/enrich/enrich_test.go`

**Interfaces:**
- Consumes: `ledger.Reserve`, `ledger.Settle`, `ledger.Available`, `ledger.TierSpec`; `resolver.Resolve`, `resolver.SourceRecordInput`.
- Produces:
  - `type Candidate struct { Ref, LegalName, State string; IDs map[string]string }`
  - `type Provider interface { Name() string; Lookup(ctx context.Context, c Candidate) ([]resolver.SourceRecordInput, error) }` — empty slice + nil = zero-hit.
  - `type Quote struct { CandidateCount, AlreadyEnriched, Billable int; CreditsRequired, CreditsAvailable, Shortfall int64; OverageEstimateCents int64 }`
  - `type Service struct { Pool *pgxpool.Pool; Provider Provider; Limiter *rate.Limiter; CacheTTL time.Duration; Now func() time.Time }`
  - `(*Service) Quote(ctx, workspaceID string, cs []Candidate) (Quote, error)` — read-only; cache key = `Ref`; fresh cache rows (`fetched_at > now - CacheTTL`) count as already enriched.
  - `type RunResult struct { RunID, ReservationID string; Hits, ZeroHits, Failed, Cached int; CreditsCommitted, CreditsRefunded int64; Companies []string }`
  - `(*Service) Run(ctx, workspaceID, apiKeyID, idemKey, buyBoxID string, cs []Candidate) (RunResult, error)` — returns ledger errors unwrapped-checkable via `errors.Is`.
  - `FakeProvider{ Results map[string][]resolver.SourceRecordInput; Errs map[string]error; Calls atomic.Int64 }`.

Run flow: dedupe candidates by `Ref`; quote; `Billable == 0` → no reservation, run recorded, cached refs resolved from cache. Else `ledger.Reserve(Amount=Billable, IdempotencyKey=idemKey)`; if reservation already settled → load and return prior `enrichment_runs` result (replay). Insert `enrichment_runs` row `running`. Per billable candidate: `Limiter.Wait(ctx)`, `Provider.Lookup`; hit → in one tx `resolver.Resolve` each record + upsert cache row (`zero_hit=false`); zero-hit → upsert cache `zero_hit=true`; error → no cache write. `used = hits` only. `ledger.Settle(used)`. Finalize run row (`completed`; or `failed` if every lookup errored — still settles, refunding all). Context canceled mid-loop → settle with hits so far, return `ctx.Err()`.

- [ ] **Step 1: Write failing tests** (all assert `ledger.CheckInvariants` nil at end):
  - `TestQuoteIsFree`: quote → `credit_buckets` and `credit_ledger_entries` row snapshots identical before/after; `Billable` correct with 1 of 3 cached.
  - `TestQuoteShortfallOverage`: solo, available 2, billable 5 → shortfall 3, overage 180 cents.
  - `TestRunCommitsHitsRefundsZeroHits`: 3 candidates: 2 hits, 1 zero-hit → committed 2, refunded 1, `Companies` len ≥ 2.
  - `TestRunRefundsFailures`: provider error on 1 → refunded 1, failed 1, run `completed`.
  - `TestRunAllFailedRefundsAll`: status `failed`, reservation `refunded`.
  - `TestRunIdempotentReplay`: same key twice → identical `RunResult`, provider `Calls` unchanged on 2nd, credits charged once.
  - `TestRunConcurrentSameKey`: 10 goroutines same key → provider called once per candidate; one charge.
  - `TestRunInsufficientCredits`: `errors.Is(err, ledger.ErrInsufficientCredits)`, provider never called.
  - `TestRunCachedCandidateFree`: second run (new key) on same refs within TTL → 0 credits reserved, provider not called.
  - `TestRunCanceledSettlesPartial`: provider cancels ctx after first hit → committed 1, rest refunded, no `reserved` reservations remain.
- [ ] **Step 2:** FAIL. **Step 3:** Implement. **Step 4:** `make test` → PASS.
- [ ] **Step 5: Commit** `feat(enrich): zero-credit quote and idempotent metered run with refunds`

---

### Task 12: Export gating, redaction, audit

**Files:**
- Create: `services/core/internal/export/export.go`
- Test: `services/core/internal/export/export_test.go`

**Interfaces:**
- Consumes: `quality.ExportPolicy`, `ledger.Tier`, `field_citations`, `buy_box_evaluations`.
- Produces:
  - `Permitted(p quality.ExportPolicy, t ledger.Tier) bool` — `allow`: all; `deal_team_plus`: deal_team, fund; `fund_only`: fund; `display_only`: none.
  - `type Request struct { WorkspaceID, ActorID, BuyBoxID string; Columns []string; Fit []string }` (`Fit` filter optional).
  - `type Result struct { AuditID, ArtifactRef string; RowCount int; RedactedFields []string; CSV []byte }`
  - `Export(ctx, pool, artifactDir string, req Request, now time.Time) (Result, error)`

CSV layout: `canonical_company_id, display_name, fit, overall_score`, then per requested column three columns `<col>`, `<col>__data_quality_status`, `<col>__source` (`source_system:source_ref@retrieved_at RFC3339`). Value from `BestCitation`. Not permitted → value `[REDACTED]`, quality + source columns still emitted, field added to `RedactedFields` (deduped, sorted). Unknown column → error before writing anything. Artifact written to `artifactDir/<sha256>.csv` (write tmp + rename; ponytail: local disk, object storage later). Audit row inserted in same tx as the read snapshot (`REPEATABLE READ`).

- [ ] **Step 1: Write failing tests**:
  - `TestPermittedMatrix`: 4 policies × 3 tiers exact table.
  - `TestExportRedactsForSolo`: solo workspace, `revenue_cents` citation `deal_team_plus` → `[REDACTED]`, `revenue_cents__data_quality_status` still `commercial_estimate`, `RedactedFields == ["revenue_cents"]`.
  - `TestExportAllowsForFund`: same data, fund → value present.
  - `TestExportDisplayOnlyAlwaysRedacted`: fund + `display_only` → redacted.
  - `TestExportAudited`: `export_audits` row with row_count, requested_columns, redacted_fields, artifact_ref matching file on disk.
  - `TestExportUnknownColumnNoAudit`: error, zero audit rows, no file.
  - `TestExportFitFilter`: only `match` rows when `Fit=["match"]`.
- [ ] **Step 2:** FAIL. **Step 3:** Implement. **Step 4:** `make test` → PASS.
- [ ] **Step 5: Commit** `feat(export): policy-gated CSV export with redaction and audit`

---

### Task 13: Agent HTTP API

**Files:**
- Create: `services/core/internal/api/server.go`, `auth.go`, `idempotency.go`, `handlers.go`, `services/core/cmd/core/main.go`
- Test: `services/core/internal/api/api_test.go`

**Interfaces:**
- Consumes: everything above.
- Produces:
  - `api.New(deps Deps) http.Handler`; `type Deps struct { Pool *pgxpool.Pool; Enrich *enrich.Service; ArtifactDir string; Now func() time.Time }`
  - `HashKey(token string) []byte` (sha256). Token format `rk_live_<32 random bytes base32>`.
  - Routes (Go 1.22 `ServeMux` patterns). `I` = requires `Idempotency-Key`:

| Method & path | Body → response | Credits |
| --- | --- | --- |
| `POST /v1/buy-boxes` I | `{name, criteria, thesis_notes}` → buy box | 0 |
| `GET /v1/buy-boxes/{id}` | → buy box | 0 |
| `POST /v1/discovery/search` | `{buy_box_id, fit?}` → canonicals + evaluations | 0 |
| `POST /v1/enrichment/quote` | `{candidates}` → `Quote` | 0 |
| `POST /v1/enrichment/run` I | `{buy_box_id?, candidates}` → `RunResult` | billable |
| `GET /v1/companies/{id}` | → profile, all citations (with `data_quality_status`, `origin_type`, `export_policy`), `proxy_summary`, links with scores/reasons | 0 |
| `GET /v1/buy-boxes/{id}/evaluations` | → evaluations with full `criteria_breakdown` | 0 |
| `GET /v1/review-queue` | → pending candidates | 0 |
| `POST /v1/review-queue/{id}/resolve` I | `{action}` | 0 |
| `POST /v1/companies/{id}/split` I | `{source_record_ids, reason}` → `{new_canonical_id}` | 0 |
| `POST /v1/exports` I | export `Request` → CSV (`text/csv`) + `X-Rowan-Redacted-Fields` | 0 |
| `GET /v1/credits` | → available by bucket type + month-to-date key spend vs cap | 0 |

Middleware order: auth (`Authorization: Bearer`, unknown/revoked → 401) → per-key rate limit (`rate.NewLimiter(rate_per_minute/60, burst=rate_per_minute)`, map guarded by mutex; ponytail: per-process limiter, move to Redis/PG if multi-instance; exceeded → 429 + `Retry-After`) → idempotency (only `I` routes). Idempotency: missing header → 400; `request_hash = sha256(method + path + canonical body bytes)`; `INSERT ... (state='in_progress') ON CONFLICT DO NOTHING`; conflict row with different hash → 409 `idempotency_key_reuse`; same hash `done` → replay stored status/body with header `Idempotent-Replay: true`; same hash `in_progress` → 409 `request_in_progress`. On handler completion store response (2xx and 4xx; 5xx deletes the row so client can retry). Bodies decoded with `DisallowUnknownFields`, max 1 MiB. Errors JSON `{error: {code, message}}`; ledger errors map: insufficient → 402 (body includes quote), spend cap → 403 `spend_cap_exceeded`, `ErrInvalidSplit` → 422, `ErrAlreadyResolved` → 409. Cross-workspace ids → 404 (all queries filter by authenticated `workspace_id`).
  - `main.go`: env `DATABASE_URL`, `ROWAN_ARTIFACT_DIR`, `PORT` (default 8080); runs `db.Migrate`; background ticker every minute calls `ledger.ExpireDue` and `ledger.ReleaseStale(15m)`; graceful shutdown on SIGTERM. Uses `FakeProvider` only when `ROWAN_PROVIDER=fake`; otherwise exits with "no provider configured" (real providers arrive in Plan 4).

- [ ] **Step 1: Write failing tests** (`httptest`, `FakeProvider`):
  - `TestAuthRequired` (no header → 401; revoked → 401).
  - `TestRateLimit429`: key with `rate_per_minute=2`, third request → 429 with `Retry-After`.
  - `TestIdempotencyKeyRequired`: run without header → 400.
  - `TestIdempotencyReplay`: same key + body → same body, `Idempotent-Replay: true`, one charge.
  - `TestIdempotencyConflict`: same key, different body → 409 `idempotency_key_reuse`.
  - `TestIdempotencyScopedPerWorkspace`: workspaces W1, W2 send same key, different bodies → both 200, each gets own result.
  - `TestRunInsufficient402`: body has `shortfall`.
  - `TestSpendCap403`.
  - `TestCrossWorkspace404`: W2 key GETs W1 company → 404.
  - `TestCompanyResponseHasProvenance`: every citation JSON object has non-empty `data_quality_status`, `origin_type`, `source_system`, `retrieved_at`.
  - `TestUnknownFieldRejected`: extra JSON field → 400.
- [ ] **Step 2:** FAIL. **Step 3:** Implement. **Step 4:** `make test` → PASS.
- [ ] **Step 5: Commit** `feat(api): agent API with key auth, rate limits, idempotency`

---

### Task 14: End-to-end validation, seed CLI, runbook

**Files:**
- Create: `services/core/internal/api/e2e_test.go`, `services/core/cmd/rowanctl/main.go`, `README.md`
- Test: `e2e_test.go`

**Interfaces:**
- Consumes: `api.New`, `ledger.GrantPeriod`, `HashKey`, `FakeProvider`.
- Produces: `rowanctl workspace create --name --tier`, `rowanctl key create --workspace --cap --rate` (prints token once), `rowanctl credits grant-period --workspace --period YYYY-MM-01`, `rowanctl credits topup --workspace --credits`. Stdlib `flag` subcommands.

- [ ] **Step 1: Write failing tests** — each maps to PRD §7:
  - `TestE2ESourcingLoop` (§7.1): solo workspace + period grant → create buy-box (revenue ≥ $2M, states [OH,PA], NAICS [238220]) → quote 4 candidates (assert 0 credit change) → run: fake returns 2 records sharing UEI for cand-1 (auto-merge), DBA variant for cand-2 (review), verified revenue for cand-1, commercial_estimate revenue for cand-3, zero-hit cand-4 → assert committed 3, refunded 1 → evaluations: cand-1 `MET` on revenue, cand-3 `UNVERIFIED_LOW_CONFIDENCE` → review queue has 1 → reject it → split one record off cand-1's canonical → export columns `[revenue_cents, state]` → revenue redacted (policy `deal_team_plus`, tier solo), quality/source columns present, audit row exists.
  - `TestE2ELedgerIntegrity` (§7.2): 25 concurrent run requests, 5 distinct keys × 5 retries each → each key charged once; then advance `Now` 61 days, `ExpireDue` → solo lot expired; `CheckInvariants` nil; zero-hit refund confirmed via `/v1/credits`.
  - `TestE2EAgentParity` (§7.3): key with cap 3 → quote → run 3 candidates ok → 4th-candidate run → 403 `spend_cap_exceeded` → GET company returns structured citations.
- [ ] **Step 2:** `make test` → FAIL (until wiring gaps found and fixed; failures here are real integration bugs — fix in owning package with a regression test there).
- [ ] **Step 3:** Implement `rowanctl` + README runbook: `make db-up`, `make test`, seeding, `curl` examples for quote/run with `Idempotency-Key`.
- [ ] **Step 4:** `make test` → all PASS with `-race`. Manual smoke: `ROWAN_PROVIDER=fake go run ./services/core/cmd/core`, seed via `rowanctl`, `curl` quote → 200 with `credits_required`.
- [ ] **Step 5: Commit** `test: end-to-end PRD success-criteria validation; seed CLI and runbook`

---

## Validation Matrix (PRD/AGENTS requirement → proving test)

| Requirement | Test(s) |
| --- | --- |
| Auto-merge `>= 0.85`, review `0.55–0.84`, distinct `< 0.55` | `TestClassifyBoundaries`, `TestResolveAutoMergeOnHardID`, `TestResolveReviewBand`, `TestReviewScoreBand` |
| `do_not_merge_pairs` + split/unmerge consistency | `TestResolveRespectsDoNotMerge`, `TestSplit*`, `TestResolveReviewReject` |
| `UNVERIFIED_LOW_CONFIDENCE` on non-verified data | `TestRevenueCommercialEstimateUnverified`, `TestScrapedExclusionUnverified`, `TestMissingFieldUnverified` |
| Quality/citation never dropped | `TestResolveCitationsKeepQuality`, `TestCompanyResponseHasProvenance`, `TestEvaluateCompanyPersists`, `TestExportRedactsForSolo` |
| Transactional, idempotent reserve/commit/refund/expire | Task 4–5 suite, `CheckInvariants` everywhere |
| Zero-hit / failure refunds | `TestRunCommitsHitsRefundsZeroHits`, `TestRunRefundsFailures`, `TestRunAllFailedRefundsAll` |
| FIFO 60/90-day | `TestReserveFIFOOrder`, `TestGrantPeriodRollsIncludedToRollover`, `TestExpireDue`, `TestE2ELedgerIntegrity` |
| 0-credit discovery/quote | `TestQuoteIsFree`, `TestE2ESourcingLoop` |
| Idempotency-Key replay / 409 | `TestIdempotency*`, `TestRunIdempotentReplay`, `TestRunConcurrentSameKey` |
| Key spend caps | `TestSpendCapExceeded`, `TestSpendCap403`, `TestE2EAgentParity` |
| Rate limiting + caching | `TestRateLimit429`, `TestRunCachedCandidateFree` |
| Export gating + metering | `TestPermittedMatrix`, `TestExport*` |
| Post-sale proxy labeling | Task 10 suite |
