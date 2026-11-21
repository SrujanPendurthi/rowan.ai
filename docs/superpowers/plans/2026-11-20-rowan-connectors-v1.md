# Rowan Connectors v1 (Plan 4) Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Replace the fake enrichment provider with real public-record sources — SAM.gov public extract, Florida Sunbiz corporate + event files, Florida DBPR contractor licenses — loaded through a polite fetcher, and turn their change history into cited post-sale proxy summaries on canonical companies.

**Architecture:** Bulk sources are loaded by `rowanctl` into one global table, `registry_entities` (public records; no per-workspace data). A new `registry.Provider` implements `enrich.Provider` by querying that table, so per-candidate lookups are local, free of upstream rate limits, and still metered by the existing ledger. Loaders diff each incoming row against the stored row and write dated `registry_signals`; `proxy.Recompute` joins signals to canonical companies through `source_records` + active `entity_merge_links` and writes `canonical_companies.proxy_summary`. HTTP downloads go through `fetch.Fetcher` (robots.txt, per-host rate/concurrency limits, ETag/Last-Modified, content-addressed snapshots on disk).

**Tech Stack:** Go 1.26, PostgreSQL 17, `pgx/v5`, `golang.org/x/time/rate`, stdlib `archive/zip`, `encoding/csv`, `net/http`. No new dependencies.

**Spec:** `PRD.md` §2, §4, §5 (source hierarchy, fetcher policy, post-sale proxy, citations), `Architecture.md` (connectors, object storage, `source_records`), `AGENTS.md`. Plan 1 (`docs/superpowers/plans/2026-11-20-rowan-core-v1.md`) defines every package this plan extends. Executors read all of them.

## Program Context

Plan 1 (core) is merged on `main`. This plan depends only on it. Pilot scope chosen by the user: **Florida only, contractors/trades vertical, no live API keys** — every connector is built and tested against fixture files; live downloads use env-provided keys later.

## Source Facts (verified 2026-11-20 — do not re-derive)

**SAM.gov public monthly extract** (`SAM_PUBLIC_MONTHLY_V2_YYYYMMDD.ZIP` containing one `.dat`):
- Download: `GET https://api.sam.gov/data-services/v1/extracts?api_key=KEY&fileType=ENTITY&sensitivity=PUBLIC&frequency=MONTHLY&date=MM/YYYY&charset=UTF8`. Published first Sunday of each month. Personal non-federal key without role = **10 requests/day**, so per-candidate Entity API lookups are not used.
- Pipe-delimited (`|`). First line starts `BOF PUBLIC`, last line starts `EOF`. Each data record ends with the field `!end`. The pipe delimiter and BOF/EOF framing are not stated in the layout xlsx; the parser fails loudly (`ErrUnknownFormat`) if either is wrong.
- 1-based column order from "SAM Master Extract Mapping v6.0 Public File V2 Layout.xlsx": 1 UEI, 4 CAGE, 6 SAM extract code, 8 initial registration date (YYYYMMDD), 10 last update date, 12 legal business name, 13 DBA name, 16 physical address line 1, 18 city, 19 state, 20 ZIP, 22 country code, 25 entity start date, 27 entity URL, 33 primary NAICS, 35 NAICS string (`~`-separated), 47 govt business POC first name, 49 govt business POC last name, 119 no-public-display flag (`NPDY` = must not display). Minimum 119 fields per data record.

**Florida Sunbiz corporate data file** (fixed-width ASCII, record length **1440**; files obtained manually via the state's SFTP — the download page blocks automation, so loaders read local files):
- 1-based start, length: doc number 1,12 · name 13,192 · status 205,1 (`A`/`I`) · filing type 206,15 · principal addr1 221,42 · addr2 263,42 · city 305,28 · state 333,2 · zip 335,10 · country 345,2 · file date 473,8 · FEI 481,14 · more-than-six-officers flag 495,1 · last transaction date 496,8 · RA name 545,42 · RA type 587,1 · RA address 588,42 · RA city 630,28 · RA state 658,2 · RA zip 660,9.
- Officers: up to 6 groups, officer n starts at `669 + 128*(n-1)`; within group (offset, length): title 0,4 · type 4,1 · name 5,42 · address 47,42 · city 89,28 · state 117,2 · zip 119,9.

**Florida Sunbiz corporate event file** (fixed-width, record length **662**): doc number 1,12 · seq 13,5 · event code 18,20 · event description 38,40 · effective date 78,8 · filed date 86,8 · name-change flag 413,1 (`Y`). Event code values are not published.

**Sunbiz dates:** 8 digits, format unpublished. Parse as `YYYYMMDD` when the first four digits are 1900–2100, else `MMDDYYYY`; blank/zero → no date.

**Florida DBPR licensee files** (quote/comma CSV, ASCII, **no header row**, weekly refresh, no robots.txt, free download under Fla. Stat. ch. 119):
- Construction: `https://www2.myfloridalicense.com/sto/file_download/extracts//CONSTRUCTIONLICENSE_1.csv` (double slash as published). Electrical: `https://www2.myfloridalicense.com/sto/file_download/extracts/lic08el.csv`.
- 0-based columns: 0 board number · 1 occupation code · 2 licensee name · 3 DBA name · 4 class code · 5 addr1 · 6 addr2 · 7 addr3 · 8 city · 9 state · 10 zip · 11 county code · 12 license number · 13 primary status · 14 secondary status · 15 original licensure date · 16 effective date · 17 expiration date · 18 blank · 19 renewal period · 20 alternate license #. Rows carry 22 fields (trailing empty); accept ≥ 21. Dates: parse `MM/DD/YYYY`, else `YYYYMMDD`, else none.

## Global Constraints

- Plan 1 Global Constraints still apply (thresholds, `data_quality_status`, FIFO, idempotency, export policy).
- Source systems (exact strings): `sam`, `fl_sunbiz`, `fl_dbpr`.
- ID keys (exact, lowercase): `uei`, `cage` (SAM); `filing:fl`, `fein` (Sunbiz); `license:fl` (DBPR). Values: trimmed, uppercased. Same keys are accepted from API candidates.
- All registry citations: `origin_type=raw_source`, `data_quality_status=verified_filing`, `export_policy=allow`, `source_system`=source, `source_ref`=snapshot file name, `table_name` ∈ {`sam_public_v2`, `cordata`, `corevent`, `dbpr_license`}, `row_key`=external ref, `page_or_span`=`line N`, `retrieved_at`=load time.
- SAM records flagged `NPDY` are skipped, never stored.
- Fetcher User-Agent: `RowanBot/1.0 (+https://rowan.ai/bot)`; robots group token `rowanbot`. Default per-host limit 1 req/s, burst 1, 2 concurrent. Robots cache 24h. 4xx robots → allow all; 5xx/network robots → disallow.
- Secrets in URLs (query params `api_key`, `key`, `token`) are replaced with `REDACTED` before any URL is stored, logged, or returned in an error.
- Registry lookups return at most `MaxPerSource` (default 3) records per source per candidate.
- Loads commit in batches of 5000 rows; a failed load leaves earlier batches applied, and re-running the same file is idempotent (unchanged rows are no-ops, signals dedupe).
- A row seen for the first time never emits diff signals (first load = baseline).
- Every test uses fixtures under `testdata/` and `httptest`; no test touches the network.

## Review Focus

1. **Common-name flood** — candidate "ABC Plumbing" with no state matches hundreds of rows → at most `MaxPerSource` per source, deterministic order. (Task 8, `TestLookupCapsPerSource`)
2. **Re-loading the same file** → zero updated rows, zero new signals, no duplicate signal from a re-run event file. (Task 5 `TestLoadSunbizIdempotent`, Task 6 `TestEventsDedupe`)
3. **API key leaking into storage** — SAM extract URL carries `api_key` → never in `fetch_snapshots.url`, errors, or logs. (Task 3, `TestRedactsSecretsInURL`)
4. **Split or review-merge changes which records a company owns** → `proxy_summary` recomputed for every affected canonical in the same transaction. (Task 9, `TestSplitRecomputesProxy`)
5. **Truncated or wrong-format file** (short Sunbiz line, SAM file without `BOF`, DBPR row with 12 fields) → load aborts with the line number; no half-parsed row stored. (Tasks 4, 5, 7 `...RejectsMalformed`)

---

### Task 1: Plan 1 follow-ups

**Files:**
- Modify: `services/core/internal/enrich/enrich.go` (`Run`/`finish` return path)
- Modify: `services/core/internal/api/handlers_enrich.go`
- Modify: `README.md` (idempotency section)
- Test: `services/core/internal/api/api_test.go`

**Interfaces:** Produces nothing new. A partial `RunResult` (non-empty `RunID`, `Status=="partial"`) is returned with `err == nil`.

- [ ] **Step 1: Write failing test `TestEnrichRunPartialReturns200`.** FakeProvider `OnLookup` cancels the run context after the first of 3 candidates (use the existing helper pattern in `api_test.go`). Assert HTTP 200, body `status=="partial"`, `unprocessed==2`, and a replay with the same `Idempotency-Key` returns the identical body.
- [ ] **Step 2: Run** `go test ./services/core/internal/api -run TestEnrichRunPartialReturns200 -v` → FAIL (500).
- [ ] **Step 3: Fix.** When `finish` has persisted the run row, `Run` returns `(res, nil)` even if `runErr != nil`; `runErr` only sets `Status`. Do not change the money path.
- [ ] **Step 4: README.** State that responses not stored under an idempotency key are: 402, 403 `spend_cap_exceeded`, 409 `request_in_progress`, 409 `idempotency_key_reuse`; all other 4xx are stored.
- [ ] **Step 5: Run** `make test` → PASS.
- [ ] **Step 6: Commit** `fix(enrich,api): return 200 for persisted partial runs; correct stored-4xx docs`.

---

### Task 2: Registry schema and batch loader core

**Files:**
- Create: `migrations/0005_registry.sql`
- Create: `services/core/internal/registry/registry.go`, `registry/load.go`
- Test: `services/core/internal/registry/load_test.go`

**Interfaces:**
- Produces:
  ```go
  package registry
  type Entity struct {
      SourceSystem, ExternalRef string
      Facts           resolver.RecordFacts   // LegalName, DBA, Address1, Zip(5), State, Domain, Principal, RegisteredAgent, IDs
      Extra           map[string]any         // source-specific (officers, naics, status, dates)
      Raw             map[string]string      // every parsed field, trimmed
      SourceUpdatedAt *time.Time
      Line            int
  }
  type Signal struct {
      SourceSystem, ExternalRef string
      Type       proxy.SignalType
      ObservedAt time.Time
      Citation   quality.Citation
  }
  type LoadMeta struct { SourceSystem, FileName, TableName, SnapshotRef string; LoadedAt time.Time }
  type LoadStats struct { Read, Inserted, Updated, Unchanged, Skipped, Signals int; SignalRefs []string /* refs that got ≥1 new signal, sorted, deduped */ }
  // Diff returns signals for a changed row; old is the stored entity. Called only when the row existed and its content hash changed.
  type Diff func(old, new Entity, meta LoadMeta) []Signal
  func Load(ctx context.Context, pool *pgxpool.Pool, meta LoadMeta, next func() (Entity, bool, error), diff Diff) (LoadStats, error)
  func InsertSignals(ctx context.Context, tx pgx.Tx, sigs []Signal) (int, error) // ON CONFLICT (dedupe_key) DO NOTHING; returns inserted count
  func NormID(key, value string) (string, string)  // lower(trim key), upper(trim value)
  func Cite(meta LoadMeta, e Entity, field string, value any) quality.Citation // Global Constraints citation shape
  ```
  `next` returns `(entity, true, nil)` per row, `(_, false, nil)` at EOF.

- [ ] **Step 1: Migration `0005_registry.sql`.**
  ```sql
  CREATE TABLE registry_entities (
      source_system text NOT NULL CHECK (source_system IN ('sam','fl_sunbiz','fl_dbpr')),
      external_ref text NOT NULL,
      legal_name text NOT NULL,
      names text[] NOT NULL,            -- resolver.NormalizeName of legal name and DBA, deduped, non-empty
      state text NOT NULL DEFAULT '',
      zip5 text NOT NULL DEFAULT '',
      ids jsonb NOT NULL DEFAULT '{}',
      entity jsonb NOT NULL,            -- registry.Entity
      content_hash bytea NOT NULL,
      file_name text NOT NULL,
      snapshot_ref text NOT NULL DEFAULT '',
      source_updated_at date,
      loaded_at timestamptz NOT NULL,
      PRIMARY KEY (source_system, external_ref)
  );
  CREATE INDEX registry_entities_names_idx ON registry_entities USING gin (names);
  CREATE INDEX registry_entities_ids_idx ON registry_entities USING gin (ids jsonb_path_ops);
  CREATE TABLE registry_signals (
      id bigserial PRIMARY KEY,
      source_system text NOT NULL,
      external_ref text NOT NULL,
      signal_type text NOT NULL CHECK (signal_type IN ('ucc_filing','ucc_amendment','officer_change','registered_agent_change','entity_change','new_holdco','domain_shift','leadership_change','broker_listing_removed','disclosed_transaction')),
      observed_at timestamptz NOT NULL,
      citation jsonb NOT NULL,
      dedupe_key text NOT NULL UNIQUE,
      created_at timestamptz NOT NULL DEFAULT now()
  );
  CREATE INDEX registry_signals_ref_idx ON registry_signals (source_system, external_ref);
  CREATE TABLE fetch_snapshots (
      url text PRIMARY KEY,             -- secrets redacted
      etag text NOT NULL DEFAULT '',
      last_modified text NOT NULL DEFAULT '',
      content_hash bytea NOT NULL,
      snapshot_ref text NOT NULL,
      bytes bigint NOT NULL,
      fetched_at timestamptz NOT NULL,
      checked_at timestamptz NOT NULL
  );
  CREATE INDEX source_records_ref_idx ON source_records (source_system, external_ref);
  ```
- [ ] **Step 2: Write failing tests** in `load_test.go` with a slice-backed `next` and a recording `diff`:
  - `TestLoadInsertsThenNoops`: load 3 entities → `Inserted==3`; load same 3 → `Unchanged==3`, diff never called.
  - `TestLoadDiffOnChangeOnly`: second load changes one entity's `Facts.Principal` → `Updated==1`, diff called once with old/new, its signal stored, `Signals==1`.
  - `TestLoadBaselineNoSignals`: a brand-new row never calls diff.
  - `TestInsertSignalsDedupe`: same signal twice → second returns 0.
  - `TestLoadBatches`: 12001 entities load with 3 commits (assert via row count after each batch is not required; assert final count 12001 and `Read==12001`).
  - `TestNormID`: `NormID(" Filing:FL ", " p0400012 ")` → `("filing:fl","P0400012")`.
- [ ] **Step 3: Run** `go test ./services/core/internal/registry -v` → FAIL.
- [ ] **Step 4: Implement.** `content_hash` = sha256 of canonical JSON of `Entity` without `Line`. Per batch of 5000 in one tx: `SELECT ... WHERE (source_system, external_ref) IN (...)` (use `unnest($1::text[])`) to fetch stored hash + entity, then `INSERT ... ON CONFLICT (source_system, external_ref) DO UPDATE ... WHERE registry_entities.content_hash <> EXCLUDED.content_hash`, then diff + `InsertSignals` for changed rows. `dedupe_key` = hex sha256 of `source|ref|type|observed_at date|citation value`. `names` built with `resolver.NormalizeName`.
- [ ] **Step 5: Run** tests → PASS. `make test` → PASS.
- [ ] **Step 6: Commit** `feat(registry): registry tables, batch loader with change diff and signals`.

---

### Task 3: Polite fetcher

**Files:**
- Create: `services/core/internal/fetch/fetch.go`, `fetch/robots.go`, `fetch/snapshot.go`
- Test: `services/core/internal/fetch/fetch_test.go`, `fetch/robots_test.go`

**Interfaces:**
- Consumes: `fetch_snapshots` (Task 2).
- Produces:
  ```go
  package fetch
  var ErrDisallowed = errors.New("fetch: disallowed by robots.txt")
  var ErrTooLarge   = errors.New("fetch: response exceeds MaxBytes")
  type Fetcher struct {
      Pool        *pgxpool.Pool
      Client      *http.Client        // nil → http.DefaultClient
      SnapshotDir string
      UserAgent   string              // "" → "RowanBot/1.0 (+https://rowan.ai/bot)"
      HostRate    map[string]rate.Limit // per host override; default 1/s
      Concurrency int                 // per host; 0 → 2
      MaxBytes    int64               // 0 → 2 GiB
      MaxRetries  int                 // 0 → 3
      Now         func() time.Time
  }
  type Result struct {
      URL, SnapshotRef, Path string    // URL redacted; SnapshotRef "sha256:<hex>"; Path absolute file
      ContentHash []byte
      Bytes       int64
      NotModified bool
      FetchedAt   time.Time
  }
  func (f *Fetcher) Get(ctx context.Context, rawURL string) (Result, error)
  func RedactURL(rawURL string) string
  // robots.go
  func parseRobots(body []byte, agent string) rules
  func (r rules) allowed(path string) bool
  ```

- [ ] **Step 1: Write failing robots tests** (`robots_test.go`):
  - `TestRobotsAgentGroupWins`: groups `User-agent: *` / `Disallow: /` and `User-agent: rowanbot` / `Disallow: /private` → `/data` allowed, `/private/x` disallowed.
  - `TestRobotsLongestMatchAllowTie`: `Disallow: /a`, `Allow: /a/b` → `/a/b/c` allowed, `/a/c` disallowed; equal-length Allow vs Disallow → allowed.
  - `TestRobotsWildcards`: `Disallow: /*.zip$` → `/f.zip` disallowed, `/f.zip?x=1` allowed.
  - `TestRobotsEmptyDisallow`: `Disallow:` → everything allowed.
- [ ] **Step 2: Write failing fetcher tests** (`fetch_test.go`, `httptest.Server`, testdb, `t.TempDir()` snapshots):
  - `TestGetStoresSnapshot`: 200 body `hello` + `ETag: "v1"` → file at `Path` has `hello`, `SnapshotRef=="sha256:"+hex(sha256("hello"))`, row in `fetch_snapshots`.
  - `TestGetConditional304`: second Get sends `If-None-Match: "v1"`; server returns 304 → `NotModified`, same `Path`, `checked_at` advanced.
  - `TestGetRobotsDisallow`: robots disallows path → `errors.Is(err, ErrDisallowed)`, target handler never hit.
  - `TestGetRobots404AllowsAll` and `TestGetRobots500Disallows`.
  - `TestGetRetriesRetryAfter`: first response 429 `Retry-After: 1`, second 200 → success, ≥1s elapsed (inject `sleep func(time.Duration)` field set by test to record durations instead of sleeping).
  - `TestGetPerHostConcurrency`: 6 parallel Gets to a handler that tracks max in-flight → max ≤ 2.
  - `TestGetTooLarge`: `MaxBytes=4`, body 10 bytes → `ErrTooLarge`, no file left in `SnapshotDir`.
  - `TestRedactsSecretsInURL`: Get `…/x?api_key=SECRET&date=11/2026` → stored url and `Result.URL` contain `api_key=REDACTED`, no `SECRET` in DB row or in the error string of a failing Get.
  - `TestGetSendsUserAgent`.
- [ ] **Step 3: Run** `go test ./services/core/internal/fetch -v` → FAIL.
- [ ] **Step 4: Implement.** Per-host state in a mutex-guarded map: `*rate.Limiter` + buffered-channel semaphore + robots rules with fetch time. Robots fetched through the same limiter. Body streamed through `io.LimitReader(MaxBytes+1)` and `sha256` into a temp file in `SnapshotDir`, renamed to `SnapshotDir/<hex[:2]>/<hex>`; temp removed on any error. Retries only on 429/503 and network errors; `Retry-After` seconds capped at 5m, else backoff `2^attempt` s. Non-2xx/304 after retries → error with redacted URL.
- [ ] **Step 5: Run** tests → PASS. `go test -race ./services/core/internal/fetch` → PASS.
- [ ] **Step 6: Commit** `feat(fetch): polite fetcher with robots, per-host limits, conditional GET, snapshots`.

---

### Task 4: SAM.gov public extract loader

**Files:**
- Create: `services/core/internal/registry/sam.go`
- Create: `services/core/internal/registry/testdata/sam_public_v2.dat` (BOF line, 4 records, EOF line), `testdata/sam_public_v2.zip` (same `.dat` zipped; generate in test setup with `archive/zip` instead of committing if simpler)
- Test: `services/core/internal/registry/sam_test.go`

**Interfaces:**
- Consumes: `Load`, `Entity`, `NormID`, `LoadMeta` (Task 2).
- Produces: `func LoadSAM(ctx context.Context, pool *pgxpool.Pool, path, snapshotRef string, now time.Time) (LoadStats, error)` — `path` is `.zip` (first `.dat` entry) or `.dat`. `var ErrUnknownFormat = errors.New("registry: unknown file format")`.

Fixture records (fields not listed are empty; each line has 136 `|`-separated fields ending `!end`): (a) UEI `ABCDEF123456`, CAGE `1A2B3`, `ACME HVAC LLC`, `100 MAIN ST`, `TAMPA`, `FL`, `33602`, URL `https://acmehvac.example`, NAICS `238220`, POC `JOHN` `SMITH`; (b) same shape, `GULF ELECTRIC INC`, `ORLANDO FL 32801`; (c) `NPDY` record → skipped; (d) Texas entity.

- [ ] **Step 1: Write failing tests:**
  - `TestLoadSAMFixture`: `Inserted==3`, `Skipped==1`; ACME row: `ids=={"uei":"ABCDEF123456","cage":"1A2B3"}`, `Facts.Domain=="acmehvac.example"`, `Facts.Principal=="JOHN SMITH"`, `Facts.Zip=="33602"`, `Extra["primary_naics"]=="238220"`, `source_updated_at` = column 10.
  - `TestLoadSAMZip`: same counts from the zip.
  - `TestLoadSAMRejectsMalformed`: file without `BOF` → `ErrUnknownFormat`; a record with 50 fields → error containing `line 3`, and no row from that batch stored.
- [ ] **Step 2: Run** → FAIL.
- [ ] **Step 3: Implement** with `bufio.Scanner` (buffer 1 MiB per line), column constants named after the Source Facts list. On a malformed line `next` returns the error and `Load` rolls back the in-flight batch. `meta`: `SourceSystem:"sam"`, `TableName:"sam_public_v2"`, `FileName: filepath.Base(path)`.
- [ ] **Step 4: Run** → PASS.
- [ ] **Step 5: Commit** `feat(registry): SAM.gov public V2 extract loader`.

---

### Task 5: Sunbiz corporate file loader with officer/agent diff signals

**Files:**
- Create: `services/core/internal/registry/sunbiz.go`
- Create: `services/core/internal/registry/testdata/cordata_q1.txt`, `testdata/cordata_q2.txt` (built by a test helper `fixedWidth(fields map[int]string, starts…)` is acceptable instead of hand-padded files; commit whichever is clearer)
- Test: `services/core/internal/registry/sunbiz_test.go`

Fixture anchor row (shared with Tasks 4, 7, 8, 10 so cross-source merge works): `P26000012345` = `ACME HVAC LLC`, principal `100 MAIN ST`, `TAMPA`, `FL`, `33602`, FEI `59-1234567`, officers `P JOHN SMITH` and `S MARY SMITH`, RA `REGISTERED AGENTS INC`.

**Interfaces:**
- Consumes: Task 2.
- Produces: `func LoadSunbizCorp(ctx context.Context, pool *pgxpool.Pool, path, snapshotRef string, now time.Time) (LoadStats, error)`; `func parseSunbizDate(s string) *time.Time`; `func sunbizDiff(old, new Entity, meta LoadMeta) []Signal`.

Entity mapping: `ExternalRef`=doc number; `IDs`={`filing:fl`: doc, `fein`: FEI when it is 9 digits after stripping non-digits and not all zeros}; `Facts.LegalName`=name, `Address1`/`Zip`(first 5)/`State` = principal address, `Principal`=first officer whose title contains `P`, else officer 1; `RegisteredAgent`=RA name; `Extra`: `status`, `filing_type`, `file_date`, `last_transaction_date`, `officers` ([]{title,type,name,city,state}); `SourceUpdatedAt`=last transaction date.

Diff rules (only when old row exists and hash changed; `observed_at` = new last transaction date if later than old's, else `meta.LoadedAt`):
- normalized officer-name set differs, both sides non-empty → `officer_change`, citation field `officers`, value = new officer list.
- normalized RA name differs, both non-empty → `registered_agent_change`, field `registered_agent`.
- normalized legal name differs → `entity_change`, field `legal_name`.

- [ ] **Step 1: Write failing tests:**
  - `TestParseSunbizDate`: `"20260415"`→2026-04-15, `"04152026"`→2026-04-15, `"00000000"`/`"        "`→nil.
  - `TestLoadSunbizCorpFields`: q1 loads 3 rows; row `P26000012345` has `filing:fl`, `fein=="591234567"`, 2 officers, Principal = the `P`-titled officer.
  - `TestLoadSunbizOfficerChangeSignal`: q1 then q2 where row 1's officers change and last transaction date = 2026-04-15 → one `officer_change` signal, `observed_at` 2026-04-15, citation `table_name=="cordata"`, `row_key=="P26000012345"`, `verified_filing`.
  - `TestLoadSunbizAgentChangeSignal`: RA name changes → `registered_agent_change`.
  - `TestLoadSunbizIdempotent`: load q2 twice → second `Unchanged==3`, `Signals==0`.
  - `TestLoadSunbizRejectsMalformed`: a 900-char line → error containing the line number.
- [ ] **Step 2: Run** → FAIL.
- [ ] **Step 3: Implement.** Lines shorter than 1440 after trimming `\r` → error. Slice by the Source Facts offsets (convert 1-based start to 0-based), `strings.TrimSpace` every field.
- [ ] **Step 4: Run** → PASS.
- [ ] **Step 5: Commit** `feat(registry): Sunbiz corporate loader with officer/agent change signals`.

---

### Task 6: Sunbiz event file signals

**Files:**
- Create: `services/core/internal/registry/sunbiz_events.go`
- Create: `services/core/internal/registry/testdata/corevent.txt`
- Test: `services/core/internal/registry/sunbiz_events_test.go`

**Interfaces:**
- Consumes: `InsertSignals`, `Cite`, `parseSunbizDate`.
- Produces: `type EventStats struct { Read, Signals, Unmapped int; TopUnmapped []string /* up to 20 "DESC (n)" */; SignalRefs []string }`; `func LoadSunbizEvents(ctx context.Context, pool *pgxpool.Pool, path, snapshotRef string, now time.Time) (EventStats, error)`; `func classifyEvent(desc string, nameChange bool) (proxy.SignalType, bool)`.

Event file rows do not touch `registry_entities`; they only write signals (history from before the first corporate load, so the proxy works on day one). `observed_at` = filed date, else effective date; rows with neither are counted Unmapped. `dedupe_key` input includes doc number + seq.

Classification (uppercase description, first match wins):
```
nameChange flag == "Y"                          → entity_change
contains "REGISTERED AGENT" or "REG. AGENT"     → registered_agent_change
contains "MERGER" | "CONVERSION" | "NAME CHANGE" → entity_change
contains "OFFICER" | "DIRECTOR" | "MANAGER" | "MEMBER" → officer_change
otherwise                                        → unmapped
```
`// ponytail: keyword map from unpublished event codes; refine from TopUnmapped after the first real load.`

- [ ] **Step 1: Write failing tests:**
  - `TestClassifyEvent`: table over the five branches, including `"CHANGE OF REGISTERED AGENT"`, `"MERGER"`, `"AMENDED ANNUAL REPORT"` (unmapped), name flag `Y` with desc `"AMENDMENT"`.
  - `TestLoadSunbizEvents`: fixture with 4 events (RA change, officer/manager change, merger, annual report) → `Signals==3`, `Unmapped==1`, `TopUnmapped==["ANNUAL REPORT (1)"]`, citation `table_name=="corevent"`.
  - `TestEventsDedupe`: load twice → second `Signals==0`.
- [ ] **Step 2: Run** → FAIL. **Step 3: Implement** (line length < 662 → error with line number). **Step 4: Run** → PASS.
- [ ] **Step 5: Commit** `feat(registry): Sunbiz event file to post-sale signals`.

---

### Task 7: DBPR contractor license loader

**Files:**
- Create: `services/core/internal/registry/dbpr.go`
- Create: `services/core/internal/registry/testdata/dbpr_construction.csv`, `testdata/dbpr_electrical.csv`
- Test: `services/core/internal/registry/dbpr_test.go`

**Interfaces:**
- Consumes: Task 2.
- Produces: `func LoadDBPR(ctx context.Context, pool *pgxpool.Pool, path, snapshotRef string, now time.Time) (LoadStats, error)`.

Entity mapping: `ExternalRef`=license number (col 12); `IDs`={`license:fl`: license number}; `Facts.LegalName`=DBA if non-empty else licensee name; `Facts.DBA`=DBA; `Facts.Principal`=licensee name; `Address1`=addr1, `Zip`=first 5 of zip, `State`=state; `Extra`: `board`, `occupation_code`, `class_code`, `primary_status`, `secondary_status`, `original_date`, `expiration_date`, `county_code`; `SourceUpdatedAt`=effective date. Diff: none in v1 (pass `nil`; DBPR has no qualifier field).

Fixtures (no header, quoted, 22 fields): construction file with `CGC1520001` (DBA `ACME HVAC LLC`, licensee `JOHN SMITH`, `100 MAIN ST`, `TAMPA`, `FL`, `33602`) and one more; electrical with `EC13001234` (`GULF ELECTRIC INC`) and one with empty DBA.

- [ ] **Step 1: Write failing tests:**
  - `TestLoadDBPRFixtures`: both files load; `CGC1520001` has `LegalName=="ACME HVAC LLC"`, `Principal=="JOHN SMITH"`, `names` contains both normalized names; empty-DBA row uses licensee name.
  - `TestLoadDBPRDates`: `"04/15/2019"` and `"20190415"` both parse; garbage → nil.
  - `TestLoadDBPRRejectsMalformed`: row with 12 fields → error containing `line 2`.
- [ ] **Step 2: Run** → FAIL. **Step 3: Implement** with `csv.Reader{FieldsPerRecord: -1, LazyQuotes: true}`; track line via `reader.FieldPos`. **Step 4: Run** → PASS.
- [ ] **Step 5: Commit** `feat(registry): Florida DBPR contractor license loader`.

---

### Task 8: Registry provider

**Files:**
- Create: `services/core/internal/registry/provider.go`
- Modify: `services/core/cmd/core/main.go` (`ROWAN_PROVIDER=registry`)
- Test: `services/core/internal/registry/provider_test.go`

**Interfaces:**
- Consumes: `enrich.Candidate`, `enrich.Provider` (Plan 1), `registry_entities` (Tasks 2, 4, 5, 7).
- Produces: `type Provider struct { Pool *pgxpool.Pool; MaxPerSource int }` with `Name() string` = `"registry"` and `Lookup(ctx, enrich.Candidate) ([]resolver.SourceRecordInput, error)`.

Lookup algorithm:
1. For every candidate ID (normalized with `NormID`), match `ids @> jsonb_build_object(k, v)`. If any row matches, return only ID matches.
2. Else match `names @> ARRAY[resolver.NormalizeName(c.LegalName)]` and, when `c.State != ""`, `state = upper(c.State)`.
3. Per source keep `MaxPerSource` rows ordered by `external_ref` (`row_number() OVER (PARTITION BY source_system ORDER BY external_ref)`).
4. Each row → `SourceRecordInput{SourceSystem, ExternalRef, Facts, Payload: entity json, FetchedAt: loaded_at, Citations}` with one `Cite` per non-empty field among `legal_name`, `dba`, `address`, `zip`, `state`, `principal`, `registered_agent`, `domain`, plus `primary_naics` and `status` when present in `Extra`. `WorkspaceID` left empty (enrich sets it).
5. Empty candidate name and no IDs → zero-hit, not an error.

- [ ] **Step 1: Write failing tests** (load Task 4/5/7 fixtures in setup):
  - `TestLookupByID`: candidate `IDs{"UEI":"abcdef123456"}` → exactly one `sam` record; citations all `verified_filing`, `export_policy=allow`.
  - `TestLookupByNameAcrossSources`: `LegalName "Acme HVAC, LLC"`, `State "fl"` → records from `sam`, `fl_sunbiz`, `fl_dbpr`.
  - `TestLookupStateFilter`: same name, `State "TX"` → zero.
  - `TestLookupCapsPerSource`: insert 10 `fl_dbpr` rows with the same name → 3 returned, refs ascending.
  - `TestLookupZeroHit`: unknown name → empty slice, nil error.
  - `TestRegistryEnrichAutoMerge`: run `enrich.Service{Provider: &Provider{...}}` for the ACME candidate (fund workspace with credits) → one canonical company with 3 active links (hard ID or name+address corroboration ≥ 0.85), 1 credit committed.
- [ ] **Step 2: Run** → FAIL. **Step 3: Implement.** In `main.go` add `case "registry": provider = &registry.Provider{Pool: pool, MaxPerSource: 3}` and update the error text to `set ROWAN_PROVIDER=registry or fake`. **Step 4: Run** → PASS.
- [ ] **Step 5: Commit** `feat(registry): registry-backed enrichment provider`.

---

### Task 9: Post-sale proxy recompute

**Files:**
- Create: `services/core/internal/proxy/store.go`
- Modify: `services/core/internal/resolver/store.go` (`Resolve`, `ResolveReview`), `resolver/split.go` (`Split`)
- Modify: `services/core/cmd/core/main.go` (daily recompute)
- Test: `services/core/internal/proxy/store_test.go`, `services/core/internal/resolver/proxy_hook_test.go`

**Interfaces:**
- Consumes: `registry_signals` (Task 2), `proxy.Analyze`, `Signal`, `Summary` (Plan 1).
- Produces:
  ```go
  func Recompute(ctx context.Context, tx pgx.Tx, canonicalID string, now time.Time) (Summary, error)
  func RecomputeAll(ctx context.Context, pool *pgxpool.Pool, now time.Time) (int, error)
  func RecomputeForRefs(ctx context.Context, pool *pgxpool.Pool, sourceSystem string, refs []string, now time.Time) (int, error)
  ```
  `Recompute` loads signals via `registry_signals s JOIN source_records r ON (r.source_system, r.external_ref) = (s.source_system, s.external_ref) JOIN entity_merge_links l ON l.source_record_id = r.id AND l.status = 'active' WHERE l.canonical_company_id = $1`, runs `Analyze`, and writes `proxy_summary` (always, including label `none`, when the company has any active link). `RecomputeAll` covers companies with a linked signal or a non-null `proxy_summary`, one tx per company. `RecomputeForRefs` covers canonicals linked to those refs.

Hook points (same transaction as the link change): end of `Resolve` for the returned canonical; `ResolveReview` for both canonicals it touches; `Split` for the source and the new canonical. Loader commands call `RecomputeForRefs` with refs that got new signals (Task 10).

- [ ] **Step 1: Write failing tests:**
  - `TestRecomputeInWindow`: company linked to `fl_sunbiz/P26000012345`, signal `officer_change` observed `now - 7 months` → `label=="post_sale_proxy"`, `signal_count==1`, stored row matches.
  - `TestRecomputeOutOfWindow`: signal at `now - 11 months` → `label=="none"`, stored.
  - `TestRecomputeIgnoresSplitLinks`: link status `split` → signal not counted.
  - `TestRecomputeAllSlidesWindow`: signal at `now - 5 months`; `RecomputeAll(now)` → `none`; `RecomputeAll(now + 2 months)` → `post_sale_proxy`.
  - `TestResolveRecomputesProxy` (resolver package): signal exists before the source record is resolved → after `Resolve`, company's `proxy_summary.label=="post_sale_proxy"`.
  - `TestSplitRecomputesProxy`: canonical with a Sunbiz record (signal) and a DBPR record; split the Sunbiz record out → original company `none`, new company `post_sale_proxy`.
- [ ] **Step 2: Run** → FAIL. **Step 3: Implement.** In `main.go`, run `proxy.RecomputeAll` once at startup and on a 24h ticker inside the existing sweep goroutine; log errors, never exit. **Step 4: Run** `make test` → PASS.
- [ ] **Step 5: Commit** `feat(proxy): recompute post-sale proxy summaries on link changes and daily`.

---

### Task 10: rowanctl load/fetch commands, README, end-to-end

**Files:**
- Modify: `services/core/cmd/rowanctl/main.go`, `rowanctl/main_test.go`
- Modify: `README.md` (Data sources section)
- Test: `services/core/internal/api/connectors_e2e_test.go`

**Interfaces:**
- Consumes: `LoadSAM`, `LoadSunbizCorp`, `LoadSunbizEvents`, `LoadDBPR`, `proxy.RecomputeForRefs`, `fetch.Fetcher`.
- Produces CLI commands (exact):
  ```
  rowanctl load sam <file.zip|file.dat>
  rowanctl load sunbiz-corp <file>
  rowanctl load sunbiz-events <file>
  rowanctl load dbpr <file.csv>
  rowanctl fetch dbpr                         # downloads both DBPR URLs via fetch.Fetcher, then loads each
  rowanctl fetch sam --month MM/YYYY          # needs SAM_API_KEY; downloads extract via fetch.Fetcher, then loads
  ```
  Env: `DATABASE_URL` (required), `ROWAN_SNAPSHOT_DIR` (default `./data/snapshots`), `SAM_API_KEY` (fetch sam only). Every command prints its stats as one JSON line. `load` commands set `snapshotRef` to `sha256:<hex>` of the input file. Loads that produced signals call `RecomputeForRefs`. Refs come from `LoadStats.SignalRefs` / `EventStats.SignalRefs`.

- [ ] **Step 1: Write failing CLI tests:** `TestLoadDBPRCommand` (runs `run([]string{"load","dbpr",fixture})` against testdb → output JSON `inserted==2`); `TestFetchSAMRequiresKey` (no `SAM_API_KEY` → error `SAM_API_KEY is required`); `TestFetchDBPRUsesFetcher` (fetcher pointed at an `httptest` server via an unexported base-URL override → both files loaded).
- [ ] **Step 2: Write failing e2e `TestConnectorsSourcingLoop`** (api package, server `now` fixed at 2026-11-20): load SAM, Sunbiz corp, Sunbiz events (RA change filed 2026-04-10), DBPR fixtures → start server with `registry.Provider` → `POST /v1/enrich/run` with candidate `{"ref":"c1","legal_name":"Acme HVAC LLC","state":"FL"}` → 200, 1 credit committed → `GET /v1/companies/{id}` shows citations from all three source systems, every citation `verified_filing`, and `proxy_summary.label=="post_sale_proxy"` with one `registered_agent_change` signal citing `corevent`.
- [ ] **Step 3: Run** → FAIL. **Step 4: Implement**; README gets a "Data sources" section: the three sources, how to obtain each file (Sunbiz via the state's SFTP, manual), the commands above, the 10/day SAM key limit, and that registry data is shared public data while `source_records` stay per workspace. **Step 5: Run** `make test` → PASS.
- [ ] **Step 6: Commit** `feat(rowanctl): load and fetch commands for registry sources; e2e sourcing loop`.

---

## Deferred (not in this plan)

- **Commercial enrichment adapters** — no vendor chosen; add with the first vendor contract (per-field export terms → `commercial_estimate` citations).
- **UCC filings** (Florida registry data is paid), **broker-listing removal**, **domain/leadership shifts** (need web crawling on top of `fetch.Fetcher`), **new-holdco detection** (officer cross-reference across Sunbiz entities) — Plan 4b.
- **Fuzzy name matching** (`pg_trgm`) — add if exact normalized-name recall proves too low on real data.
- **Scheduled loads** — run `rowanctl fetch dbpr` weekly and `fetch sam` monthly from cron/ops; no in-process scheduler.
- **More states** — one loader per registry once Florida proves the pipeline.
