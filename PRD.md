# PRD: Rowan (rowan.ai) — Off-Market Deal Sourcing & Enrichment Platform for Small Alternative PE Firms (v1)

## 1. Overview & Product Vision
Build **Rowan** (`rowan.ai`), a minimalist, warm, high-trust B2B SaaS platform—with an agent-accessible API surface—for smaller alternative private equity firms, independent sponsors, and lower-middle-market searchers (typically 1–10 person teams).

**v1 Core Value Proposition:** Act as a personal, focused research partner that helps small PE buyers discover off-market or near-off-market acquisition targets matching a granular Buy-Box profile, resolve entities with strict provenance and confidence scoring, and produce cited, export-gated lead lists without enterprise seat bloat.

**Framing Guardrail:** All outputs are framed strictly as **thesis match + cited facts**—never financial, legal, tax, or investment advice.

---

## 2. v1 Scope Boundaries

### In Scope (v1)
- **Buy-Box / Thesis Profiles:** Structured criteria (sector/NAICS/SIC, geography, size/revenue/headcount/contract thresholds, inclusions/exclusions) plus freeform thesis notes.
- **Multi-Source Discovery & Enrichment:** Ingestion across commercial enrichment APIs, broker listings/aggregations, state/local licensing boards, SAM.gov, web/filing signals, and **6–9 month post-sale proxy signals**.
- **Entity Resolution:** Gated high-confidence auto-merging (`>= 0.85`), a "Possible Match / Needs Review" queue (`0.55–0.84`), and **1-click Unmerge / Split** with persisted `do_not_merge_pairs`.
- **Provenance & Quality Status:** Field-level and criterion-level citations plus `data_quality_status` (`verified_filing`, `commercial_estimate`, `scraped_web`, `missing`).
- **Hybrid Pricing & Credit Ledger:** Base monthly subscription + bundled credits + tiered overage rates + FIFO 60/90-day rollovers + reserve/commit/refund ledger + auto-refund on zero-hit lookups.
- **Agent-Accessible API:** Parity endpoints with dry-run/quote (`0` credits), mandatory `Idempotency-Key` on billable calls, API-key credit caps, and structured provenance in responses.
- **Export Gating & Metering:** Column/plan-level gating for raw commercial fields vs. derived enrichment, plus export metering/auditing.

### Out of Scope (v1 — Deferred to v2+)
- Automated outbound email/SMS/call sequencing or CRM reply tracking.
- CIM PDF parsing, tax return/QuickBooks financial ingestion, SDE/EBITDA add-back auditing, SBA 7(a)/senior debt DSCR modeling, valuation multiples, or deal underwriting memos.

---

## 3. Target Personas & Workflows

### Primary Personas
1. **Solo Searcher / Independent Sponsor:** Lean operator running targeted thesis searches across specific states/niches with tight budget discipline.
2. **Small PE / Micro-Fund / Family Office (1–10 pros):** Evaluates multiple theses or roll-up angles, collaborates on lead review, and optionally connects internal research agents.

### Core User & Agent Workflow
1. **Create / Refine Buy-Box:** Define structured filters and freeform thesis nuances.
2. **Discover & Quote (`0` credits):** Run discovery/filtering across sources and request a dry-run enrichment quote showing candidate counts, already-enriched counts, and estimated credit burn.
3. **Enrich & Resolve:** Execute bounded enrichment for approved candidates; Go workers fetch, normalize, auto-merge (`>= 0.85`), route borderline matches (`0.55–0.84`) to review, and attach citations.
4. **Inspect, Split & Export:** Filter leads by fit and `data_quality_status`, inspect exact source citations, split any bad merges in 1 click, and export permitted columns under plan/license rules.

---

## 4. Data Sources & 6–9 Month Post-Sale Proxy

### Source Hierarchy & Fetcher Policy
- **Prefer official APIs and bulk open data** (e.g., SAM.gov, bulk public registries) where available.
- **Fallback to polite, rate-limited, cached fetchers** on public registries, licensing boards, and web sources: enforce per-source rate limits/concurrency caps, respect `robots.txt`/terms, cache raw snapshots with content hashes/ETags, and preserve full attribution.
- **Integrate commercial enrichment APIs** for firmographics, contacts/principals, and supplementary attributes, respecting per-provider terms for in-app display vs. export/redistribution.

### 6–9 Month Post-Sale Proxy
Because most small/LMM transactions do not disclose a public sale date, Rowan v1 approximates the **6–9 month post-sale window** using indirect signals:
- UCC-1 filings or amendments.
- Secretary of State officer, registered agent, or entity changes; new holdco or subsidiary registrations.
- Domain/website shifts, leadership changes, or broker listing removals/status changes.
- **Explicit Labeling:** Always label this as a **post-sale / ownership-change proxy** (with underlying source citations) unless a public filing or disclosed transaction establishes a confirmed transfer/sale event.

---

## 5. Entity Resolution, Provenance & Decision Evaluation

### Merge Confidence Thresholds
- **`>= 0.85` (Auto-Merge):** Exact hard identifier match (e.g., UEI/CAGE, license number, verified filing ID) OR normalized legal name plus strong corroboration (address/ZIP + domain/phone, or name + principal/owner + state/metro). Persists `merge_confidence_score` and `match_reasons`.
- **`0.55–0.84` (Possible Match / Needs Review):** Ambiguous/borderline overlap (e.g., DBA vs. legal name, suite mismatch, shared registered agent/PO box without matching contact/domain/owner). Queued for review rather than silently polluting the primary list.
- **`< 0.55` (Distinct):** Kept as separate canonical entities.
- **1-Click Unmerge / Split:** Detaches selected source record(s) into a distinct canonical entity, records a `do_not_merge_pair` constraint to prevent automatic re-merging, and recomputes fit scores.

### Provenance & Quality Status
Every attribute and Buy-Box criterion carries:
- `citation`: source system, URL/endpoint/filing ID, table/row/page/span identifier, and retrieval timestamp.
- `data_quality_status`: `verified_filing` | `commercial_estimate` | `scraped_web` | `missing`.
- `origin_type`: `raw_source` | `derived_enrichment`.

### Buy-Box Decision Rule on Uncertain Data
- Hard rules (e.g., `revenue >= $2M`) evaluate deterministically only when supported by verified/qualifying data.
- When a criterion relies on `commercial_estimate`, `scraped_web`, or `missing` data, mark the criterion **`UNVERIFIED_LOW_CONFIDENCE`** rather than fabricating a deterministic pass or fail.

---

## 6. Hybrid Pricing, Credit Ledger & Agent API Guardrails

### Tier Structure
| Tier | Target Buyer | Monthly Price | Included Credits | Overage / Top-Up | Rollover Window |
| :--- | :--- | :--- | :--- | :--- | :--- |
| **Search / Solo** | Search funds & solo independent sponsors | $299–$499/mo | 500 credits | $0.60 / credit | 60-day FIFO |
| **Deal Team** | Small PE firms (2–5 investment pros) | $999–$1,499/mo | 2,500 credits | $0.45 / credit | 90-day FIFO |
| **Fund / Scale** | Active LMM funds & roll-up platforms | $2,500+/mo | 7,500+ credits | $0.35 / credit | 90-day FIFO |

### Credit & API Invariants
- **Free Discovery & Quotes:** Searching, filtering, and dry-run cost quotes cost `0` credits.
- **FIFO Bucket Ledger:** Consume oldest expiring rollover credits first, then current included credits, then top-up credits, enforcing 60-day or 90-day expiration per lot.
- **Atomic Reserve → Commit / Refund:** Reserve credits in PostgreSQL before billable enrichment; commit on success; release/refund on upstream failure or zero-hit lookup.
- **Agent Safety:** Require `Idempotency-Key` on billable/mutating calls, enforce per-key credit spend caps, and include citations + `data_quality_status` in API responses.
- **Export Gating:** Strip or block restricted commercial raw fields based on plan/license policy; meter all export events.

---

## 7. v1 Success Criteria & Milestones
1. **End-to-End Sourcing Loop:** Create a Buy-Box, run discovery + `0`-credit quote, enrich candidates, inspect citations & `data_quality_status`, review/split a merge, and export a gated lead list.
2. **Ledger & Idempotency Integrity:** Zero double-charges under concurrent retries, accurate 60/90-day FIFO expiration, and automatic refunds on zero-hit lookups.
3. **Agent API Parity:** External agent or script can authenticate with a capped API key, quote credit cost, trigger idempotent enrichment, and read structured citations.