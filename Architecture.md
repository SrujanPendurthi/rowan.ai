# Rowan (`rowan.ai`) System Architecture (v1): TypeScript + Go + PostgreSQL

## 1. High-Level Topology
- **Web Frontend & BFF (`apps/web` & `apps/bff` — TypeScript):**
  - Minimalist, warm research-desk UI for **Rowan** (`rowan.ai`): Buy-Box builder, job status/quote views, lead list explorer, citation & `data_quality_status` inspector, "Possible Match / Needs Review" queue, 1-click Unmerge/Split UI, and export center.
  - Auth/session handling, API key management, export policy enforcement, and typed client calls to Go services.
- **Core Backend & Worker Engine (`services/core` — Go):**
  - Source connectors (official APIs, bulk loaders, polite rate-limited cached fetchers, commercial enrichment adapters).
  - Entity resolver (`>= 0.85` auto-merge, `0.55–0.84` review queue, `< 0.55` split, `do_not_merge_pairs`, and 1-click unmerge/split).
  - Proxy signal analyzer (6–9 month post-sale multi-signal scoring with explicit proxy labels).
  - Buy-Box decision evaluator (`MET`, `NOT_MET`, `UNVERIFIED_LOW_CONFIDENCE`).
  - Transactional credit ledger (FIFO 60/90-day buckets, reserve/commit/refund, idempotency, key spend caps).
- **PostgreSQL + Object Storage:**
  - PostgreSQL is the system of record for workspaces, Buy-Boxes, canonical companies, merge links/exclusions, field citations, Buy-Box evaluations, credit buckets/ledger entries, idempotency records, and export audit logs.
  - Object storage holds raw fetched payloads/snapshots and immutable export artifacts.

---

## 2. Core Domain Models (PostgreSQL Concepts)
- `workspaces`, `users`, `api_keys` (with scopes, rate/spend caps, and tier metadata).
- `buy_boxes` (structured filters JSONB + freeform thesis text + scoring/verification rules).
- `source_records` (raw normalized payloads per source, snapshot pointer, fetched timestamp).
- `canonical_companies` (merged company profile + post-sale proxy summary).
- `entity_merge_links` (`canonical_company_id`, `source_record_id`, `merge_confidence_score`, `match_reasons`, `merge_mode`, `status`).
- `possible_match_candidates` (pairs scoring `0.55–0.84` awaiting human/agent resolution).
- `do_not_merge_pairs` (`record_a_id`, `record_b_id`, `reason`, `created_by`).
- `field_citations` (`canonical_company_id`, `field_name`, `field_value`, `origin_type`, `data_quality_status`, `export_policy`, `source_system`, `source_ref`, `table_name`, `row_key`, `page_or_span`, `retrieved_at`).
- `buy_box_evaluations` (`buy_box_id`, `canonical_company_id`, `overall_score`, `criteria_breakdown` with `MET` / `NOT_MET` / `UNVERIFIED_LOW_CONFIDENCE`).
- `credit_buckets` & `credit_ledger_entries` (`bucket_type`: `rollover` | `included` | `topup`, `expires_at`, `balance_remaining`, `reservation_id`, `idempotency_key`, entry type `reserve` | `commit` | `refund` | `expire` | `grant`).
- `export_audits` (`workspace_id`, `user_or_key_id`, `row_count`, `redacted_fields`, `credits_charged`, `created_at`).

---

## 3. Critical Flows & Invariants
1. **Quote Before Enrichment:** `POST /v1/enrichment/quote` computes candidate counts and credit requirements without mutating `credit_buckets`.
2. **Idempotent Billable Execution:** `POST /v1/enrichment/run` requires `Idempotency-Key`. Replays with identical payload + key return the existing outcome without re-charging; conflicting payloads with the same key return `409 Conflict`.
3. **Atomic Ledger Transitions:** Reservations deduct available FIFO bucket capacity inside a database transaction (`SELECT ... FOR UPDATE`), commit on completion, and credit back on failure or zero-hit results.
4. **Safe Unmerge / Split:** Runs in a single transaction—creates/updates target `canonical_companies`, inserts `do_not_merge_pairs` entries, re-links citations, and queues Buy-Box re-evaluation.

---

## 4. Suggested Repository Layout
```text
├── PRD.md
├── ARCHITECTURE.md
├── CLAUDE.md
├── AGENTS.md
├── apps/
│   ├── web/              # TypeScript frontend (Buy-Box, Lead List, Review Queue, Split UX)
│   └── bff/              # TypeScript API/BFF (auth, key caps, export gating, proxy to Go)
├── services/
│   └── core/             # Go service & workers (connectors, resolver, scorer, ledger)
└── migrations/           # Versioned SQL migrations for PostgreSQL