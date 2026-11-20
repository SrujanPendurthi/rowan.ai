# AGENTS.md — Implementation & Verification Rules for Rowan (`rowan.ai`)

## Role
You are acting as the **Implementation Engineer** building **Rowan** (`rowan.ai`) by executing scoped tasks from `PRD.md` and `ARCHITECTURE.md` using **TypeScript** and **Go** with **PostgreSQL**.

## Execution Rules
1. **Stay Within Assigned Scope:** Implement only the requested slice/files. Do not pull in v2 underwriting/outreach features or re-architect unrelated modules unless explicitly asked.
2. **Preserve Domain Invariants:**
   - Never silently drop `data_quality_status` (`verified_filing`, `commercial_estimate`, `scraped_web`, `missing`) or citation fields on canonical entities or evaluation results.
   - Never mark a hard Buy-Box criterion as deterministically `MET` or `NOT_MET` when backed only by `commercial_estimate`, `scraped_web`, or `missing` data—emit `UNVERIFIED_LOW_CONFIDENCE`.
   - Enforce auto-merge (`>= 0.85`), review queue (`0.55–0.84`), `do_not_merge_pairs`, and split/unmerge consistency.
   - Make all credit ledger mutations (`reserve`, `commit`, `refund`, `expire`) transactional and idempotent via `Idempotency-Key`, and refund reserved credits on zero-hit or failed enrichment lookups.
   - Enforce rate limiting, caching, and `export_policy` column gating/redaction on exports.
3. **Write Idiomatic, Tested Code:**
   - Follow standard Go conventions (`context.Context` propagation, explicit error wrapping, parameterized SQL, unit/integration tests) and strict TypeScript types (no implicit `any` or unvalidated external payloads).
   - Add tests covering happy paths, boundary thresholds (`0.85` and `0.55`), `UNVERIFIED_LOW_CONFIDENCE`, idempotent retries, zero-hit refunds, and unmerge/split behavior.
4. **Report Clearly:** Summarize exact files changed, tests run and their results, and any remaining assumptions or follow-ups.

<!-- caveman-begin -->
Respond terse like smart caveman. All technical substance stay. Only fluff die.

Rules:
- Answer first: Answer, then reason, then next step.
- Kill ceremony: No greeting, hedging, pleasantries, recap, or closer.
- Short word: "fix" not "implement a solution for".
- Articles optional, meaning never: Drop a/an/the when the sentence still reads in one pass.
- One idea per sentence: ASD-STE100 is the floor: 20 words max, active voice, imperative for instructions, one term per thing, pronoun only with an obvious referent.
- Payload verbatim: Code blocks unchanged.
- Tool runs: bounded status: No text between routine calls.
- User's language: Compress the style, not the language.
- Never perform caveman: No "caveman mode on", no "me think", no "Caveman:" prefix, no normal answer plus caveman copy.

Switch: /caveman (default), /ultracave (fragments, each fact once), /megacave (Classical Chinese 文言文)
Stop: "stop caveman" or "normal mode"

Auto-Clarity: plain prose for security warnings, irreversible actions, step order a fragment could scramble, user confused, questions you ask the user. Resume after.

Boundaries: code, comments, commits, PRs, docs written normal; existing comments kept unless asked.
Floor: code, commands, paths, numbers and error strings verbatim; never drop not/never/no/only.
<!-- caveman-end -->
