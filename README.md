# Rowan core

Rowan core is the agent-facing service behind `rowan.ai`. It resolves company records from public and commercial sources into canonical entities (auto-merge at score `>= 0.85`, review queue at `0.55–0.84`), evaluates them against a buy box with cited facts (`MET`, `NOT_MET`, or `UNVERIFIED_LOW_CONFIDENCE` for anything not backed by a `verified_filing`), meters enrichment through a transactional credit ledger, and exports policy-gated CSVs. Output is thesis match plus cited facts, not advice.

## Prerequisites

Go (see `go.mod`), Docker with Compose, `curl`.

## Run

```sh
make db-up        # Postgres on :54329
make test         # full suite, -race, needs the DB
make test-short   # no DB

export DATABASE_URL='postgres://rowan:rowan@localhost:54329/rowan?sslmode=disable'
ROWAN_PROVIDER=fake ROWAN_ARTIFACT_DIR=./artifacts go run ./services/core/cmd/core   # :8080, or set PORT
```

The server migrates the database on start. `ROWAN_PROVIDER=fake` is the only provider in v1; it returns zero hits for every lookup.

## Seed

`rowanctl` reads `DATABASE_URL`. Each command prints the IDs it created.

```sh
go run ./services/core/cmd/rowanctl workspace create --name acme --tier solo        # prints workspace_id
go run ./services/core/cmd/rowanctl key create --workspace $WS --cap 500 --rate 60  # prints key_id and token
go run ./services/core/cmd/rowanctl credits grant-period --workspace $WS --period 2026-11-01
go run ./services/core/cmd/rowanctl credits topup --workspace $WS --credits 100
```

The token is shown once; only its hash is stored. `--cap` is the key's monthly spend cap in credits, `--rate` is requests per minute.

## Use

```sh
export KEY=rk_live_...   # token from `key create`

# Quote: free, writes nothing.
curl -X POST localhost:8080/v1/enrichment/quote -H "Authorization: Bearer $KEY" \
  -d '{"candidates":[{"ref":"a","legal_name":"Acme LLC","state":"OH"}]}'

# Run: billable. Idempotency-Key is required; retry with the same key and body to replay, never to double-charge.
curl -X POST localhost:8080/v1/enrichment/run -H "Authorization: Bearer $KEY" -H 'Idempotency-Key: run-001' \
  -d '{"candidates":[{"ref":"a","legal_name":"Acme LLC","state":"OH"}]}'

curl localhost:8080/v1/credits -H "Authorization: Bearer $KEY"
```

One credit per billable lookup; zero-hit and failed lookups are refunded. Other routes: `/v1/buy-boxes`, `/v1/discovery/search`, `/v1/companies/{id}`, `/v1/companies/{id}/split`, `/v1/review-queue`, `/v1/exports`.

### Idempotency and limits

- The `Idempotency-Key` namespace is per workspace: keys are shared across all API keys in a workspace.
- Stored responses replay for the same key and body. A different body under the same key returns `409`.
- Responses not stored under an idempotency key are: `402`, `403 spend_cap_exceeded`, `409 request_in_progress`, and `409 idempotency_key_reuse`; all other 4xx are stored. After fixing a stored 4xx (for example `400`), send a new key with the changed request.
- `402` and `403 spend_cap_exceeded` are not stored, so the same key and body succeeds after a top-up or a raised cap, and is charged once.
- A run takes at most 500 distinct candidate refs (`400 invalid_request` above that). Candidates sharing one identity are looked up and billed once.
- A run that stops early (timeout or provider rate-limit error) returns HTTP `200` once persisted, with `"status": "partial"` and an `"unprocessed"` count; credits for unprocessed candidates are refunded. The same key and body replay the stored response.
- JSON responses apply the same tier `export_policy` gating as CSV exports: gated `field_value`, criterion `observed`, and `display_name` are `"[REDACTED]"`.
