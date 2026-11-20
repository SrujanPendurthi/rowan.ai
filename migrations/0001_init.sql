CREATE TABLE workspaces (
    id uuid PRIMARY KEY DEFAULT gen_random_uuid(),
    name text NOT NULL,
    tier text NOT NULL CHECK (tier IN ('solo', 'deal_team', 'fund')),
    created_at timestamptz NOT NULL DEFAULT now()
);

CREATE TABLE api_keys (
    id uuid PRIMARY KEY DEFAULT gen_random_uuid(),
    workspace_id uuid NOT NULL REFERENCES workspaces(id),
    key_hash bytea NOT NULL UNIQUE,
    label text NOT NULL,
    monthly_spend_cap bigint NOT NULL CHECK (monthly_spend_cap >= 0),
    rate_per_minute int NOT NULL DEFAULT 120,
    revoked_at timestamptz,
    created_at timestamptz NOT NULL DEFAULT now()
);

CREATE TABLE buy_boxes (
    id uuid PRIMARY KEY DEFAULT gen_random_uuid(),
    workspace_id uuid NOT NULL REFERENCES workspaces(id),
    name text NOT NULL,
    criteria jsonb NOT NULL,
    thesis_notes text NOT NULL DEFAULT '',
    created_at timestamptz NOT NULL DEFAULT now(),
    updated_at timestamptz NOT NULL DEFAULT now()
);

CREATE TABLE source_records (
    id uuid PRIMARY KEY DEFAULT gen_random_uuid(),
    workspace_id uuid NOT NULL REFERENCES workspaces(id),
    source_system text NOT NULL,
    external_ref text NOT NULL,
    payload jsonb NOT NULL,
    snapshot_ref text,
    content_hash bytea NOT NULL,
    fetched_at timestamptz NOT NULL,
    UNIQUE (workspace_id, source_system, external_ref)
);

CREATE TABLE canonical_companies (
    id uuid PRIMARY KEY DEFAULT gen_random_uuid(),
    workspace_id uuid NOT NULL REFERENCES workspaces(id),
    display_name text NOT NULL,
    pending_review bool NOT NULL DEFAULT false,
    proxy_summary jsonb,
    created_at timestamptz NOT NULL DEFAULT now(),
    updated_at timestamptz NOT NULL DEFAULT now()
);

CREATE TABLE entity_merge_links (
    id uuid PRIMARY KEY DEFAULT gen_random_uuid(),
    canonical_company_id uuid NOT NULL REFERENCES canonical_companies(id) ON DELETE CASCADE,
    source_record_id uuid NOT NULL UNIQUE REFERENCES source_records(id),
    merge_confidence_score numeric(4,3) NOT NULL CHECK (merge_confidence_score BETWEEN 0 AND 1),
    match_reasons text[] NOT NULL,
    merge_mode text NOT NULL CHECK (merge_mode IN ('auto', 'manual', 'seed')),
    status text NOT NULL CHECK (status IN ('active', 'split')),
    created_at timestamptz NOT NULL DEFAULT now()
);

CREATE TABLE possible_match_candidates (
    id uuid PRIMARY KEY DEFAULT gen_random_uuid(),
    workspace_id uuid NOT NULL REFERENCES workspaces(id),
    canonical_company_id uuid NOT NULL REFERENCES canonical_companies(id) ON DELETE CASCADE,
    source_record_id uuid NOT NULL REFERENCES source_records(id),
    score numeric(4,3) NOT NULL CHECK (score >= 0.55 AND score < 0.85),
    match_reasons text[] NOT NULL,
    status text NOT NULL CHECK (status IN ('pending', 'merged', 'rejected')),
    resolved_by text,
    resolved_at timestamptz,
    UNIQUE (canonical_company_id, source_record_id)
);

CREATE TABLE do_not_merge_pairs (
    record_a_id uuid NOT NULL REFERENCES source_records(id),
    record_b_id uuid NOT NULL REFERENCES source_records(id),
    reason text NOT NULL,
    created_by text NOT NULL,
    created_at timestamptz NOT NULL DEFAULT now(),
    PRIMARY KEY (record_a_id, record_b_id),
    CHECK (record_a_id < record_b_id)
);

CREATE TABLE field_citations (
    id uuid PRIMARY KEY DEFAULT gen_random_uuid(),
    canonical_company_id uuid NOT NULL REFERENCES canonical_companies(id) ON DELETE CASCADE,
    source_record_id uuid NOT NULL REFERENCES source_records(id),
    field_name text NOT NULL,
    field_value jsonb NOT NULL,
    origin_type text NOT NULL CHECK (origin_type IN ('raw_source', 'derived_enrichment')),
    data_quality_status text NOT NULL CHECK (data_quality_status IN ('verified_filing', 'commercial_estimate', 'scraped_web', 'missing')),
    export_policy text NOT NULL CHECK (export_policy IN ('allow', 'deal_team_plus', 'fund_only', 'display_only')),
    source_system text NOT NULL,
    source_ref text NOT NULL,
    table_name text,
    row_key text,
    page_or_span text,
    retrieved_at timestamptz NOT NULL
);

CREATE TABLE buy_box_evaluations (
    buy_box_id uuid NOT NULL REFERENCES buy_boxes(id) ON DELETE CASCADE,
    canonical_company_id uuid NOT NULL REFERENCES canonical_companies(id) ON DELETE CASCADE,
    overall_score numeric(4,3) NOT NULL,
    fit text NOT NULL CHECK (fit IN ('match', 'partial_unverified', 'fail')),
    criteria_breakdown jsonb NOT NULL,
    evaluated_at timestamptz NOT NULL,
    PRIMARY KEY (buy_box_id, canonical_company_id)
);

CREATE TABLE credit_buckets (
    id bigserial PRIMARY KEY,
    workspace_id uuid NOT NULL REFERENCES workspaces(id),
    bucket_type text NOT NULL CHECK (bucket_type IN ('rollover', 'included', 'topup')),
    granted bigint NOT NULL CHECK (granted > 0),
    balance_remaining bigint NOT NULL CHECK (balance_remaining >= 0),
    granted_at timestamptz NOT NULL,
    expires_at timestamptz NOT NULL,
    period_start date
);

CREATE TABLE credit_reservations (
    id uuid PRIMARY KEY DEFAULT gen_random_uuid(),
    workspace_id uuid NOT NULL REFERENCES workspaces(id),
    api_key_id uuid REFERENCES api_keys(id),
    idempotency_key text NOT NULL,
    amount bigint NOT NULL CHECK (amount > 0),
    status text NOT NULL CHECK (status IN ('reserved', 'committed', 'refunded')),
    committed_amount bigint NOT NULL DEFAULT 0,
    created_at timestamptz NOT NULL DEFAULT now(),
    settled_at timestamptz,
    UNIQUE (workspace_id, idempotency_key)
);

CREATE TABLE credit_ledger_entries (
    id bigserial PRIMARY KEY,
    workspace_id uuid NOT NULL REFERENCES workspaces(id),
    bucket_id bigint NOT NULL REFERENCES credit_buckets(id),
    reservation_id uuid REFERENCES credit_reservations(id),
    entry_type text NOT NULL CHECK (entry_type IN ('grant', 'reserve', 'commit', 'refund', 'expire')),
    amount bigint NOT NULL CHECK (amount > 0),
    created_at timestamptz NOT NULL DEFAULT now()
);

CREATE TABLE idempotency_records (
    workspace_id uuid NOT NULL REFERENCES workspaces(id),
    key text NOT NULL,
    request_hash bytea NOT NULL,
    state text NOT NULL CHECK (state IN ('in_progress', 'done')),
    status_code int,
    response_body bytea,
    created_at timestamptz NOT NULL DEFAULT now(),
    PRIMARY KEY (workspace_id, key)
);

CREATE TABLE enrichment_runs (
    id uuid PRIMARY KEY DEFAULT gen_random_uuid(),
    workspace_id uuid NOT NULL REFERENCES workspaces(id),
    idempotency_key text NOT NULL,
    buy_box_id uuid REFERENCES buy_boxes(id),
    reservation_id uuid REFERENCES credit_reservations(id),
    result jsonb,
    candidate_count int NOT NULL DEFAULT 0,
    hit_count int NOT NULL DEFAULT 0,
    zero_hit_count int NOT NULL DEFAULT 0,
    failed_count int NOT NULL DEFAULT 0,
    status text NOT NULL CHECK (status IN ('running', 'completed', 'failed')),
    created_at timestamptz NOT NULL DEFAULT now(),
    finished_at timestamptz,
    UNIQUE (workspace_id, idempotency_key)
);

CREATE TABLE source_fetch_cache (
    provider text NOT NULL,
    cache_key text NOT NULL,
    content_hash bytea NOT NULL,
    payload jsonb,
    zero_hit bool NOT NULL,
    fetched_at timestamptz NOT NULL,
    PRIMARY KEY (provider, cache_key)
);

CREATE TABLE export_audits (
    id uuid PRIMARY KEY DEFAULT gen_random_uuid(),
    workspace_id uuid NOT NULL REFERENCES workspaces(id),
    user_or_key_id text NOT NULL,
    buy_box_id uuid REFERENCES buy_boxes(id),
    row_count int NOT NULL,
    requested_columns text[] NOT NULL,
    redacted_fields text[] NOT NULL,
    credits_charged bigint NOT NULL DEFAULT 0,
    artifact_ref text NOT NULL,
    created_at timestamptz NOT NULL DEFAULT now()
);

CREATE INDEX credit_buckets_available_idx ON credit_buckets (workspace_id, expires_at)
    WHERE balance_remaining > 0;
CREATE INDEX field_citations_company_field_idx ON field_citations (canonical_company_id, field_name);
CREATE INDEX possible_match_candidates_pending_idx ON possible_match_candidates (workspace_id)
    WHERE status = 'pending';
