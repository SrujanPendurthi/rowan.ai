CREATE TABLE registry_entities (
    source_system text NOT NULL CHECK (source_system IN ('sam','fl_sunbiz','fl_dbpr')),
    external_ref text NOT NULL,
    legal_name text NOT NULL,
    names text[] NOT NULL,
    state text NOT NULL DEFAULT '',
    zip5 text NOT NULL DEFAULT '',
    ids jsonb NOT NULL DEFAULT '{}',
    entity jsonb NOT NULL,
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
    url text PRIMARY KEY,
    etag text NOT NULL DEFAULT '',
    last_modified text NOT NULL DEFAULT '',
    content_hash bytea NOT NULL,
    snapshot_ref text NOT NULL,
    bytes bigint NOT NULL,
    fetched_at timestamptz NOT NULL,
    checked_at timestamptz NOT NULL
);
CREATE INDEX source_records_ref_idx ON source_records (source_system, external_ref);
