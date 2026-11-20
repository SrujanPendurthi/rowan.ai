ALTER TABLE enrichment_runs DROP CONSTRAINT enrichment_runs_status_check;
ALTER TABLE enrichment_runs ADD CONSTRAINT enrichment_runs_status_check
    CHECK (status IN ('running', 'completed', 'partial', 'failed'));

CREATE INDEX credit_ledger_entries_reservation_idx ON credit_ledger_entries (reservation_id);
CREATE INDEX entity_merge_links_canonical_idx ON entity_merge_links (canonical_company_id);
CREATE INDEX field_citations_source_record_idx ON field_citations (source_record_id);
CREATE INDEX credit_reservations_key_created_idx ON credit_reservations (api_key_id, created_at);
CREATE INDEX credit_reservations_status_created_idx ON credit_reservations (status, created_at);
