package api

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"time"

	"github.com/jackc/pgx/v5"

	"rowan.ai/rowan/services/core/internal/db"
	"rowan.ai/rowan/services/core/internal/export"
	"rowan.ai/rowan/services/core/internal/quality"
	"rowan.ai/rowan/services/core/internal/resolver"
)

type linkJSON struct {
	SourceRecordID       string   `json:"source_record_id"`
	MergeConfidenceScore float64  `json:"merge_confidence_score"`
	MatchReasons         []string `json:"match_reasons"`
	MergeMode            string   `json:"merge_mode"`
	Status               string   `json:"status"`
}

type companyJSON struct {
	ID            string             `json:"id"`
	DisplayName   string             `json:"display_name"`
	PendingReview bool               `json:"pending_review"`
	CreatedAt     time.Time          `json:"created_at"`
	UpdatedAt     time.Time          `json:"updated_at"`
	ProxySummary  json.RawMessage    `json:"proxy_summary"`
	Citations     []quality.Citation `json:"citations"`
	Links         []linkJSON         `json:"links"`
}

func (s *server) getCompany(c *call) (response, error) {
	id, err := c.pathID("id")
	if err != nil {
		return response{}, err
	}
	var out companyJSON
	// One repeatable-read snapshot keeps citations and links consistent with the company row.
	tx, err := s.Pool.BeginTx(c.ctx(), pgx.TxOptions{IsoLevel: pgx.RepeatableRead, AccessMode: pgx.ReadOnly})
	if err != nil {
		return response{}, fmt.Errorf("begin company read: %w", err)
	}
	defer func() { _ = tx.Rollback(context.WithoutCancel(c.ctx())) }()
	err = tx.QueryRow(c.ctx(), `SELECT id::text,display_name,pending_review,created_at,updated_at,proxy_summary
		FROM canonical_companies WHERE id=$1 AND workspace_id=$2`, id, c.key.WorkspaceID).
		Scan(&out.ID, &out.DisplayName, &out.PendingReview, &out.CreatedAt, &out.UpdatedAt, &out.ProxySummary)
	if errors.Is(err, pgx.ErrNoRows) {
		return response{}, errNotFound
	}
	if err != nil {
		return response{}, fmt.Errorf("get company: %w", err)
	}
	rows, err := tx.Query(c.ctx(), `SELECT field_name,field_value,origin_type,data_quality_status,export_policy,source_system,source_ref,
		COALESCE(table_name,''),COALESCE(row_key,''),COALESCE(page_or_span,''),retrieved_at,source_record_id::text
		FROM field_citations WHERE canonical_company_id=$1 ORDER BY field_name, retrieved_at DESC, id`, id)
	if err != nil {
		return response{}, fmt.Errorf("list citations: %w", err)
	}
	out.Citations, err = pgx.CollectRows(rows, func(row pgx.CollectableRow) (quality.Citation, error) {
		var c quality.Citation
		err := row.Scan(&c.FieldName, &c.Value, &c.Origin, &c.Status, &c.ExportPolicy, &c.SourceSystem, &c.SourceRef,
			&c.TableName, &c.RowKey, &c.PageOrSpan, &c.RetrievedAt, &c.SourceRecordID)
		return c, err
	})
	if err != nil {
		return response{}, fmt.Errorf("list citations: %w", err)
	}
	rows, err = tx.Query(c.ctx(), `SELECT source_record_id::text,merge_confidence_score::float8,match_reasons,merge_mode,status
		FROM entity_merge_links WHERE canonical_company_id=$1 ORDER BY created_at, source_record_id`, id)
	if err != nil {
		return response{}, fmt.Errorf("list links: %w", err)
	}
	out.Links, err = pgx.CollectRows(rows, func(row pgx.CollectableRow) (linkJSON, error) {
		var l linkJSON
		err := row.Scan(&l.SourceRecordID, &l.MergeConfidenceScore, &l.MatchReasons, &l.MergeMode, &l.Status)
		return l, err
	})
	if err != nil {
		return response{}, fmt.Errorf("list links: %w", err)
	}
	tier, err := workspaceTier(c.ctx(), tx, c.key.WorkspaceID)
	if err != nil {
		return response{}, err
	}
	for i := range out.Citations {
		redactCitation(&out.Citations[i], tier)
	}
	names, err := export.NameRedactions(c.ctx(), tx, []string{id}, tier)
	if err != nil {
		return response{}, err
	}
	if names[id] {
		out.DisplayName = export.Redacted
	}
	if out.Citations == nil {
		out.Citations = []quality.Citation{}
	}
	if out.Links == nil {
		out.Links = []linkJSON{}
	}
	return jsonResponse(200, out), nil
}

func (s *server) splitCompany(c *call) (response, error) {
	id, err := c.pathID("id")
	if err != nil {
		return response{}, err
	}
	var in struct {
		SourceRecordIDs []string `json:"source_record_ids"`
		Reason          string   `json:"reason"`
	}
	if err := c.decode(&in); err != nil {
		return response{}, err
	}
	var newID string
	err = db.InTx(c.ctx(), s.Pool, func(tx pgx.Tx) error {
		// Split takes no workspace; prove ownership first, in the same tx.
		var owned bool
		if err := tx.QueryRow(c.ctx(), `SELECT EXISTS (SELECT 1 FROM canonical_companies WHERE id=$1 AND workspace_id=$2)`,
			id, c.key.WorkspaceID).Scan(&owned); err != nil {
			return fmt.Errorf("check company workspace: %w", err)
		}
		if !owned {
			return errNotFound
		}
		var err error
		newID, err = resolver.Split(c.ctx(), tx, id, in.SourceRecordIDs, c.actor(), in.Reason, c.now)
		return err
	})
	if err != nil {
		return response{}, err
	}
	return jsonResponse(200, map[string]string{"new_canonical_id": newID}), nil
}
