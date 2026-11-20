package api

import (
	"fmt"

	"github.com/jackc/pgx/v5"

	"rowan.ai/rowan/services/core/internal/db"
	"rowan.ai/rowan/services/core/internal/resolver"
)

type reviewJSON struct {
	ID                 string   `json:"id"`
	CanonicalCompanyID string   `json:"canonical_company_id"`
	SourceRecordID     string   `json:"source_record_id"`
	SourceSystem       string   `json:"source_system"`
	ExternalRef        string   `json:"external_ref"`
	Score              float64  `json:"score"`
	MatchReasons       []string `json:"match_reasons"`
}

func (s *server) reviewQueue(c *call) (response, error) {
	rows, err := s.Pool.Query(c.ctx(), `SELECT p.id::text,p.canonical_company_id::text,p.source_record_id::text,
		r.source_system,r.external_ref,p.score::float8,p.match_reasons
		FROM possible_match_candidates p JOIN source_records r ON r.id=p.source_record_id
		WHERE p.workspace_id=$1 AND p.status='pending' ORDER BY p.score DESC, p.id`, c.key.WorkspaceID)
	if err != nil {
		return response{}, fmt.Errorf("list review queue: %w", err)
	}
	list, err := pgx.CollectRows(rows, func(row pgx.CollectableRow) (reviewJSON, error) {
		var r reviewJSON
		err := row.Scan(&r.ID, &r.CanonicalCompanyID, &r.SourceRecordID, &r.SourceSystem, &r.ExternalRef, &r.Score, &r.MatchReasons)
		return r, err
	})
	if err != nil {
		return response{}, fmt.Errorf("list review queue: %w", err)
	}
	if list == nil {
		list = []reviewJSON{}
	}
	return jsonResponse(200, map[string]any{"candidates": list}), nil
}

func (s *server) resolveReview(c *call) (response, error) {
	id, err := c.pathID("id")
	if err != nil {
		return response{}, err
	}
	var in struct {
		Action string `json:"action"`
	}
	if err := c.decode(&in); err != nil {
		return response{}, err
	}
	err = db.InTx(c.ctx(), s.Pool, func(tx pgx.Tx) error {
		// ResolveReview takes no workspace; prove ownership first, in the same tx.
		var owned bool
		if err := tx.QueryRow(c.ctx(), `SELECT EXISTS (SELECT 1 FROM possible_match_candidates WHERE id=$1 AND workspace_id=$2)`,
			id, c.key.WorkspaceID).Scan(&owned); err != nil {
			return fmt.Errorf("check candidate workspace: %w", err)
		}
		if !owned {
			return errNotFound
		}
		return resolver.ResolveReview(c.ctx(), tx, id, in.Action, c.actor(), c.now)
	})
	if err != nil {
		return response{}, err
	}
	status := "rejected"
	if in.Action == "merge" {
		status = "merged"
	}
	return jsonResponse(200, map[string]string{"candidate_id": id, "status": status}), nil
}
