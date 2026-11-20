package api

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/jackc/pgx/v5"

	"rowan.ai/rowan/services/core/internal/db"
	"rowan.ai/rowan/services/core/internal/evaluate"
	"rowan.ai/rowan/services/core/internal/export"
	"rowan.ai/rowan/services/core/internal/resolver"
)

type buyBoxJSON struct {
	ID          string          `json:"id"`
	Name        string          `json:"name"`
	Criteria    json.RawMessage `json:"criteria"`
	ThesisNotes string          `json:"thesis_notes"`
	CreatedAt   time.Time       `json:"created_at"`
	UpdatedAt   time.Time       `json:"updated_at"`
}

func (s *server) createBuyBox(c *call) (response, error) {
	var in struct {
		Name        string          `json:"name"`
		Criteria    json.RawMessage `json:"criteria"`
		ThesisNotes string          `json:"thesis_notes"`
	}
	if err := c.decode(&in); err != nil {
		return response{}, err
	}
	if strings.TrimSpace(in.Name) == "" {
		return response{}, badRequest("name is required")
	}
	if _, err := evaluate.ParseCriteria(in.Criteria); err != nil {
		return response{}, badRequest(err.Error())
	}
	var out buyBoxJSON
	err := db.InTx(c.ctx(), s.Pool, func(tx pgx.Tx) error {
		// Serialize with ingestion so no company is evaluated against a half-visible buy box.
		if err := resolver.LockWorkspace(c.ctx(), tx, c.key.WorkspaceID); err != nil {
			return err
		}
		err := tx.QueryRow(c.ctx(), `INSERT INTO buy_boxes(workspace_id,name,criteria,thesis_notes,created_at,updated_at)
			VALUES ($1,$2,$3,$4,$5,$5) RETURNING id::text,name,criteria,thesis_notes,created_at,updated_at`,
			c.key.WorkspaceID, in.Name, in.Criteria, in.ThesisNotes, c.now).
			Scan(&out.ID, &out.Name, &out.Criteria, &out.ThesisNotes, &out.CreatedAt, &out.UpdatedAt)
		if err != nil {
			return fmt.Errorf("insert buy box: %w", err)
		}
		// ponytail: evaluates every existing company serially in one tx; batch if workspaces grow large.
		rows, err := tx.Query(c.ctx(), `SELECT id::text FROM canonical_companies WHERE workspace_id=$1 ORDER BY id`, c.key.WorkspaceID)
		if err != nil {
			return fmt.Errorf("list companies: %w", err)
		}
		ids, err := pgx.CollectRows(rows, pgx.RowTo[string])
		if err != nil {
			return fmt.Errorf("list companies: %w", err)
		}
		for _, id := range ids {
			if _, err := evaluate.EvaluateCompany(c.ctx(), tx, out.ID, id, c.now); err != nil {
				return err
			}
		}
		return nil
	})
	if err != nil {
		return response{}, err
	}
	return jsonResponse(201, out), nil
}

func (s *server) getBuyBox(c *call) (response, error) {
	id, err := c.pathID("id")
	if err != nil {
		return response{}, err
	}
	var out buyBoxJSON
	err = s.Pool.QueryRow(c.ctx(), `SELECT id::text,name,criteria,thesis_notes,created_at,updated_at
		FROM buy_boxes WHERE id=$1 AND workspace_id=$2`, id, c.key.WorkspaceID).
		Scan(&out.ID, &out.Name, &out.Criteria, &out.ThesisNotes, &out.CreatedAt, &out.UpdatedAt)
	if errors.Is(err, pgx.ErrNoRows) {
		return response{}, errNotFound
	}
	if err != nil {
		return response{}, fmt.Errorf("get buy box: %w", err)
	}
	return jsonResponse(200, out), nil
}

type evaluationJSON struct {
	ID                string          `json:"id"`
	DisplayName       string          `json:"display_name"`
	PendingReview     bool            `json:"pending_review"`
	Fit               string          `json:"fit"`
	OverallScore      float64         `json:"overall_score"`
	CriteriaBreakdown json.RawMessage `json:"criteria_breakdown"`
	EvaluatedAt       time.Time       `json:"evaluated_at"`
}

// requireBuyBox returns errNotFound unless the buy box belongs to the workspace.
func (s *server) requireBuyBox(ctx context.Context, workspaceID, buyBoxID string) error {
	var exists bool
	if err := s.Pool.QueryRow(ctx, `SELECT EXISTS (SELECT 1 FROM buy_boxes WHERE id=$1 AND workspace_id=$2)`, buyBoxID, workspaceID).Scan(&exists); err != nil {
		return fmt.Errorf("check buy box: %w", err)
	}
	if !exists {
		return errNotFound
	}
	return nil
}

// evaluations lists a workspace buy box's evaluations best first; fits empty means all.
func (s *server) evaluations(ctx context.Context, workspaceID, buyBoxID string, fits []string) ([]evaluationJSON, error) {
	if err := s.requireBuyBox(ctx, workspaceID, buyBoxID); err != nil {
		return nil, err
	}
	rows, err := s.Pool.Query(ctx, `SELECT c.id::text,c.display_name,c.pending_review,e.fit,e.overall_score::float8,e.criteria_breakdown,e.evaluated_at
		FROM buy_box_evaluations e JOIN canonical_companies c ON c.id=e.canonical_company_id
		WHERE e.buy_box_id=$1 AND c.workspace_id=$2 AND (cardinality($3::text[])=0 OR e.fit=ANY($3::text[]))
		ORDER BY e.overall_score DESC, c.id`, buyBoxID, workspaceID, append([]string{}, fits...))
	if err != nil {
		return nil, fmt.Errorf("list evaluations: %w", err)
	}
	out, err := pgx.CollectRows(rows, func(row pgx.CollectableRow) (evaluationJSON, error) {
		var e evaluationJSON
		err := row.Scan(&e.ID, &e.DisplayName, &e.PendingReview, &e.Fit, &e.OverallScore, &e.CriteriaBreakdown, &e.EvaluatedAt)
		return e, err
	})
	if err != nil {
		return nil, fmt.Errorf("list evaluations: %w", err)
	}
	if out == nil {
		out = []evaluationJSON{}
	}
	tier, err := workspaceTier(ctx, s.Pool, workspaceID)
	if err != nil {
		return nil, err
	}
	ids := make([]string, len(out))
	for i, e := range out {
		ids[i] = e.ID
	}
	names, err := export.NameRedactions(ctx, s.Pool, ids, tier)
	if err != nil {
		return nil, err
	}
	for i := range out {
		if names[out[i].ID] {
			out[i].DisplayName = export.Redacted
		}
		if out[i].CriteriaBreakdown, err = redactBreakdown(out[i].CriteriaBreakdown, tier); err != nil {
			return nil, err
		}
	}
	return out, nil
}

func (s *server) listEvaluations(c *call) (response, error) {
	id, err := c.pathID("id")
	if err != nil {
		return response{}, err
	}
	list, err := s.evaluations(c.ctx(), c.key.WorkspaceID, id, nil)
	if err != nil {
		return response{}, err
	}
	return jsonResponse(200, map[string]any{"buy_box_id": id, "evaluations": list}), nil
}

func (s *server) discoverySearch(c *call) (response, error) {
	var in struct {
		BuyBoxID string   `json:"buy_box_id"`
		Fit      []string `json:"fit"`
	}
	if err := c.decode(&in); err != nil {
		return response{}, err
	}
	if !validUUID(in.BuyBoxID) {
		return response{}, badRequest("buy_box_id must be a UUID")
	}
	for _, fit := range in.Fit {
		if fit != evaluate.FitMatch && fit != evaluate.FitPartialUnverified && fit != evaluate.FitFail {
			return response{}, badRequest("unknown fit " + fmt.Sprintf("%q", fit))
		}
	}
	list, err := s.evaluations(c.ctx(), c.key.WorkspaceID, in.BuyBoxID, in.Fit)
	if err != nil {
		return response{}, err
	}
	return jsonResponse(200, map[string]any{"buy_box_id": in.BuyBoxID, "results": list}), nil
}
