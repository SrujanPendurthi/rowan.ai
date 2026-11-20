package evaluate

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"time"

	"github.com/jackc/pgx/v5"
	"rowan.ai/rowan/services/core/internal/quality"
)

var ErrNotFound = errors.New("buy box or canonical company not found in workspace")

// EvaluateCompany persists an evaluation inside the caller's transaction.
func EvaluateCompany(ctx context.Context, tx pgx.Tx, buyBoxID, canonicalID string, now time.Time) (Evaluation, error) {
	var raw []byte
	err := tx.QueryRow(ctx, `SELECT b.criteria FROM buy_boxes b JOIN canonical_companies c ON c.workspace_id=b.workspace_id WHERE b.id=$1 AND c.id=$2`, buyBoxID, canonicalID).Scan(&raw)
	if errors.Is(err, pgx.ErrNoRows) {
		return Evaluation{}, fmt.Errorf("evaluate company: %w", ErrNotFound)
	}
	if err != nil {
		return Evaluation{}, fmt.Errorf("load buy box: %w", err)
	}
	criteria, err := ParseCriteria(raw)
	if err != nil {
		return Evaluation{}, fmt.Errorf("evaluate company: %w", err)
	}
	citations, err := loadCitations(ctx, tx, canonicalID)
	if err != nil {
		return Evaluation{}, err
	}
	result := Evaluate(criteria, citations)
	breakdown, err := json.Marshal(result.Criteria)
	if err != nil {
		return Evaluation{}, fmt.Errorf("marshal evaluation: %w", err)
	}
	_, err = tx.Exec(ctx, `INSERT INTO buy_box_evaluations(buy_box_id,canonical_company_id,overall_score,fit,criteria_breakdown,evaluated_at)
 VALUES ($1,$2,$3,$4,$5,$6) ON CONFLICT (buy_box_id,canonical_company_id) DO UPDATE SET
 overall_score=EXCLUDED.overall_score,fit=EXCLUDED.fit,criteria_breakdown=EXCLUDED.criteria_breakdown,evaluated_at=EXCLUDED.evaluated_at`, buyBoxID, canonicalID, result.OverallScore, result.Fit, breakdown, now)
	if err != nil {
		return Evaluation{}, fmt.Errorf("persist evaluation: %w", err)
	}
	return result, nil
}

func loadCitations(ctx context.Context, tx pgx.Tx, canonicalID string) ([]quality.Citation, error) {
	rows, err := tx.Query(ctx, `SELECT field_name,field_value,origin_type,data_quality_status,export_policy,source_system,source_ref,
 COALESCE(table_name,''),COALESCE(row_key,''),COALESCE(page_or_span,''),retrieved_at,source_record_id::text
 FROM field_citations WHERE canonical_company_id=$1 ORDER BY id`, canonicalID)
	if err != nil {
		return nil, fmt.Errorf("load evaluation citations: %w", err)
	}
	defer rows.Close()
	var citations []quality.Citation
	for rows.Next() {
		var c quality.Citation
		if err := rows.Scan(&c.FieldName, &c.Value, &c.Origin, &c.Status, &c.ExportPolicy, &c.SourceSystem, &c.SourceRef, &c.TableName, &c.RowKey, &c.PageOrSpan, &c.RetrievedAt, &c.SourceRecordID); err != nil {
			return nil, fmt.Errorf("scan evaluation citation: %w", err)
		}
		citations = append(citations, c)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("read evaluation citations: %w", err)
	}
	return citations, nil
}

// ReevaluateCompany updates every buy box in the canonical company's workspace.
func ReevaluateCompany(ctx context.Context, tx pgx.Tx, canonicalID string, now time.Time) error {
	var workspaceID string
	err := tx.QueryRow(ctx, `SELECT workspace_id::text FROM canonical_companies WHERE id=$1`, canonicalID).Scan(&workspaceID)
	if errors.Is(err, pgx.ErrNoRows) {
		return fmt.Errorf("reevaluate company: %w", ErrNotFound)
	}
	if err != nil {
		return fmt.Errorf("load evaluation workspace: %w", err)
	}
	rows, err := tx.Query(ctx, `SELECT id::text FROM buy_boxes WHERE workspace_id=$1 ORDER BY id`, workspaceID)
	if err != nil {
		return fmt.Errorf("load workspace buy boxes: %w", err)
	}
	var ids []string
	for rows.Next() {
		var id string
		if err := rows.Scan(&id); err != nil {
			rows.Close()
			return fmt.Errorf("scan workspace buy box: %w", err)
		}
		ids = append(ids, id)
	}
	rows.Close()
	if err := rows.Err(); err != nil {
		return fmt.Errorf("read workspace buy boxes: %w", err)
	}
	for _, id := range ids {
		if _, err := EvaluateCompany(ctx, tx, id, canonicalID, now); err != nil {
			return fmt.Errorf("reevaluate company: %w", err)
		}
	}
	return nil
}
