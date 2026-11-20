package api

import (
	"context"
	"encoding/json"
	"fmt"

	"github.com/jackc/pgx/v5"

	"rowan.ai/rowan/services/core/internal/evaluate"
	"rowan.ai/rowan/services/core/internal/export"
	"rowan.ai/rowan/services/core/internal/ledger"
	"rowan.ai/rowan/services/core/internal/quality"
)

var redactedValue = json.RawMessage(`"` + export.Redacted + `"`)

// workspaceTier loads the tier that decides which cited values the caller may see.
func workspaceTier(ctx context.Context, q rower, workspaceID string) (ledger.Tier, error) {
	var tier ledger.Tier
	if err := q.QueryRow(ctx, `SELECT tier FROM workspaces WHERE id=$1`, workspaceID).Scan(&tier); err != nil {
		return "", fmt.Errorf("load workspace tier: %w", err)
	}
	return tier, nil
}

// rower is satisfied by *pgxpool.Pool and pgx.Tx.
type rower interface {
	QueryRow(ctx context.Context, sql string, args ...any) pgx.Row
}

// redactCitation hides field_value when the tier may not export it; provenance fields stay.
func redactCitation(c *quality.Citation, tier ledger.Tier) bool {
	if export.Permitted(c.ExportPolicy, tier) {
		return false
	}
	c.Value = redactedValue
	return true
}

// redactBreakdown redacts `observed` and the nested citation value of every criterion whose citation the tier may not export.
func redactBreakdown(raw json.RawMessage, tier ledger.Tier) (json.RawMessage, error) {
	var results []evaluate.CriterionResult
	if err := json.Unmarshal(raw, &results); err != nil {
		return nil, fmt.Errorf("decode criteria breakdown: %w", err)
	}
	changed := false
	for i := range results {
		if cit := results[i].Citation; cit != nil && redactCitation(cit, tier) {
			results[i].Observed = redactedValue
			changed = true
		}
	}
	if !changed {
		return raw, nil
	}
	out, err := json.Marshal(results)
	if err != nil {
		return nil, fmt.Errorf("encode criteria breakdown: %w", err)
	}
	return out, nil
}
