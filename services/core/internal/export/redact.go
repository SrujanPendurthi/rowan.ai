package export

import (
	"context"
	"encoding/json"
	"fmt"

	"github.com/jackc/pgx/v5"

	"rowan.ai/rowan/services/core/internal/ledger"
	"rowan.ai/rowan/services/core/internal/quality"
)

// Redacted replaces any value the tier may not see, in CSV cells and API JSON alike.
const Redacted = "[REDACTED]"

// Querier is satisfied by *pgxpool.Pool and pgx.Tx.
type Querier interface {
	Query(ctx context.Context, sql string, args ...any) (pgx.Rows, error)
}

// NameRedactions returns the companies whose display name derives from a legal_name citation
// the tier may not see. A company with no non-empty legal_name citation is never redacted.
func NameRedactions(ctx context.Context, q Querier, companyIDs []string, tier ledger.Tier) (map[string]bool, error) {
	out := map[string]bool{}
	if len(companyIDs) == 0 {
		return out, nil
	}
	rows, err := q.Query(ctx, `SELECT canonical_company_id::text,field_value,data_quality_status,export_policy,retrieved_at
		FROM field_citations WHERE canonical_company_id=ANY($1::uuid[]) AND field_name='legal_name' ORDER BY id`, companyIDs)
	if err != nil {
		return nil, fmt.Errorf("export: load legal name citations: %w", err)
	}
	defer rows.Close()
	byCompany := map[string][]quality.Citation{}
	for rows.Next() {
		var id string
		var c quality.Citation
		if err := rows.Scan(&id, &c.Value, &c.Status, &c.ExportPolicy, &c.RetrievedAt); err != nil {
			return nil, fmt.Errorf("export: scan legal name citation: %w", err)
		}
		var name string
		if json.Unmarshal(c.Value, &name) == nil && name != "" { // same filter as the display-name refresh
			byCompany[id] = append(byCompany[id], c)
		}
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("export: read legal name citations: %w", err)
	}
	for id, cs := range byCompany {
		if best, ok := quality.BestCitation(cs); ok && !Permitted(best.ExportPolicy, tier) {
			out[id] = true
		}
	}
	return out, nil
}
