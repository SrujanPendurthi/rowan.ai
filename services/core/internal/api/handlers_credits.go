package api

import (
	"fmt"
	"time"

	"github.com/jackc/pgx/v5"
)

func (s *server) credits(c *call) (response, error) {
	available := map[string]int64{"rollover": 0, "included": 0, "topup": 0}
	rows, err := s.Pool.Query(c.ctx(), `SELECT bucket_type, COALESCE(sum(balance_remaining),0)::bigint FROM credit_buckets
		WHERE workspace_id=$1 AND expires_at>$2 GROUP BY bucket_type`, c.key.WorkspaceID, c.now)
	if err != nil {
		return response{}, fmt.Errorf("sum credit buckets: %w", err)
	}
	var total int64
	for rows.Next() {
		var kind string
		var n int64
		if err := rows.Scan(&kind, &n); err != nil {
			rows.Close()
			return response{}, fmt.Errorf("scan credit buckets: %w", err)
		}
		available[kind] = n
		total += n
	}
	rows.Close()
	if err := rows.Err(); err != nil {
		return response{}, fmt.Errorf("read credit buckets: %w", err)
	}
	// Same month-to-date measure the ledger enforces for the spend cap: reserved + committed credits.
	monthStart := time.Date(c.now.Year(), c.now.Month(), 1, 0, 0, 0, 0, time.UTC)
	var spent int64
	err = s.Pool.QueryRow(c.ctx(), `SELECT COALESCE(sum(CASE status WHEN 'reserved' THEN amount
		WHEN 'committed' THEN committed_amount ELSE 0 END),0)::bigint FROM credit_reservations
		WHERE api_key_id=$1 AND created_at>=$2 AND created_at<$3`, c.key.ID, monthStart, monthStart.AddDate(0, 1, 0)).Scan(&spent)
	if err != nil && err != pgx.ErrNoRows {
		return response{}, fmt.Errorf("sum key spend: %w", err)
	}
	return jsonResponse(200, map[string]any{
		"available":     available,
		"total":         total,
		"month_to_date": map[string]int64{"spent": spent, "cap": c.key.SpendCap},
	}), nil
}
