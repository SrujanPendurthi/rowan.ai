package ledger

import (
	"context"
	"errors"
	"fmt"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

// CheckInvariants checks lot balances and reservation accounting in one SQL snapshot.
// SQL numeric aggregates avoid overflow when a lot has many reserve/refund cycles.
func CheckInvariants(ctx context.Context, pool *pgxpool.Pool, workspaceID string) error {
	var issue string
	err := pool.QueryRow(ctx, `WITH lot_totals AS (
		SELECT b.id, b.granted, b.balance_remaining,
			COALESCE(sum(e.amount) FILTER (WHERE e.entry_type='reserve'),0) AS reserved,
			COALESCE(sum(e.amount) FILTER (WHERE e.entry_type='refund'),0) AS refunded,
			COALESCE(sum(e.amount) FILTER (WHERE e.entry_type='expire'),0) AS expired
		FROM credit_buckets b LEFT JOIN credit_ledger_entries e ON e.bucket_id=b.id
		WHERE b.workspace_id=$1 GROUP BY b.id
	), reservation_totals AS (
		SELECT r.id, r.amount, r.committed_amount, r.status,
			COALESCE(sum(e.amount) FILTER (WHERE e.entry_type='reserve'),0) AS reserved,
			COALESCE(sum(e.amount) FILTER (WHERE e.entry_type='commit'),0) AS committed,
			COALESCE(sum(e.amount) FILTER (WHERE e.entry_type='refund'),0) AS refunded,
			count(*) FILTER (WHERE e.entry_type IN ('commit','refund')) AS settlement_entries
		FROM credit_reservations r LEFT JOIN credit_ledger_entries e ON e.reservation_id=r.id
		WHERE r.workspace_id=$1 GROUP BY r.id
	), violations AS (
		SELECT 0 AS kind, id::text AS id,
			format('lot %s: granted %s - reserved %s + refunded %s - expired %s != balance %s',
				id, granted, reserved, refunded, expired, balance_remaining) AS issue
		FROM lot_totals WHERE granted-reserved+refunded-expired<>balance_remaining
		UNION ALL
		SELECT 1, id::text,
			format('reservation %s: status %s, amount %s, committed_amount %s; reserve %s, commit %s, refund %s',
				id, status, amount, committed_amount, reserved, committed, refunded)
		FROM reservation_totals WHERE reserved<>amount
			OR (status='reserved' AND settlement_entries<>0)
			OR (status='committed' AND (committed<>committed_amount OR refunded<>amount-committed_amount::numeric))
			OR (status='refunded' AND (refunded<>amount OR committed_amount<>0))
	)
	SELECT issue FROM violations ORDER BY kind, id LIMIT 1`, workspaceID).Scan(&issue)
	if errors.Is(err, pgx.ErrNoRows) {
		return nil
	}
	if err != nil {
		return fmt.Errorf("ledger: check invariants: %w", err)
	}
	return fmt.Errorf("ledger: invariant violation: %s", issue)
}
