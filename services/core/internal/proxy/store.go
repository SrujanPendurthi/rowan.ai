package proxy

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	"rowan.ai/rowan/services/core/internal/db"
)

// Recompute rebuilds canonical_companies.proxy_summary from registry_signals reachable
// through the company's active links. A missing company row is a no-op.
func Recompute(ctx context.Context, tx pgx.Tx, canonicalID string, now time.Time) (Summary, error) {
	// Row lock serializes concurrent recomputes so a stale batch run cannot overwrite a fresher summary.
	var one int
	err := tx.QueryRow(ctx, `SELECT 1 FROM canonical_companies WHERE id=$1 FOR UPDATE`, canonicalID).Scan(&one)
	if errors.Is(err, pgx.ErrNoRows) {
		return Summary{}, nil
	}
	if err != nil {
		return Summary{}, fmt.Errorf("lock proxy company: %w", err)
	}
	rows, err := tx.Query(ctx, `SELECT s.signal_type,s.observed_at,s.citation
		FROM registry_signals s
		JOIN source_records r ON (r.source_system, r.external_ref) = (s.source_system, s.external_ref)
		JOIN entity_merge_links l ON l.source_record_id = r.id AND l.status = 'active'
		WHERE l.canonical_company_id = $1 ORDER BY s.observed_at, s.id`, canonicalID)
	if err != nil {
		return Summary{}, fmt.Errorf("load proxy signals: %w", err)
	}
	var signals []Signal
	for rows.Next() {
		var sig Signal
		var raw []byte
		if err := rows.Scan(&sig.Type, &sig.ObservedAt, &raw); err != nil {
			rows.Close()
			return Summary{}, fmt.Errorf("scan proxy signal: %w", err)
		}
		if err := json.Unmarshal(raw, &sig.Citation); err != nil {
			rows.Close()
			return Summary{}, fmt.Errorf("decode proxy signal citation: %w", err)
		}
		signals = append(signals, sig)
	}
	rows.Close()
	if err := rows.Err(); err != nil {
		return Summary{}, fmt.Errorf("read proxy signals: %w", err)
	}
	summary := Analyze(signals, now)
	payload, err := json.Marshal(summary)
	if err != nil {
		return Summary{}, fmt.Errorf("marshal proxy summary: %w", err)
	}
	if _, err := tx.Exec(ctx, `UPDATE canonical_companies SET proxy_summary=$2 WHERE id=$1`, canonicalID, payload); err != nil {
		return Summary{}, fmt.Errorf("store proxy summary: %w", err)
	}
	return summary, nil
}

// RecomputeAll refreshes every company with a linked signal or an existing summary, one tx per company.
// ponytail: RecomputeAll scans every company with a summary; batch/paginate if this gets slow
func RecomputeAll(ctx context.Context, pool *pgxpool.Pool, now time.Time) (int, error) {
	ids, err := queryIDs(ctx, pool, `SELECT id::text FROM canonical_companies c
		WHERE c.proxy_summary IS NOT NULL OR EXISTS (
			SELECT 1 FROM entity_merge_links l
			JOIN source_records r ON r.id = l.source_record_id
			JOIN registry_signals s ON (s.source_system, s.external_ref) = (r.source_system, r.external_ref)
			WHERE l.canonical_company_id = c.id AND l.status = 'active')
		ORDER BY id`)
	if err != nil {
		return 0, err
	}
	return recomputeIDs(ctx, pool, ids, now)
}

// RecomputeForRefs refreshes companies actively linked to records with the given refs, in any workspace.
func RecomputeForRefs(ctx context.Context, pool *pgxpool.Pool, sourceSystem string, refs []string, now time.Time) (int, error) {
	if len(refs) == 0 {
		return 0, nil
	}
	ids, err := queryIDs(ctx, pool, `SELECT DISTINCT l.canonical_company_id::text
		FROM entity_merge_links l JOIN source_records r ON r.id = l.source_record_id
		WHERE l.status = 'active' AND r.source_system = $1 AND r.external_ref = ANY($2::text[])
		ORDER BY 1`, sourceSystem, refs)
	if err != nil {
		return 0, err
	}
	return recomputeIDs(ctx, pool, ids, now)
}

func queryIDs(ctx context.Context, pool *pgxpool.Pool, query string, args ...any) ([]string, error) {
	rows, err := pool.Query(ctx, query, args...)
	if err != nil {
		return nil, fmt.Errorf("list proxy companies: %w", err)
	}
	defer rows.Close()
	var ids []string
	for rows.Next() {
		var id string
		if err := rows.Scan(&id); err != nil {
			return nil, fmt.Errorf("scan proxy company: %w", err)
		}
		ids = append(ids, id)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("read proxy companies: %w", err)
	}
	return ids, nil
}

func recomputeIDs(ctx context.Context, pool *pgxpool.Pool, ids []string, now time.Time) (int, error) {
	for i, id := range ids {
		err := db.InTx(ctx, pool, func(tx pgx.Tx) error {
			_, err := Recompute(ctx, tx, id, now)
			return err
		})
		if err != nil {
			return i, fmt.Errorf("recompute proxy %s: %w", id, err)
		}
	}
	return len(ids), nil
}
