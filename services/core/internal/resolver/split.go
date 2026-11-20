package resolver

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgtype"
	"rowan.ai/rowan/services/core/internal/evaluate"
	"rowan.ai/rowan/services/core/internal/quality"
)

// ErrInvalidSplit reports a split request that cannot be applied; nothing is written.
var ErrInvalidSplit = errors.New("invalid split request")

func validUUID(s string) bool {
	var id pgtype.UUID
	return id.Scan(s) == nil && id.Valid
}

// Split detaches active source records from a canonical company into a new one in the caller's transaction.
// It takes the workspace row lock, then the canonical row lock; callers must hold no later-ordered locks.
func Split(ctx context.Context, tx pgx.Tx, canonicalID string, sourceRecordIDs []string, actor, reason string, now time.Time) (string, error) {
	if actor == "" || len(sourceRecordIDs) == 0 || !validUUID(canonicalID) {
		return "", fmt.Errorf("split: %w", ErrInvalidSplit)
	}
	if reason == "" {
		reason = "manual_split"
	}
	moved := make(map[string]bool, len(sourceRecordIDs))
	for _, id := range sourceRecordIDs {
		if !validUUID(id) || moved[id] {
			return "", fmt.Errorf("split: %w", ErrInvalidSplit)
		}
		moved[id] = true
	}
	var workspaceID string
	err := tx.QueryRow(ctx, `SELECT workspace_id::text FROM canonical_companies WHERE id=$1`, canonicalID).Scan(&workspaceID)
	if errors.Is(err, pgx.ErrNoRows) {
		return "", fmt.Errorf("split: %w", ErrInvalidSplit)
	}
	if err != nil {
		return "", fmt.Errorf("load split workspace: %w", err)
	}
	if err := LockWorkspace(ctx, tx, workspaceID); err != nil {
		return "", err
	}
	err = tx.QueryRow(ctx, `SELECT workspace_id::text FROM canonical_companies WHERE id=$1 FOR UPDATE`, canonicalID).Scan(&workspaceID)
	if errors.Is(err, pgx.ErrNoRows) {
		return "", fmt.Errorf("split: %w", ErrInvalidSplit)
	}
	if err != nil {
		return "", fmt.Errorf("lock split canonical: %w", err)
	}
	active, err := activeRecordIDs(ctx, tx, canonicalID)
	if err != nil {
		return "", err
	}
	var remaining []string
	found := 0
	for _, id := range active {
		if moved[id] {
			found++
		} else {
			remaining = append(remaining, id)
		}
	}
	if found != len(moved) || len(remaining) == 0 {
		return "", fmt.Errorf("split: %w", ErrInvalidSplit)
	}
	var newID string
	err = tx.QueryRow(ctx, `INSERT INTO canonical_companies(workspace_id,display_name,pending_review,created_at,updated_at)
		VALUES ($1,'',false,$2,$2) RETURNING id::text`, workspaceID, now).Scan(&newID)
	if err != nil {
		return "", fmt.Errorf("create split canonical: %w", err)
	}
	if _, err := tx.Exec(ctx, `UPDATE entity_merge_links SET canonical_company_id=$2,merge_mode='manual',status='active'
		WHERE source_record_id=ANY($1::uuid[])`, sourceRecordIDs, newID); err != nil {
		return "", fmt.Errorf("move split links: %w", err)
	}
	if _, err := tx.Exec(ctx, `UPDATE field_citations SET canonical_company_id=$2 WHERE source_record_id=ANY($1::uuid[])`, sourceRecordIDs, newID); err != nil {
		return "", fmt.Errorf("move split citations: %w", err)
	}
	for _, m := range sourceRecordIDs {
		for _, r := range remaining {
			a, b := OrderedPair(m, r)
			_, err := tx.Exec(ctx, `INSERT INTO do_not_merge_pairs(record_a_id,record_b_id,reason,created_by,created_at)
				VALUES ($1,$2,$3,$4,$5) ON CONFLICT DO NOTHING`, a, b, reason, actor, now)
			if err != nil {
				return "", fmt.Errorf("insert split pair: %w", err)
			}
		}
	}
	for _, id := range []string{canonicalID, newID} {
		if err := refreshDisplayName(ctx, tx, id, now); err != nil {
			return "", err
		}
		// Moved records may carry pending review candidates; each side's flag follows its own records.
		if _, err := tx.Exec(ctx, `UPDATE canonical_companies SET pending_review = EXISTS (
			SELECT 1 FROM possible_match_candidates p JOIN entity_merge_links l ON l.source_record_id=p.source_record_id
			WHERE l.canonical_company_id=$1 AND l.status='active' AND p.status='pending') WHERE id=$1`, id); err != nil {
			return "", fmt.Errorf("refresh split pending review: %w", err)
		}
		if err := evaluate.ReevaluateCompany(ctx, tx, id, now); err != nil {
			return "", fmt.Errorf("reevaluate split company: %w", err)
		}
	}
	return newID, nil
}

func activeRecordIDs(ctx context.Context, tx pgx.Tx, canonicalID string) ([]string, error) {
	rows, err := tx.Query(ctx, `SELECT source_record_id::text FROM entity_merge_links
		WHERE canonical_company_id=$1 AND status='active' ORDER BY created_at,source_record_id`, canonicalID)
	if err != nil {
		return nil, fmt.Errorf("load split records: %w", err)
	}
	defer rows.Close()
	var ids []string
	for rows.Next() {
		var id string
		if err := rows.Scan(&id); err != nil {
			return nil, fmt.Errorf("scan split record: %w", err)
		}
		ids = append(ids, id)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("read split records: %w", err)
	}
	return ids, nil
}

// refreshDisplayName prefers the best legal_name citation, then the first record's legal name, then its external ref.
func refreshDisplayName(ctx context.Context, tx pgx.Tx, canonicalID string, now time.Time) error {
	rows, err := tx.Query(ctx, `SELECT field_value,data_quality_status,retrieved_at FROM field_citations
		WHERE canonical_company_id=$1 AND field_name='legal_name' ORDER BY id`, canonicalID)
	if err != nil {
		return fmt.Errorf("load legal name citations: %w", err)
	}
	var cs []quality.Citation
	for rows.Next() {
		var c quality.Citation
		if err := rows.Scan(&c.Value, &c.Status, &c.RetrievedAt); err != nil {
			rows.Close()
			return fmt.Errorf("scan legal name citation: %w", err)
		}
		var s string
		if json.Unmarshal(c.Value, &s) == nil && s != "" {
			cs = append(cs, c)
		}
	}
	rows.Close()
	if err := rows.Err(); err != nil {
		return fmt.Errorf("read legal name citations: %w", err)
	}
	var name string
	if best, ok := quality.BestCitation(cs); ok {
		_ = json.Unmarshal(best.Value, &name)
	} else {
		var raw []byte
		var ref string
		err := tx.QueryRow(ctx, `SELECT s.payload->'facts',s.external_ref FROM entity_merge_links l
			JOIN source_records s ON s.id=l.source_record_id
			WHERE l.canonical_company_id=$1 AND l.status='active' ORDER BY l.created_at,s.id LIMIT 1`, canonicalID).Scan(&raw, &ref)
		if err != nil {
			return fmt.Errorf("load display name record: %w", err)
		}
		var facts RecordFacts
		if err := json.Unmarshal(raw, &facts); err != nil {
			return fmt.Errorf("decode display name facts: %w", err)
		}
		if name = facts.LegalName; name == "" {
			name = ref
		}
	}
	if _, err := tx.Exec(ctx, `UPDATE canonical_companies SET display_name=$2,updated_at=$3 WHERE id=$1`, canonicalID, name, now); err != nil {
		return fmt.Errorf("update display name: %w", err)
	}
	return nil
}
