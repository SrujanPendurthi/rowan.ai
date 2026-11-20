package resolver

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/json"
	"errors"
	"fmt"
	"sort"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgtype"
	"rowan.ai/rowan/services/core/internal/evaluate"
	"rowan.ai/rowan/services/core/internal/quality"
)

var (
	ErrInvalidRequest  = errors.New("invalid resolver request")
	ErrAlreadyResolved = errors.New("review candidate already resolved")
	ErrDoNotMerge      = errors.New("records must not merge")
)

type SourceRecordInput struct {
	WorkspaceID, SourceSystem, ExternalRef string
	Facts                                  RecordFacts
	Payload                                json.RawMessage
	Citations                              []quality.Citation
	FetchedAt                              time.Time
}

type ResolveResult struct {
	SourceRecordID, CanonicalID string
	Decision                    Decision
	Score                       float64
	Reasons                     []string
	ReviewCandidateID           string
}

type recordPayload struct {
	Facts     RecordFacts     `json:"facts"`
	Raw       json.RawMessage `json:"raw"`
	BlockKeys []string        `json:"block_keys"`
}

func blockKeys(f RecordFacts) []string {
	keys := make(map[string]bool)
	add := func(prefix, a, separator, b string) {
		if a != "" && b != "" {
			keys[prefix+a+separator+b] = true
		}
	}
	for key, value := range f.IDs {
		add("id:", normalizeText(key), "=", normalizeText(value))
	}
	state := normalizeText(f.State)
	add("name:", NormalizeName(f.LegalName), "|", state)
	add("name:", NormalizeName(f.DBA), "|", state)
	add("namezip:", NormalizeName(f.LegalName), "|", normalizeText(f.Zip))
	add("namezip:", NormalizeName(f.DBA), "|", normalizeText(f.Zip))
	add("addr:", normalizeAddress(f.Address1), "|", normalizeText(f.Zip))
	add("agent:", NormalizeName(f.RegisteredAgent), "|", state)
	result := make([]string, 0, len(keys))
	for key := range keys {
		result = append(result, key)
	}
	sort.Strings(result)
	return result
}

// OrderedPair returns UUID strings in the order required by do_not_merge_pairs.
func OrderedPair(a, b string) (string, string) {
	if a > b {
		return b, a
	}
	return a, b
}

// LockWorkspace serializes resolution writes, including concurrent first ingests.
// It takes the workspace row lock; callers must acquire it before any ledger or source_fetch_cache row locks in the same tx.
// Repeated calls within the same transaction are safe and retain the lock until transaction end.
func LockWorkspace(ctx context.Context, tx pgx.Tx, workspaceID string) error {
	var id string
	// NO KEY UPDATE permits FK key-share locks already held by enrichment callers.
	err := tx.QueryRow(ctx, `SELECT id::text FROM workspaces WHERE id=$1 FOR NO KEY UPDATE`, workspaceID).Scan(&id)
	if errors.Is(err, pgx.ErrNoRows) {
		return fmt.Errorf("lock resolver workspace: %w", ErrInvalidRequest)
	}
	if err != nil {
		return fmt.Errorf("lock resolver workspace: %w", err)
	}
	return nil
}

// Resolve ingests a source record and updates resolution in the caller's transaction.
// Existing links survive content changes, preserving rejection and split history.
// It takes the workspace row lock; callers must acquire it before any ledger or source_fetch_cache row locks in the same tx.
func Resolve(ctx context.Context, tx pgx.Tx, in SourceRecordInput, now time.Time) (ResolveResult, error) {
	if in.WorkspaceID == "" || in.SourceSystem == "" || in.ExternalRef == "" {
		return ResolveResult{}, fmt.Errorf("resolve record: %w", ErrInvalidRequest)
	}
	for _, c := range in.Citations {
		if err := c.Validate(); err != nil {
			return ResolveResult{}, fmt.Errorf("validate resolver citation: %w", err)
		}
	}
	keys := blockKeys(in.Facts)
	payload, err := json.Marshal(recordPayload{Facts: in.Facts, Raw: in.Payload, BlockKeys: keys})
	if err != nil {
		return ResolveResult{}, fmt.Errorf("marshal source payload: %w", err)
	}
	hash := sha256.Sum256(payload)
	if err := LockWorkspace(ctx, tx, in.WorkspaceID); err != nil {
		return ResolveResult{}, err
	}
	var recordID string
	var oldHash []byte
	err = tx.QueryRow(ctx, `SELECT id::text,content_hash FROM source_records
		WHERE workspace_id=$1 AND source_system=$2 AND external_ref=$3 FOR UPDATE`,
		in.WorkspaceID, in.SourceSystem, in.ExternalRef).Scan(&recordID, &oldHash)
	if err != nil && !errors.Is(err, pgx.ErrNoRows) {
		return ResolveResult{}, fmt.Errorf("find source record: %w", err)
	}
	if err == nil {
		result, linked, err := existingLink(ctx, tx, recordID)
		if err != nil {
			return ResolveResult{}, err
		}
		if linked && bytes.Equal(oldHash, hash[:]) {
			return result, nil
		}
		if _, err := tx.Exec(ctx, `UPDATE source_records SET payload=$2,content_hash=$3,fetched_at=$4 WHERE id=$1`, recordID, payload, hash[:], in.FetchedAt); err != nil {
			return ResolveResult{}, fmt.Errorf("update source record: %w", err)
		}
		if linked {
			if err := replaceCitations(ctx, tx, recordID, result.CanonicalID, in.Citations); err != nil {
				return ResolveResult{}, err
			}
			if err := evaluate.ReevaluateCompany(ctx, tx, result.CanonicalID, now); err != nil {
				return ResolveResult{}, fmt.Errorf("reevaluate updated record: %w", err)
			}
			return result, nil
		}
	} else {
		err := tx.QueryRow(ctx, `INSERT INTO source_records(workspace_id,source_system,external_ref,payload,content_hash,fetched_at)
			VALUES ($1,$2,$3,$4,$5,$6) RETURNING id::text`,
			in.WorkspaceID, in.SourceSystem, in.ExternalRef, payload, hash[:], in.FetchedAt).Scan(&recordID)
		if err != nil {
			return ResolveResult{}, fmt.Errorf("insert source record: %w", err)
		}
	}
	return resolveNewRecord(ctx, tx, in, recordID, keys, now)
}

func existingLink(ctx context.Context, tx pgx.Tx, recordID string) (ResolveResult, bool, error) {
	result := ResolveResult{SourceRecordID: recordID}
	var mode string
	err := tx.QueryRow(ctx, `SELECT canonical_company_id::text,merge_confidence_score,match_reasons,merge_mode
		FROM entity_merge_links WHERE source_record_id=$1`, recordID).
		Scan(&result.CanonicalID, &result.Score, &result.Reasons, &mode)
	if errors.Is(err, pgx.ErrNoRows) {
		return result, false, nil
	}
	if err != nil {
		return result, false, fmt.Errorf("load existing resolution: %w", err)
	}
	err = tx.QueryRow(ctx, `SELECT id::text FROM possible_match_candidates
		WHERE source_record_id=$1 AND status='pending' ORDER BY id LIMIT 1`, recordID).Scan(&result.ReviewCandidateID)
	if err != nil && !errors.Is(err, pgx.ErrNoRows) {
		return result, false, fmt.Errorf("load existing review candidate: %w", err)
	}
	switch mode {
	case "auto", "manual":
		result.Decision = AutoMerge
	case "seed":
		result.Decision = Distinct
		if result.ReviewCandidateID != "" {
			result.Decision = Review
		}
	}
	return result, true, nil
}

func resolveNewRecord(ctx context.Context, tx pgx.Tx, in SourceRecordInput, recordID string, keys []string, now time.Time) (ResolveResult, error) {
	result, err := bestCandidate(ctx, tx, in, recordID, keys)
	if err != nil {
		return ResolveResult{}, err
	}
	targetID := result.CanonicalID
	result.SourceRecordID = recordID
	result.Decision = Classify(result.Score)
	mode, linkScore, linkReasons := "auto", result.Score, result.Reasons
	if result.Decision != AutoMerge {
		name := in.Facts.LegalName
		if name == "" {
			name = in.Facts.DBA
		}
		if name == "" {
			name = in.ExternalRef
		}
		err := tx.QueryRow(ctx, `INSERT INTO canonical_companies(workspace_id,display_name,pending_review,created_at,updated_at)
			VALUES ($1,$2,$3,$4,$4) RETURNING id::text`, in.WorkspaceID, name, result.Decision == Review, now).Scan(&result.CanonicalID)
		if err != nil {
			return ResolveResult{}, fmt.Errorf("create canonical company: %w", err)
		}
		mode, linkScore, linkReasons = "seed", 1, []string{"seed"}
	}
	_, err = tx.Exec(ctx, `INSERT INTO entity_merge_links(canonical_company_id,source_record_id,merge_confidence_score,match_reasons,merge_mode,status,created_at)
		VALUES ($1,$2,$3,$4,$5,'active',$6)`, result.CanonicalID, recordID, linkScore, linkReasons, mode, now)
	if err != nil {
		return ResolveResult{}, fmt.Errorf("link source record: %w", err)
	}
	if err := replaceCitations(ctx, tx, recordID, result.CanonicalID, in.Citations); err != nil {
		return ResolveResult{}, err
	}
	if result.Decision == Review {
		err := tx.QueryRow(ctx, `INSERT INTO possible_match_candidates(workspace_id,canonical_company_id,source_record_id,score,match_reasons,status)
			VALUES ($1,$2,$3,$4,$5,'pending') RETURNING id::text`, in.WorkspaceID, targetID, recordID, result.Score, result.Reasons).Scan(&result.ReviewCandidateID)
		if err != nil {
			return ResolveResult{}, fmt.Errorf("queue possible match: %w", err)
		}
	}
	if err := evaluate.ReevaluateCompany(ctx, tx, result.CanonicalID, now); err != nil {
		return ResolveResult{}, fmt.Errorf("reevaluate resolved company: %w", err)
	}
	return result, nil
}

func bestCandidate(ctx context.Context, tx pgx.Tx, in SourceRecordInput, recordID string, keys []string) (ResolveResult, error) {
	// ponytail: payload block_keys has no GIN index; add one when record counts grow.
	rows, err := tx.Query(ctx, `WITH blocked AS (
		SELECT DISTINCT l.canonical_company_id FROM source_records s
		JOIN entity_merge_links l ON l.source_record_id=s.id AND l.status='active'
		JOIN canonical_companies c ON c.id=l.canonical_company_id AND c.workspace_id=$1
		WHERE s.workspace_id=$1 AND s.id<>$3 AND (s.payload->'block_keys') ?| $2::text[]
	) SELECT l.canonical_company_id::text,s.payload->'facts' FROM blocked b
	JOIN entity_merge_links l ON l.canonical_company_id=b.canonical_company_id AND l.status='active'
	JOIN source_records s ON s.id=l.source_record_id
	WHERE NOT EXISTS (
		SELECT 1 FROM entity_merge_links other JOIN do_not_merge_pairs p
		ON (p.record_a_id=$3 AND p.record_b_id=other.source_record_id)
		OR (p.record_b_id=$3 AND p.record_a_id=other.source_record_id)
		WHERE other.canonical_company_id=b.canonical_company_id AND other.status='active'
	) ORDER BY l.canonical_company_id,s.id`, in.WorkspaceID, keys, recordID)
	if err != nil {
		return ResolveResult{}, fmt.Errorf("load blocking candidates: %w", err)
	}
	defer rows.Close()
	best := ResolveResult{Reasons: []string{}}
	for rows.Next() {
		var canonicalID string
		var raw []byte
		if err := rows.Scan(&canonicalID, &raw); err != nil {
			return ResolveResult{}, fmt.Errorf("scan blocking candidate: %w", err)
		}
		var facts RecordFacts
		if err := json.Unmarshal(raw, &facts); err != nil {
			return ResolveResult{}, fmt.Errorf("decode blocking candidate facts: %w", err)
		}
		score, reasons := Score(in.Facts, facts)
		// Ordered rows make equal scores choose the lowest canonical and record IDs.
		if best.CanonicalID == "" || score > best.Score {
			best.CanonicalID, best.Score, best.Reasons = canonicalID, score, reasons
		}
	}
	if err := rows.Err(); err != nil {
		return ResolveResult{}, fmt.Errorf("read blocking candidates: %w", err)
	}
	return best, nil
}

func replaceCitations(ctx context.Context, tx pgx.Tx, recordID, canonicalID string, citations []quality.Citation) error {
	if _, err := tx.Exec(ctx, `DELETE FROM field_citations WHERE source_record_id=$1`, recordID); err != nil {
		return fmt.Errorf("delete replaced citations: %w", err)
	}
	for _, c := range citations {
		_, err := tx.Exec(ctx, `INSERT INTO field_citations(canonical_company_id,source_record_id,field_name,field_value,
			origin_type,data_quality_status,export_policy,source_system,source_ref,table_name,row_key,page_or_span,retrieved_at)
			VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13)`, canonicalID, recordID, c.FieldName, c.Value,
			c.Origin, c.Status, c.ExportPolicy, c.SourceSystem, c.SourceRef, c.TableName, c.RowKey, c.PageOrSpan, c.RetrievedAt)
		if err != nil {
			return fmt.Errorf("insert resolver citation: %w", err)
		}
	}
	return nil
}

type reviewCandidate struct {
	id, workspaceID, targetID, recordID, status string
	score                                       float64
	reasons                                     []string
}

// ResolveReview merges or rejects a pending candidate in the caller's transaction.
// It takes the workspace row lock; callers must acquire it before any ledger or source_fetch_cache row locks in the same tx.
func ResolveReview(ctx context.Context, tx pgx.Tx, candidateID, action, actor string, now time.Time) error {
	var id pgtype.UUID
	if err := id.Scan(candidateID); err != nil || !id.Valid || (action != "merge" && action != "reject") {
		return fmt.Errorf("resolve review: %w", ErrInvalidRequest)
	}
	var c reviewCandidate
	c.id = candidateID
	// Use the same workspace-before-record lock order as Resolve.
	err := tx.QueryRow(ctx, `SELECT workspace_id::text FROM possible_match_candidates WHERE id=$1`, candidateID).Scan(&c.workspaceID)
	if errors.Is(err, pgx.ErrNoRows) {
		return fmt.Errorf("find review candidate: %w", ErrInvalidRequest)
	}
	if err != nil {
		return fmt.Errorf("find review candidate: %w", err)
	}
	if err := LockWorkspace(ctx, tx, c.workspaceID); err != nil {
		return err
	}
	err = tx.QueryRow(ctx, `SELECT canonical_company_id::text,source_record_id::text,score,match_reasons,status
		FROM possible_match_candidates WHERE id=$1 FOR UPDATE`, candidateID).
		Scan(&c.targetID, &c.recordID, &c.score, &c.reasons, &c.status)
	if errors.Is(err, pgx.ErrNoRows) {
		return fmt.Errorf("lock review candidate: %w", ErrInvalidRequest)
	}
	if err != nil {
		return fmt.Errorf("lock review candidate: %w", err)
	}
	if (c.status == "merged" && action == "merge") || (c.status == "rejected" && action == "reject") {
		return nil
	}
	if c.status != "pending" {
		return fmt.Errorf("resolve review: %w", ErrAlreadyResolved)
	}
	var oldID string
	err = tx.QueryRow(ctx, `SELECT canonical_company_id::text FROM entity_merge_links WHERE source_record_id=$1 FOR UPDATE`, c.recordID).Scan(&oldID)
	if errors.Is(err, pgx.ErrNoRows) {
		return fmt.Errorf("find review record link: %w", ErrInvalidRequest)
	}
	if err != nil {
		return fmt.Errorf("lock review record link: %w", err)
	}
	status := "rejected"
	if action == "merge" {
		var blocked bool
		err := tx.QueryRow(ctx, `SELECT EXISTS (
			SELECT 1 FROM entity_merge_links l JOIN do_not_merge_pairs p
			ON (p.record_a_id=$1 AND p.record_b_id=l.source_record_id)
			OR (p.record_b_id=$1 AND p.record_a_id=l.source_record_id)
			WHERE l.canonical_company_id=$2 AND l.status='active'
		)`, c.recordID, c.targetID).Scan(&blocked)
		if err != nil {
			return fmt.Errorf("check review do-not-merge pairs: %w", err)
		}
		if blocked {
			return fmt.Errorf("resolve review merge: %w", ErrDoNotMerge)
		}
		status = "merged"
		_, err = tx.Exec(ctx, `UPDATE entity_merge_links SET canonical_company_id=$2,merge_mode='manual',
			merge_confidence_score=$3,match_reasons=$4,status='active' WHERE source_record_id=$1`, c.recordID, c.targetID, c.score, c.reasons)
		if err != nil {
			return fmt.Errorf("move reviewed link: %w", err)
		}
		if _, err := tx.Exec(ctx, `UPDATE field_citations SET canonical_company_id=$2 WHERE source_record_id=$1`, c.recordID, c.targetID); err != nil {
			return fmt.Errorf("move reviewed citations: %w", err)
		}
	} else if err := rejectPairs(ctx, tx, c, actor, now); err != nil {
		return err
	}
	_, err = tx.Exec(ctx, `UPDATE possible_match_candidates SET status=$2,resolved_by=$3,resolved_at=$4 WHERE id=$1`, candidateID, status, actor, now)
	if err != nil {
		return fmt.Errorf("mark review resolved: %w", err)
	}
	if err := clearPendingReview(ctx, tx, oldID, now); err != nil {
		return err
	}
	if action == "merge" {
		if err := cleanMergedCompany(ctx, tx, oldID, c.targetID, c.id, now); err != nil {
			return err
		}
		if err := evaluate.ReevaluateCompany(ctx, tx, c.targetID, now); err != nil {
			return fmt.Errorf("reevaluate review target: %w", err)
		}
	}
	return nil
}

func rejectPairs(ctx context.Context, tx pgx.Tx, c reviewCandidate, actor string, now time.Time) error {
	rows, err := tx.Query(ctx, `SELECT source_record_id::text FROM entity_merge_links
		WHERE canonical_company_id=$1 AND status='active' ORDER BY source_record_id`, c.targetID)
	if err != nil {
		return fmt.Errorf("load rejected target records: %w", err)
	}
	var ids []string
	for rows.Next() {
		var id string
		if err := rows.Scan(&id); err != nil {
			rows.Close()
			return fmt.Errorf("scan rejected target record: %w", err)
		}
		ids = append(ids, id)
	}
	rows.Close()
	if err := rows.Err(); err != nil {
		return fmt.Errorf("read rejected target records: %w", err)
	}
	for _, id := range ids {
		if id == c.recordID {
			continue
		}
		a, b := OrderedPair(c.recordID, id)
		_, err := tx.Exec(ctx, `INSERT INTO do_not_merge_pairs(record_a_id,record_b_id,reason,created_by,created_at)
			VALUES ($1,$2,'review_rejected',$3,$4) ON CONFLICT DO NOTHING`, a, b, actor, now)
		if err != nil {
			return fmt.Errorf("insert rejected pair: %w", err)
		}
	}
	return nil
}

func clearPendingReview(ctx context.Context, tx pgx.Tx, canonicalID string, now time.Time) error {
	_, err := tx.Exec(ctx, `UPDATE canonical_companies SET pending_review=false,updated_at=$2 WHERE id=$1
		AND NOT EXISTS (SELECT 1 FROM possible_match_candidates p JOIN entity_merge_links l
		ON l.source_record_id=p.source_record_id WHERE l.canonical_company_id=$1 AND l.status='active' AND p.status='pending')`, canonicalID, now)
	if err != nil {
		return fmt.Errorf("clear pending review: %w", err)
	}
	return nil
}

func cleanMergedCompany(ctx context.Context, tx pgx.Tx, oldID, targetID, candidateID string, now time.Time) error {
	if oldID == targetID {
		return nil
	}
	var linked bool
	err := tx.QueryRow(ctx, `SELECT EXISTS (SELECT 1 FROM entity_merge_links WHERE canonical_company_id=$1)`, oldID).Scan(&linked)
	if err != nil {
		return fmt.Errorf("check merged company links: %w", err)
	}
	if linked {
		// A retained company also lost citations, so its evaluations must be refreshed.
		if err := evaluate.ReevaluateCompany(ctx, tx, oldID, now); err != nil {
			return fmt.Errorf("reevaluate retained review company: %w", err)
		}
		return nil
	}
	_, err = tx.Exec(ctx, `UPDATE possible_match_candidates p SET canonical_company_id=$2
		WHERE p.canonical_company_id=$1 AND p.status='pending' AND p.id<>$3
		AND NOT EXISTS (SELECT 1 FROM possible_match_candidates existing
		WHERE existing.canonical_company_id=$2 AND existing.source_record_id=p.source_record_id)`, oldID, targetID, candidateID)
	if err != nil {
		return fmt.Errorf("retarget pending review candidates: %w", err)
	}
	if _, err := tx.Exec(ctx, `DELETE FROM canonical_companies WHERE id=$1`, oldID); err != nil {
		return fmt.Errorf("delete empty merged company: %w", err)
	}
	return nil
}
