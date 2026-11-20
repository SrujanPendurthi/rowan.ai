// Package export writes policy-gated CSV artifacts and their audit records.
package export

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/csv"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	"rowan.ai/rowan/services/core/internal/ledger"
	"rowan.ai/rowan/services/core/internal/quality"
)

var (
	ErrInvalidRequest = errors.New("invalid export request")
	ErrUnknownColumn  = errors.New("unknown export column")
	ErrNotFound       = errors.New("export buy box not found")
)

type Request struct {
	WorkspaceID, ActorID, BuyBoxID string
	Columns                        []string
	Fit                            []string
}

type Result struct {
	AuditID, ArtifactRef string
	RowCount             int
	RedactedFields       []string
	CSV                  []byte
}

// Permitted fails closed for unknown policies and tiers.
func Permitted(p quality.ExportPolicy, t ledger.Tier) bool {
	if t != ledger.Solo && t != ledger.DealTeam && t != ledger.Fund {
		return false
	}
	switch p {
	case quality.Allow:
		return true
	case quality.DealTeamPlus:
		return t == ledger.DealTeam || t == ledger.Fund
	case quality.FundOnly:
		return t == ledger.Fund
	default:
		return false
	}
}

func validate(req Request) error {
	allowed := map[string]bool{"legal_name": true, "dba": true, "naics": true, "state": true, "zip": true,
		"address": true, "domain": true, "phone": true, "principal": true, "revenue_cents": true, "headcount": true, "contract_value_cents": true}
	// Unknown columns take precedence and are rejected before any I/O.
	for _, col := range req.Columns {
		if !allowed[col] {
			return fmt.Errorf("%w: %q", ErrUnknownColumn, col)
		}
	}
	if req.WorkspaceID == "" || req.ActorID == "" || req.BuyBoxID == "" {
		return ErrInvalidRequest
	}
	seen := make(map[string]bool, len(req.Columns))
	for _, col := range req.Columns {
		if seen[col] {
			return fmt.Errorf("%w: duplicate column %q", ErrInvalidRequest, col)
		}
		seen[col] = true
	}
	for _, fit := range req.Fit {
		if fit != "match" && fit != "partial_unverified" && fit != "fail" {
			return fmt.Errorf("%w: fit %q", ErrInvalidRequest, fit)
		}
	}
	return nil
}

// Export reads a repeatable snapshot, writes an artifact, and commits its audit.
func Export(ctx context.Context, pool *pgxpool.Pool, artifactDir string, req Request, now time.Time) (Result, error) {
	if err := validate(req); err != nil {
		return Result{}, fmt.Errorf("export: validate: %w", err)
	}
	tx, err := pool.BeginTx(ctx, pgx.TxOptions{IsoLevel: pgx.RepeatableRead})
	if err != nil {
		return Result{}, fmt.Errorf("export: begin: %w", err)
	}
	defer func() {
		cleanupCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), 5*time.Second)
		defer cancel()
		_ = tx.Rollback(cleanupCtx)
	}()
	var tier ledger.Tier
	err = tx.QueryRow(ctx, `SELECT w.tier FROM buy_boxes b JOIN workspaces w ON w.id=b.workspace_id
 WHERE b.id=$1 AND b.workspace_id=$2`, req.BuyBoxID, req.WorkspaceID).Scan(&tier)
	if errors.Is(err, pgx.ErrNoRows) {
		err = ErrNotFound
	}
	if err != nil {
		return Result{}, fmt.Errorf("export: buy box: %w", err)
	}
	result, err := buildCSV(ctx, tx, req, tier)
	if err != nil {
		return Result{}, fmt.Errorf("export: CSV: %w", err)
	}
	result.ArtifactRef = fmt.Sprintf("%x.csv", sha256.Sum256(result.CSV))
	if err := writeArtifact(ctx, artifactDir, result.ArtifactRef, result.CSV); err != nil {
		return Result{}, fmt.Errorf("export: artifact: %w", err)
	}
	// ponytail: Local disk is content-addressed until object storage is added.
	// A failed audit/commit can leave a harmless orphan artifact for later collection.
	columns := append([]string{}, req.Columns...)
	err = tx.QueryRow(ctx, `INSERT INTO export_audits(workspace_id,user_or_key_id,buy_box_id,row_count,
 requested_columns,redacted_fields,credits_charged,artifact_ref,created_at)
 VALUES ($1,$2,$3,$4,$5,$6,0,$7,$8) RETURNING id::text`, req.WorkspaceID, req.ActorID, req.BuyBoxID,
		result.RowCount, columns, result.RedactedFields, result.ArtifactRef, now.UTC()).Scan(&result.AuditID)
	if err != nil {
		return Result{}, fmt.Errorf("export: audit: %w", err)
	}
	if err := tx.Commit(ctx); err != nil {
		return Result{}, fmt.Errorf("export: commit: %w", err)
	}
	return result, nil
}

type companyRow struct{ id, name, fit, score string }

func buildCSV(ctx context.Context, tx pgx.Tx, req Request, tier ledger.Tier) (Result, error) {
	rows, err := tx.Query(ctx, `SELECT c.id::text,c.display_name,e.fit,e.overall_score::text
 FROM buy_box_evaluations e JOIN canonical_companies c ON c.id=e.canonical_company_id
 WHERE e.buy_box_id=$1 AND c.workspace_id=$2 AND (cardinality($3::text[])=0 OR e.fit=ANY($3::text[]))
 ORDER BY e.overall_score DESC,c.id ASC`, req.BuyBoxID, req.WorkspaceID, append([]string{}, req.Fit...))
	if err != nil {
		return Result{}, err
	}
	companies, err := pgx.CollectRows(rows, func(row pgx.CollectableRow) (companyRow, error) {
		var c companyRow
		err := row.Scan(&c.id, &c.name, &c.fit, &c.score)
		return c, err
	})
	if err != nil {
		return Result{}, err
	}
	var buf bytes.Buffer
	writer := csv.NewWriter(&buf)
	header := []string{"canonical_company_id", "display_name", "fit", "overall_score"}
	for _, col := range req.Columns {
		header = append(header, col, col+"__data_quality_status", col+"__source")
	}
	if err := writer.Write(header); err != nil {
		return Result{}, err
	}
	redacted := make(map[string]bool)
	for _, company := range companies {
		citations, err := loadCitations(ctx, tx, company.id, req.Columns)
		if err != nil {
			return Result{}, err
		}
		record := []string{company.id, company.name, company.fit, company.score}
		for _, col := range req.Columns {
			value, status, source := "", string(quality.Missing), ""
			if best, ok := quality.BestCitation(citations[col]); ok {
				status, source = string(best.Status), best.SourceSystem+":"+best.SourceRef+"@"+best.RetrievedAt.UTC().Format(time.RFC3339)
				if !Permitted(best.ExportPolicy, tier) {
					value = "[REDACTED]"
					redacted[col] = true
				} else {
					value, err = cellValue(best.Value)
					if err != nil {
						return Result{}, err
					}
				}
			}
			record = append(record, value, status, source)
		}
		for i := range record {
			record[i] = safeCell(record[i])
		}
		if err := writer.Write(record); err != nil {
			return Result{}, err
		}
	}
	writer.Flush()
	if err := writer.Error(); err != nil {
		return Result{}, err
	}
	result := Result{RowCount: len(companies), CSV: buf.Bytes(), RedactedFields: []string{}}
	for field := range redacted {
		result.RedactedFields = append(result.RedactedFields, field)
	}
	sort.Strings(result.RedactedFields)
	return result, nil
}

func loadCitations(ctx context.Context, tx pgx.Tx, company string, columns []string) (map[string][]quality.Citation, error) {
	rows, err := tx.Query(ctx, `SELECT field_name,field_value,origin_type,data_quality_status,export_policy,
 source_system,source_ref,retrieved_at FROM field_citations
 WHERE canonical_company_id=$1 AND field_name=ANY($2::text[]) ORDER BY id`, company, append([]string{}, columns...))
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	result := make(map[string][]quality.Citation)
	for rows.Next() {
		var c quality.Citation
		if err := rows.Scan(&c.FieldName, &c.Value, &c.Origin, &c.Status, &c.ExportPolicy, &c.SourceSystem, &c.SourceRef, &c.RetrievedAt); err != nil {
			return nil, err
		}
		result[c.FieldName] = append(result[c.FieldName], c)
	}
	return result, rows.Err()
}

func cellValue(raw json.RawMessage) (string, error) {
	var s string
	if len(raw) > 0 && raw[0] == '"' {
		if err := json.Unmarshal(raw, &s); err != nil {
			return "", fmt.Errorf("decode string: %w", err)
		}
		return s, nil
	}
	// Preserve exact JSON numbers, including bigint monetary amounts.
	return string(raw), nil
}

func safeCell(s string) string {
	if len(s) > 0 {
		switch s[0] {
		case '=', '+', '-', '@', '\t', '\r':
			return "'" + s
		}
	}
	return s
}

func writeArtifact(ctx context.Context, dir, name string, data []byte) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	if err := os.MkdirAll(dir, 0700); err != nil {
		return fmt.Errorf("create directory: %w", err)
	}
	file, err := os.CreateTemp(dir, ".export-*.tmp")
	if err != nil {
		return fmt.Errorf("create temporary file: %w", err)
	}
	defer os.Remove(file.Name())
	_, writeErr := file.Write(data)
	closeErr := file.Close()
	if writeErr != nil {
		return fmt.Errorf("write temporary file: %w", writeErr)
	}
	if closeErr != nil {
		return fmt.Errorf("close temporary file: %w", closeErr)
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	if err := os.Rename(file.Name(), filepath.Join(dir, name)); err != nil {
		return fmt.Errorf("rename temporary file: %w", err)
	}
	return nil
}
