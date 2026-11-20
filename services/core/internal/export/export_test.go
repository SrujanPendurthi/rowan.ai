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
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
	"rowan.ai/rowan/services/core/internal/ledger"
	"rowan.ai/rowan/services/core/internal/quality"
	"rowan.ai/rowan/services/core/internal/testdb"
)

var exportTime = time.Date(2026, 11, 20, 12, 0, 0, 0, time.UTC)

func TestPermittedMatrix(t *testing.T) {
	for _, tc := range []struct {
		p    quality.ExportPolicy
		want [3]bool
	}{
		{quality.Allow, [3]bool{true, true, true}},
		{quality.DealTeamPlus, [3]bool{false, true, true}},
		{quality.FundOnly, [3]bool{false, false, true}},
		{quality.DisplayOnly, [3]bool{false, false, false}},
	} {
		for i, tier := range []ledger.Tier{ledger.Solo, ledger.DealTeam, ledger.Fund} {
			if got := Permitted(tc.p, tier); got != tc.want[i] {
				t.Errorf("%s/%s = %v", tc.p, tier, got)
			}
		}
	}
	if Permitted("unknown", ledger.Fund) || Permitted(quality.Allow, "unknown") {
		t.Fatal("unknown policy/tier must fail closed")
	}
}

type fixture struct {
	pool            *pgxpool.Pool
	req             Request
	company, source string
}

func seed(t *testing.T, tier ledger.Tier) fixture {
	t.Helper()
	f := fixture{pool: testdb.New(t)}
	ctx := context.Background()
	mustScan := func(query string, args []any, target *string) {
		t.Helper()
		if err := f.pool.QueryRow(ctx, query, args...).Scan(target); err != nil {
			t.Fatal(err)
		}
	}
	mustScan(`INSERT INTO workspaces(name,tier) VALUES ('Export',$1) RETURNING id::text`, []any{tier}, &f.req.WorkspaceID)
	mustScan(`INSERT INTO buy_boxes(workspace_id,name,criteria) VALUES ($1,'Box','{}') RETURNING id::text`, []any{f.req.WorkspaceID}, &f.req.BuyBoxID)
	mustScan(`INSERT INTO canonical_companies(workspace_id,display_name) VALUES ($1,'Acme') RETURNING id::text`, []any{f.req.WorkspaceID}, &f.company)
	mustScan(`INSERT INTO source_records(workspace_id,source_system,external_ref,payload,content_hash,fetched_at)
 VALUES ($1,'vendor','record','{}',decode('01','hex'),$2) RETURNING id::text`, []any{f.req.WorkspaceID, exportTime}, &f.source)
	f.req.ActorID = "test-actor"
	f.req.Columns = []string{"revenue_cents"}
	execSQL(t, f.pool, `INSERT INTO buy_box_evaluations(buy_box_id,canonical_company_id,overall_score,fit,criteria_breakdown,evaluated_at)
 VALUES ($1,$2,0.900,'match','[]',$3)`, f.req.BuyBoxID, f.company, exportTime)
	citation(t, f, "revenue_cents", json.RawMessage(`1234567890123456789`), quality.CommercialEstimate, quality.DealTeamPlus, exportTime)
	return f
}
func execSQL(t *testing.T, pool *pgxpool.Pool, q string, args ...any) {
	t.Helper()
	if _, err := pool.Exec(context.Background(), q, args...); err != nil {
		t.Fatal(err)
	}
}
func citation(t *testing.T, f fixture, field string, value json.RawMessage, status quality.Status, policy quality.ExportPolicy, at time.Time) {
	t.Helper()
	execSQL(t, f.pool, `INSERT INTO field_citations(canonical_company_id,source_record_id,field_name,field_value,origin_type,
 data_quality_status,export_policy,source_system,source_ref,retrieved_at) VALUES ($1,$2,$3,$4,'raw_source',$5,$6,'vendor','ref',$7)`,
		f.company, f.source, field, value, status, policy, at)
}
func runExport(t *testing.T, f fixture) (Result, [][]string, string) {
	t.Helper()
	dir := filepath.Join(t.TempDir(), "artifacts")
	result, err := Export(context.Background(), f.pool, dir, f.req, exportTime)
	if err != nil {
		t.Fatal(err)
	}
	records, err := csv.NewReader(bytes.NewReader(result.CSV)).ReadAll()
	if err != nil {
		t.Fatal(err)
	}
	return result, records, dir
}
func TestExportRedactsForSolo(t *testing.T) {
	f := seed(t, ledger.Solo)
	result, rows, _ := runExport(t, f)
	if rows[1][4] != "[REDACTED]" || rows[1][5] != "commercial_estimate" || rows[1][6] != "vendor:ref@2026-11-20T12:00:00Z" {
		t.Fatalf("row: %v", rows[1])
	}
	if !reflect.DeepEqual(result.RedactedFields, []string{"revenue_cents"}) {
		t.Fatal(result.RedactedFields)
	}
}
func TestExportAllowsForFund(t *testing.T) {
	f := seed(t, ledger.Fund)
	result, rows, _ := runExport(t, f)
	if rows[1][4] != "1234567890123456789" || len(result.RedactedFields) != 0 {
		t.Fatalf("row/result: %v %+v", rows[1], result)
	}
}
func TestExportDisplayOnlyAlwaysRedacted(t *testing.T) {
	f := seed(t, ledger.Fund)
	execSQL(t, f.pool, `UPDATE field_citations SET export_policy='display_only'`)
	_, rows, _ := runExport(t, f)
	if rows[1][4] != "[REDACTED]" {
		t.Fatal(rows[1])
	}
}
func TestExportAudited(t *testing.T) {
	f := seed(t, ledger.Solo)
	result, rows, dir := runExport(t, f)
	wantHeader := []string{"canonical_company_id", "display_name", "fit", "overall_score", "revenue_cents", "revenue_cents__data_quality_status", "revenue_cents__source"}
	if !reflect.DeepEqual(rows[0], wantHeader) {
		t.Fatal(rows[0])
	}
	var workspace, actor, box, ref string
	var count int
	var charged int64
	var columns, redacted []string
	var at time.Time
	err := f.pool.QueryRow(context.Background(), `SELECT workspace_id::text,user_or_key_id,buy_box_id::text,row_count,requested_columns,
 redacted_fields,credits_charged,artifact_ref,created_at FROM export_audits WHERE id=$1`, result.AuditID).
		Scan(&workspace, &actor, &box, &count, &columns, &redacted, &charged, &ref, &at)
	if err != nil {
		t.Fatal(err)
	}
	if workspace != f.req.WorkspaceID || actor != f.req.ActorID || box != f.req.BuyBoxID || count != 1 || result.RowCount != 1 || charged != 0 || !at.Equal(exportTime) || !reflect.DeepEqual(columns, f.req.Columns) || !reflect.DeepEqual(redacted, result.RedactedFields) {
		t.Fatalf("audit mismatch: %s %s %s %d %v %v %d %v", workspace, actor, box, count, columns, redacted, charged, at)
	}
	data, err := os.ReadFile(filepath.Join(dir, ref))
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(data, result.CSV) || ref != result.ArtifactRef || ref != fmt.Sprintf("%x.csv", sha256.Sum256(data)) {
		t.Fatal("artifact mismatch")
	}
}

func TestExportUnknownColumnNoAudit(t *testing.T) {
	// A nil pool proves column validation precedes any query.
	dir := filepath.Join(t.TempDir(), "artifacts")
	_, err := Export(context.Background(), nil, dir, Request{Columns: []string{"secret"}}, exportTime)
	if !errors.Is(err, ErrUnknownColumn) {
		t.Fatal(err)
	}
	if _, err := os.Stat(dir); !os.IsNotExist(err) {
		t.Fatalf("artifact directory created: %v", err)
	}
	f := seed(t, ledger.Solo)
	f.req.Columns = []string{"secret"}
	_, err = Export(context.Background(), f.pool, dir, f.req, exportTime)
	if !errors.Is(err, ErrUnknownColumn) {
		t.Fatal(err)
	}
	assertNoAudit(t, f)
}
func assertNoAudit(t *testing.T, f fixture) {
	t.Helper()
	var count int
	if err := f.pool.QueryRow(context.Background(), `SELECT count(*) FROM export_audits`).Scan(&count); err != nil || count != 0 {
		t.Fatalf("audits=%d err=%v", count, err)
	}
}
func TestExportInvalidRequest(t *testing.T) {
	for _, req := range []Request{
		{WorkspaceID: "w", ActorID: "a"},
		{WorkspaceID: "w", ActorID: "a", BuyBoxID: "b", Columns: []string{"dba", "dba"}},
		{WorkspaceID: "w", ActorID: "a", BuyBoxID: "b", Fit: []string{"unknown"}},
	} {
		_, err := Export(context.Background(), nil, t.TempDir(), req, exportTime)
		if !errors.Is(err, ErrInvalidRequest) {
			t.Fatalf("%+v: %v", req, err)
		}
	}
}
func TestExportWorkspaceIsolation(t *testing.T) {
	f := seed(t, ledger.Solo)
	var other string
	if err := f.pool.QueryRow(context.Background(), `INSERT INTO workspaces(name,tier) VALUES ('Other','fund') RETURNING id::text`).Scan(&other); err != nil {
		t.Fatal(err)
	}
	f.req.WorkspaceID = other
	_, err := Export(context.Background(), f.pool, t.TempDir(), f.req, exportTime)
	if !errors.Is(err, ErrNotFound) {
		t.Fatal(err)
	}
	assertNoAudit(t, f)
}
func TestExportFitFilter(t *testing.T) {
	f := seed(t, ledger.Fund)
	execSQL(t, f.pool, `INSERT INTO canonical_companies(id,workspace_id,display_name) VALUES ('00000000-0000-0000-0000-000000000001',$1,'Fail')`, f.req.WorkspaceID)
	execSQL(t, f.pool, `INSERT INTO buy_box_evaluations VALUES ($1,'00000000-0000-0000-0000-000000000001',0.950,'fail','[]',$2)`, f.req.BuyBoxID, exportTime)
	f.req.Fit = []string{"match"}
	result, rows, _ := runExport(t, f)
	if result.RowCount != 1 || rows[1][0] != f.company {
		t.Fatal(rows)
	}
	f.req.Fit = []string{"partial_unverified"}
	result, rows, _ = runExport(t, f)
	if result.RowCount != 0 || len(rows) != 1 {
		t.Fatal(rows)
	}
}
func TestExportBestCitationMissingAndJSON(t *testing.T) {
	f := seed(t, ledger.Fund)
	f.req.Columns = []string{"dba", "headcount", "legal_name", "phone"}
	citation(t, f, "dba", json.RawMessage(`"weak"`), quality.ScrapedWeb, quality.Allow, exportTime.Add(time.Hour))
	citation(t, f, "dba", json.RawMessage(`"old"`), quality.VerifiedFiling, quality.Allow, exportTime.Add(-time.Hour))
	citation(t, f, "dba", json.RawMessage(`"Best, Inc."`), quality.VerifiedFiling, quality.Allow, exportTime)
	citation(t, f, "headcount", json.RawMessage(`true`), quality.VerifiedFiling, quality.Allow, exportTime)
	citation(t, f, "phone", json.RawMessage(`""`), quality.VerifiedFiling, quality.Allow, exportTime)
	_, rows, _ := runExport(t, f)
	if rows[1][4] != "Best, Inc." || rows[1][7] != "true" || rows[1][10] != "" || rows[1][11] != "missing" || rows[1][12] != "" || rows[1][13] != "" {
		t.Fatal(rows[1])
	}
}
func TestExportFormulaGuard(t *testing.T) {
	f := seed(t, ledger.Fund)
	f.req.Columns = []string{"legal_name", "dba", "domain", "phone", "principal", "address"}
	for i, value := range []string{"=1+1", "+123", "-123", "@SUM(A1)", "\tbad", "\rbad"} {
		raw, _ := json.Marshal(value)
		citation(t, f, f.req.Columns[i], raw, quality.VerifiedFiling, quality.Allow, exportTime)
	}
	execSQL(t, f.pool, `UPDATE canonical_companies SET display_name='=evil'`)
	execSQL(t, f.pool, `UPDATE field_citations SET source_system='@vendor'`)
	_, rows, _ := runExport(t, f)
	if rows[1][1] != "'=evil" {
		t.Fatal(rows[1])
	}
	for i := range f.req.Columns {
		if !strings.HasPrefix(rows[1][4+i*3], "'") || !strings.HasPrefix(rows[1][6+i*3], "'@vendor:") {
			t.Fatal(rows[1])
		}
	}
}
func TestExportFileFailureNoAudit(t *testing.T) {
	f := seed(t, ledger.Solo)
	dir := filepath.Join(t.TempDir(), "file")
	if err := os.WriteFile(dir, []byte("blocking file"), 0600); err != nil {
		t.Fatal(err)
	}
	_, err := Export(context.Background(), f.pool, dir, f.req, exportTime)
	if err == nil {
		t.Fatal("expected write failure")
	}
	assertNoAudit(t, f)
}
func TestExportOrderingAndRedactionSet(t *testing.T) {
	f := seed(t, ledger.Solo)
	f.req.Columns = []string{"revenue_cents", "headcount"}
	citation(t, f, "headcount", json.RawMessage(`10`), quality.CommercialEstimate, quality.FundOnly, exportTime)
	for _, id := range []string{"00000000-0000-0000-0000-000000000002", "00000000-0000-0000-0000-000000000001"} {
		execSQL(t, f.pool, `INSERT INTO canonical_companies(id,workspace_id,display_name) VALUES ($1,$2,'Other')`, id, f.req.WorkspaceID)
		execSQL(t, f.pool, `INSERT INTO buy_box_evaluations VALUES ($1,$2,0.950,'match','[]',$3)`, f.req.BuyBoxID, id, exportTime)
		other := f
		other.company = id
		citation(t, other, "revenue_cents", json.RawMessage(`1`), quality.CommercialEstimate, quality.DealTeamPlus, exportTime)
	}
	result, rows, _ := runExport(t, f)
	if result.RowCount != 3 || !strings.HasSuffix(rows[1][0], "0001") || !strings.HasSuffix(rows[2][0], "0002") || rows[3][0] != f.company {
		t.Fatal(rows)
	}
	if !reflect.DeepEqual(result.RedactedFields, []string{"headcount", "revenue_cents"}) {
		t.Fatal(result.RedactedFields)
	}
}
