package evaluate

import (
	"context"
	"encoding/json"
	"errors"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
	"rowan.ai/rowan/services/core/internal/testdb"
)

func seedID(t *testing.T, tx pgx.Tx, query string, args ...any) string {
	t.Helper()
	var id string
	if err := tx.QueryRow(context.Background(), query, args...).Scan(&id); err != nil {
		t.Fatal(err)
	}
	return id
}
func seedWorkspace(t *testing.T, tx pgx.Tx) string {
	return seedID(t, tx, `INSERT INTO workspaces(name,tier) VALUES ('test','solo') RETURNING id`)
}
func seedBox(t *testing.T, tx pgx.Tx, workspace string) string {
	return seedID(t, tx, `INSERT INTO buy_boxes(workspace_id,name,criteria) VALUES ($1,'test','{"revenue_min_cents":20,"states_in":["TX"]}') RETURNING id`, workspace)
}
func TestEvaluateCompanyPersists(t *testing.T) {
	pool := testdb.New(t)
	ctx := context.Background()
	tx, err := pool.Begin(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer tx.Rollback(ctx)
	workspace := seedWorkspace(t, tx)
	box := seedBox(t, tx, workspace)
	canonical := seedID(t, tx, `INSERT INTO canonical_companies(workspace_id,display_name) VALUES ($1,'test') RETURNING id`, workspace)
	source := seedID(t, tx, `INSERT INTO source_records(workspace_id,source_system,external_ref,payload,content_hash,fetched_at) VALUES ($1,'filing','ref','{}','hash',now()) RETURNING id`, workspace)
	for _, field := range []struct{ name, value, status string }{{"revenue_cents", "30", "verified_filing"}, {"state", `"TX"`, "scraped_web"}} {
		_, err = tx.Exec(ctx, `INSERT INTO field_citations(canonical_company_id,source_record_id,field_name,field_value,origin_type,data_quality_status,export_policy,source_system,source_ref,table_name,row_key,page_or_span,retrieved_at) VALUES ($1,$2,$3,$4,'raw_source',$5,'display_only','filing','ref','table','row','page',now())`, canonical, source, field.name, field.value, field.status)
		if err != nil {
			t.Fatal(err)
		}
	}
	now := time.Date(2026, 11, 20, 12, 0, 0, 0, time.UTC)
	for i := 0; i < 2; i++ {
		if _, err := EvaluateCompany(ctx, tx, box, canonical, now.Add(time.Duration(i)*time.Hour)); err != nil {
			t.Fatal(err)
		}
	}
	var raw []byte
	var score float64
	var fit string
	var evaluated time.Time
	if err := tx.QueryRow(ctx, `SELECT criteria_breakdown,overall_score,fit,evaluated_at FROM buy_box_evaluations WHERE buy_box_id=$1 AND canonical_company_id=$2`, box, canonical).Scan(&raw, &score, &fit, &evaluated); err != nil {
		t.Fatal(err)
	}
	var results []CriterionResult
	if err := json.Unmarshal(raw, &results); err != nil {
		t.Fatal(err)
	}
	if len(results) != 2 || score != .5 || fit != FitPartialUnverified || !evaluated.Equal(now.Add(time.Hour)) {
		t.Fatalf("bad persisted result: %s %v %s %v", raw, score, fit, evaluated)
	}
	for _, r := range results {
		if r.DataQualityStatus == "" || r.Citation == nil || r.Citation.SourceRecordID != source || r.Citation.TableName != "table" || r.Citation.RowKey != "row" || r.Citation.PageOrSpan != "page" || r.Citation.ExportPolicy != "display_only" {
			t.Fatalf("lost provenance: %+v", r)
		}
	}
	otherWorkspace := seedWorkspace(t, tx)
	otherBox := seedBox(t, tx, otherWorkspace)
	if _, err := EvaluateCompany(ctx, tx, otherBox, canonical, now); !errors.Is(err, ErrNotFound) {
		t.Fatalf("cross-workspace error: %v", err)
	}
	missing := "00000000-0000-0000-0000-000000000001"
	if _, err := EvaluateCompany(ctx, tx, box, missing, now); !errors.Is(err, ErrNotFound) {
		t.Fatalf("missing company: %v", err)
	}
	if _, err := EvaluateCompany(ctx, tx, missing, canonical, now); !errors.Is(err, ErrNotFound) {
		t.Fatalf("missing box: %v", err)
	}
	secondBox := seedBox(t, tx, workspace)
	if err := ReevaluateCompany(ctx, tx, canonical, now); err != nil {
		t.Fatal(err)
	}
	var count int
	if err := tx.QueryRow(ctx, `SELECT count(*) FROM buy_box_evaluations WHERE canonical_company_id=$1`, canonical).Scan(&count); err != nil {
		t.Fatal(err)
	}
	if count != 2 || secondBox == box {
		t.Fatalf("expected two evaluations, got %d", count)
	}
	emptyCompany := seedID(t, tx, `INSERT INTO canonical_companies(workspace_id,display_name) VALUES ($1,'empty') RETURNING id`, otherWorkspace)
	if _, err := tx.Exec(ctx, `DELETE FROM buy_boxes WHERE id=$1`, otherBox); err != nil {
		t.Fatal(err)
	}
	if err := ReevaluateCompany(ctx, tx, emptyCompany, now); err != nil {
		t.Fatal(err)
	}
	if err := ReevaluateCompany(ctx, tx, missing, now); !errors.Is(err, ErrNotFound) {
		t.Fatalf("missing reevaluation company: %v", err)
	}
}
