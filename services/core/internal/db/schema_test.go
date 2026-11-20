package db_test

import (
	"context"
	"errors"
	"testing"

	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgxpool"
	"rowan.ai/rowan/services/core/internal/testdb"
)

func schemaFixture(t *testing.T) (*pgxpool.Pool, string, string, string) {
	t.Helper()
	pool := testdb.New(t)
	ctx := context.Background()
	var workspace, company, record string
	if err := pool.QueryRow(ctx, "INSERT INTO workspaces (name, tier) VALUES ('test', 'solo') RETURNING id").Scan(&workspace); err != nil {
		t.Fatal(err)
	}
	if err := pool.QueryRow(ctx, "INSERT INTO canonical_companies (workspace_id, display_name) VALUES ($1, 'test') RETURNING id", workspace).Scan(&company); err != nil {
		t.Fatal(err)
	}
	if err := pool.QueryRow(ctx, "INSERT INTO source_records (workspace_id, source_system, external_ref, payload, content_hash, fetched_at) VALUES ($1, 'test', '1', '{}', $2, now()) RETURNING id", workspace, []byte{1}).Scan(&record); err != nil {
		t.Fatal(err)
	}
	return pool, workspace, company, record
}

func requireCheckViolation(t *testing.T, err error) {
	t.Helper()
	var pgErr *pgconn.PgError
	if !errors.As(err, &pgErr) || pgErr.Code != "23514" {
		t.Fatalf("error = %v, want check violation (23514)", err)
	}
}

func TestDoNotMergePairOrdered(t *testing.T) {
	pool, workspace, _, record := schemaFixture(t)
	ctx := context.Background()
	var other string
	if err := pool.QueryRow(ctx, "INSERT INTO source_records (workspace_id, source_system, external_ref, payload, content_hash, fetched_at) VALUES ($1, 'test', '2', '{}', '\\x02', now()) RETURNING id", workspace).Scan(&other); err != nil {
		t.Fatal(err)
	}
	_, err := pool.Exec(ctx, "INSERT INTO do_not_merge_pairs (record_a_id, record_b_id, reason, created_by) VALUES (greatest($1::uuid, $2::uuid), least($1::uuid, $2::uuid), 'test', 'test')", record, other)
	requireCheckViolation(t, err)
	if _, err := pool.Exec(ctx, "INSERT INTO do_not_merge_pairs (record_a_id, record_b_id, reason, created_by) VALUES (least($1::uuid, $2::uuid), greatest($1::uuid, $2::uuid), 'test', 'test')", record, other); err != nil {
		t.Fatal(err)
	}
}

func TestQualityStatusChecked(t *testing.T) {
	pool, _, company, record := schemaFixture(t)
	for _, quality := range []string{"guess", "verified_filing", "commercial_estimate", "scraped_web", "missing"} {
		_, err := pool.Exec(context.Background(), "INSERT INTO field_citations (canonical_company_id, source_record_id, field_name, field_value, origin_type, data_quality_status, export_policy, source_system, source_ref, retrieved_at) VALUES ($1, $2, 'revenue', '1', 'raw_source', $3, 'allow', 'test', 'test', now())", company, record, quality)
		if quality == "guess" {
			requireCheckViolation(t, err)
		} else if err != nil {
			t.Fatal(err)
		}
	}
}

func TestReviewScoreBand(t *testing.T) {
	for _, tc := range []struct {
		score string
		valid bool
	}{{"0.549", false}, {"0.55", true}, {"0.849", true}, {"0.85", false}} {
		t.Run(tc.score, func(t *testing.T) {
			pool, workspace, company, record := schemaFixture(t)
			_, err := pool.Exec(context.Background(), "INSERT INTO possible_match_candidates (workspace_id, canonical_company_id, source_record_id, score, match_reasons, status) VALUES ($1, $2, $3, $4, ARRAY['test'], 'pending')", workspace, company, record, tc.score)
			if tc.valid {
				if err != nil {
					t.Fatal(err)
				}
			} else {
				requireCheckViolation(t, err)
			}
		})
	}
}

func TestBucketBalanceNonNegative(t *testing.T) {
	pool, workspace, _, _ := schemaFixture(t)
	ctx := context.Background()
	var bucket int64
	if err := pool.QueryRow(ctx, "INSERT INTO credit_buckets (workspace_id, bucket_type, granted, balance_remaining, granted_at, expires_at) VALUES ($1, 'included', 1, 1, now(), now() + interval '60 days') RETURNING id", workspace).Scan(&bucket); err != nil {
		t.Fatal(err)
	}
	_, err := pool.Exec(ctx, "UPDATE credit_buckets SET balance_remaining = -1 WHERE id = $1", bucket)
	requireCheckViolation(t, err)
	if _, err := pool.Exec(ctx, "UPDATE credit_buckets SET balance_remaining = 0 WHERE id = $1", bucket); err != nil {
		t.Fatal(err)
	}
}
