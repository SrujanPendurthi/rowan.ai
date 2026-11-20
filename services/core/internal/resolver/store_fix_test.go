package resolver

import (
	"context"
	"errors"
	"reflect"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
	"rowan.ai/rowan/services/core/internal/db"
	"rowan.ai/rowan/services/core/internal/quality"
	"rowan.ai/rowan/services/core/internal/testdb"
)

func TestResolveNameZipContactWithoutMatchingState(t *testing.T) {
	for _, states := range [][2]string{{"", ""}, {"NY", "TX"}} {
		t.Run("states="+states[0]+","+states[1], func(t *testing.T) {
			pool := testdb.New(t)
			w := seedWorkspace(t, pool)
			facts := RecordFacts{LegalName: "Acme LLC", Zip: "10001", Phone: "212-555-0100", State: states[0]}
			first := ingest(t, pool, recordInput(w, "first", facts))
			facts.LegalName, facts.State, facts.Phone = "ACME Inc.", states[1], "+1 (212) 555-0100"
			second := ingest(t, pool, recordInput(w, "second", facts))
			if second.CanonicalID != first.CanonicalID || second.Decision != AutoMerge || second.Score != .92 {
				t.Fatalf("name/zip/contact did not merge: %+v / %+v", first, second)
			}
		})
	}
}

func TestBlockKeysNameZip(t *testing.T) {
	for _, tc := range []struct {
		name  string
		facts RecordFacts
		want  []string
	}{
		{"legal_and_dba", RecordFacts{LegalName: " Acme LLC ", DBA: " Trade Inc.", Zip: " 10001 "}, []string{"namezip:acme|10001", "namezip:trade|10001"}},
		{"deduplicate", RecordFacts{LegalName: "Acme LLC", DBA: "ACME Inc.", Zip: "10001"}, []string{"namezip:acme|10001"}},
		{"missing_names", RecordFacts{Zip: "10001"}, []string{}},
		{"missing_zip", RecordFacts{LegalName: "Acme", DBA: "Trade"}, []string{}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			if got := blockKeys(tc.facts); !reflect.DeepEqual(got, tc.want) {
				t.Fatalf("block keys: got %v, want %v", got, tc.want)
			}
		})
	}
}

func TestResolveReviewRetainsCanonicalWithSplitLink(t *testing.T) {
	pool := testdb.New(t)
	w := seedWorkspace(t, pool)
	_, second, _ := seedReview(t, pool, w)
	in := recordInput(w, "split", RecordFacts{})
	in.Citations = []quality.Citation{stateCitation(quality.VerifiedFiling)}
	third := ingest(t, pool, in)
	execSQL(t, pool, `UPDATE entity_merge_links SET canonical_company_id=$1,status='split' WHERE source_record_id=$2`, second.CanonicalID, third.SourceRecordID)
	execSQL(t, pool, `UPDATE field_citations SET canonical_company_id=$1 WHERE source_record_id=$2`, second.CanonicalID, third.SourceRecordID)
	if err := review(t, pool, second.ReviewCandidateID, "merge", resolverNow); err != nil {
		t.Fatal(err)
	}
	if countSQL(t, pool, `SELECT count(*) FROM canonical_companies WHERE id=$1`, second.CanonicalID) != 1 ||
		countSQL(t, pool, `SELECT count(*) FROM entity_merge_links WHERE source_record_id=$1 AND canonical_company_id=$2 AND status='split'`, third.SourceRecordID, second.CanonicalID) != 1 ||
		countSQL(t, pool, `SELECT count(*) FROM field_citations WHERE source_record_id=$1 AND canonical_company_id=$2`, third.SourceRecordID, second.CanonicalID) != 1 {
		t.Fatal("review merge deleted canonical, split history, or citations")
	}
}

func TestResolveReplayUsesMergeMode(t *testing.T) {
	pool := testdb.New(t)
	w := seedWorkspace(t, pool)
	first, second, in := seedReview(t, pool, w)
	replayed := ingest(t, pool, in)
	if replayed.Decision != Review || replayed.Score != 1 || replayed.ReviewCandidateID != second.ReviewCandidateID {
		t.Fatalf("pending seed replay: %+v", replayed)
	}
	if err := review(t, pool, second.ReviewCandidateID, "merge", resolverNow); err != nil {
		t.Fatal(err)
	}
	replayed = ingest(t, pool, in)
	if replayed.Decision != AutoMerge || replayed.Score != .65 || replayed.CanonicalID != first.CanonicalID {
		t.Fatalf("manual replay: %+v", replayed)
	}
	auto := recordInput(w, "auto", RecordFacts{LegalName: "Acme", State: "TX", Principal: "Lee"})
	auto.Facts.IDs = map[string]string{"uei": "same"}
	third := ingest(t, pool, auto)
	auto.ExternalRef = "auto-second"
	fourth := ingest(t, pool, auto)
	replayed = ingest(t, pool, auto)
	if replayed.Decision != AutoMerge || replayed.Score != .97 || replayed.CanonicalID != third.CanonicalID || replayed.SourceRecordID != fourth.SourceRecordID {
		t.Fatalf("auto replay: %+v", replayed)
	}
}

func resolverSnapshot(t *testing.T, tx pgx.Tx) string {
	t.Helper()
	var snapshot string
	err := tx.QueryRow(context.Background(), `SELECT jsonb_build_array(
		(SELECT jsonb_agg(to_jsonb(s) ORDER BY id) FROM source_records s),
		(SELECT jsonb_agg(to_jsonb(c) ORDER BY id) FROM canonical_companies c),
		(SELECT jsonb_agg(to_jsonb(l) ORDER BY id) FROM entity_merge_links l),
		(SELECT jsonb_agg(to_jsonb(f) ORDER BY id) FROM field_citations f),
		(SELECT jsonb_agg(to_jsonb(p) ORDER BY id) FROM possible_match_candidates p),
		(SELECT jsonb_agg(to_jsonb(e) ORDER BY buy_box_id,canonical_company_id) FROM buy_box_evaluations e),
		(SELECT jsonb_agg(to_jsonb(p) ORDER BY record_a_id,record_b_id) FROM do_not_merge_pairs p)
	)::text`).Scan(&snapshot)
	if err != nil {
		t.Fatal(err)
	}
	return snapshot
}

func TestResolveInvalidCitationsNeverWrite(t *testing.T) {
	for _, path := range []string{"new", "changed", "replay"} {
		t.Run(path, func(t *testing.T) {
			pool := testdb.New(t)
			in := recordInput(seedWorkspace(t, pool), "record", RecordFacts{LegalName: "Acme"})
			in.Citations = []quality.Citation{stateCitation(quality.VerifiedFiling)}
			if path != "new" {
				ingest(t, pool, in)
			}
			if path == "changed" {
				in.Facts.State = "NY"
				in.FetchedAt = resolverNow.Add(time.Hour)
			}
			invalid := stateCitation(quality.ScrapedWeb)
			invalid.Status = "bogus"
			in.Citations = append(in.Citations, invalid)
			err := db.InTx(context.Background(), pool, func(tx pgx.Tx) error {
				before := resolverSnapshot(t, tx)
				if _, err := Resolve(context.Background(), tx, in, resolverNow); err == nil {
					t.Error("invalid citation accepted")
				}
				if after := resolverSnapshot(t, tx); after != before {
					t.Error("invalid citation changed transaction state before caller rollback")
				}
				return nil // Deliberately commit to verify validation needs no caller rollback.
			})
			if err != nil {
				t.Fatal(err)
			}
		})
	}
}

func TestLockWorkspaceThenResolve(t *testing.T) {
	pool := testdb.New(t)
	w := seedWorkspace(t, pool)
	_, second, in := seedReview(t, pool, w)
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	err := db.InTx(ctx, pool, func(tx pgx.Tx) error {
		for i := 0; i < 2; i++ {
			if err := LockWorkspace(ctx, tx, w); err != nil {
				return err
			}
		}
		if _, err := Resolve(ctx, tx, in, resolverNow); err != nil {
			return err
		}
		return ResolveReview(ctx, tx, second.ReviewCandidateID, "merge", "reviewer", resolverNow)
	})
	if err != nil {
		t.Fatal(err)
	}
}

func TestResolveReviewRechecksDoNotMerge(t *testing.T) {
	for _, status := range []string{"active", "split"} {
		t.Run(status, func(t *testing.T) {
			pool := testdb.New(t)
			w := seedWorkspace(t, pool)
			first, second, _ := seedReview(t, pool, w)
			third := ingest(t, pool, recordInput(w, "blocker", RecordFacts{}))
			execSQL(t, pool, `UPDATE entity_merge_links SET canonical_company_id=$1,status=$3 WHERE source_record_id=$2`, first.CanonicalID, third.SourceRecordID, status)
			a, b := OrderedPair(second.SourceRecordID, third.SourceRecordID)
			execSQL(t, pool, `INSERT INTO do_not_merge_pairs(record_a_id,record_b_id,reason,created_by) VALUES ($1,$2,'split','actor')`, a, b)
			err := db.InTx(context.Background(), pool, func(tx pgx.Tx) error {
				before := resolverSnapshot(t, tx)
				err := ResolveReview(context.Background(), tx, second.ReviewCandidateID, "merge", "reviewer", resolverNow.Add(time.Hour))
				if status == "active" {
					if !errors.Is(err, ErrDoNotMerge) {
						t.Errorf("blocked merge returned %v", err)
					}
					if resolverSnapshot(t, tx) != before {
						t.Error("blocked review merge wrote transaction state")
					}
					return nil // Deliberately commit: forbidden merge must perform no writes.
				}
				return err
			})
			if err != nil {
				t.Fatal(err)
			}
		})
	}
}
