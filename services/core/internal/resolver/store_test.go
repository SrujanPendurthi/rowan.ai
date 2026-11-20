package resolver

import (
	"context"
	"crypto/sha256"
	"encoding/json"
	"errors"
	"reflect"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
	"rowan.ai/rowan/services/core/internal/db"
	"rowan.ai/rowan/services/core/internal/evaluate"
	"rowan.ai/rowan/services/core/internal/quality"
	"rowan.ai/rowan/services/core/internal/testdb"
)

func TestResolveAutoMergeOnHardID(t *testing.T) {
	pool := testdb.New(t)
	w := seedWorkspace(t, pool)
	first := ingest(t, pool, recordInput(w, "first", RecordFacts{IDs: map[string]string{"UEI": " AbC "}}))
	second := ingest(t, pool, recordInput(w, "second", RecordFacts{IDs: map[string]string{"uei": "abc"}}))
	if first.CanonicalID != second.CanonicalID || second.Decision != AutoMerge || second.Score != .970 || !reflect.DeepEqual(second.Reasons, []string{"hard_id:uei"}) {
		t.Fatalf("unexpected hard-ID merge: %+v / %+v", first, second)
	}
	var mode string
	var score float64
	var reasons []string
	if err := pool.QueryRow(context.Background(), `SELECT merge_mode,merge_confidence_score,match_reasons FROM entity_merge_links WHERE source_record_id=$1`, second.SourceRecordID).Scan(&mode, &score, &reasons); err != nil {
		t.Fatal(err)
	}
	if mode != "auto" || score != .97 || !reflect.DeepEqual(reasons, second.Reasons) {
		t.Fatalf("incorrect persisted link: %s %v %v", mode, score, reasons)
	}
}

func TestResolveReviewBand(t *testing.T) {
	pool := testdb.New(t)
	w := seedWorkspace(t, pool)
	first, second, _ := seedReview(t, pool, w)
	if first.CanonicalID == second.CanonicalID || second.Decision != Review || second.Score != .650 || second.ReviewCandidateID == "" {
		t.Fatalf("unexpected review result: %+v", second)
	}
	if n := countSQL(t, pool, `SELECT count(*) FROM canonical_companies WHERE id=$1 AND pending_review`, second.CanonicalID); n != 1 {
		t.Fatal("review company not flagged")
	}
	if n := countSQL(t, pool, `SELECT count(*) FROM possible_match_candidates WHERE id=$1 AND canonical_company_id=$2 AND score=.650 AND status='pending' AND match_reasons=ARRAY['dba_legal_state']`, second.ReviewCandidateID, first.CanonicalID); n != 1 {
		t.Fatal("missing review candidate")
	}
	if n := countSQL(t, pool, `SELECT count(*) FROM entity_merge_links WHERE source_record_id=$1 AND merge_mode='seed' AND merge_confidence_score=1 AND match_reasons=ARRAY['seed']`, second.SourceRecordID); n != 1 {
		t.Fatal("incorrect seed link")
	}
}

func TestResolveDistinct(t *testing.T) {
	pool := testdb.New(t)
	w := seedWorkspace(t, pool)
	first := ingest(t, pool, recordInput(w, "first", RecordFacts{LegalName: "Acme"}))
	second := ingest(t, pool, recordInput(w, "second", RecordFacts{LegalName: "Unrelated"}))
	if first.CanonicalID == second.CanonicalID || second.Decision != Distinct || second.ReviewCandidateID != "" {
		t.Fatalf("unexpected distinct result: %+v", second)
	}
	if n := countSQL(t, pool, `SELECT count(*) FROM possible_match_candidates`); n != 0 {
		t.Fatalf("unexpected candidates: %d", n)
	}
}

func TestResolveSameContentNoop(t *testing.T) {
	pool := testdb.New(t)
	w := seedWorkspace(t, pool)
	in := recordInput(w, "first", RecordFacts{LegalName: "Acme", State: "TX"})
	in.Citations = []quality.Citation{stateCitation(quality.VerifiedFiling)}
	first := ingest(t, pool, in)
	var citationID string
	if err := pool.QueryRow(context.Background(), `SELECT id::text FROM field_citations`).Scan(&citationID); err != nil {
		t.Fatal(err)
	}
	in.FetchedAt = resolverNow.Add(time.Hour)
	second := ingest(t, pool, in)
	if first.CanonicalID != second.CanonicalID || first.SourceRecordID != second.SourceRecordID || second.Score != 1 || second.Decision != Distinct {
		t.Fatalf("incorrect replay: %+v", second)
	}
	if n := countSQL(t, pool, `SELECT count(*) FROM source_records WHERE fetched_at=$1`, resolverNow); n != 1 {
		t.Fatal("no-op changed fetched_at or inserted a record")
	}
	if n := countSQL(t, pool, `SELECT count(*) FROM field_citations WHERE id=$1`, citationID); n != 1 {
		t.Fatal("no-op replaced citation")
	}
}

func TestResolveReviewMerge(t *testing.T) {
	pool := testdb.New(t)
	w := seedWorkspace(t, pool)
	box := seedID(t, pool, `INSERT INTO buy_boxes(workspace_id,name,criteria) VALUES ($1,'states','{"states_in":["TX"]}') RETURNING id::text`, w)
	first, second, _ := seedReview(t, pool, w)
	linkID := seedID(t, pool, `SELECT id::text FROM entity_merge_links WHERE source_record_id=$1`, second.SourceRecordID)
	now := resolverNow.Add(time.Hour)
	if err := review(t, pool, second.ReviewCandidateID, "merge", now); err != nil {
		t.Fatal(err)
	}
	if n := countSQL(t, pool, `SELECT count(*) FROM entity_merge_links WHERE id=$1 AND canonical_company_id=$2 AND merge_mode='manual' AND merge_confidence_score=.650 AND match_reasons=ARRAY['dba_legal_state']`, linkID, first.CanonicalID); n != 1 {
		t.Fatal("manual merge did not update existing link")
	}
	if n := countSQL(t, pool, `SELECT count(*) FROM canonical_companies WHERE id=$1`, second.CanonicalID); n != 0 {
		t.Fatal("empty canonical retained")
	}
	if n := countSQL(t, pool, `SELECT count(*) FROM field_citations WHERE source_record_id=$1 AND canonical_company_id=$2`, second.SourceRecordID, first.CanonicalID); n != 1 {
		t.Fatal("citations not moved")
	}
	var raw []byte
	if err := pool.QueryRow(context.Background(), `SELECT criteria_breakdown FROM buy_box_evaluations WHERE buy_box_id=$1 AND canonical_company_id=$2 AND evaluated_at=$3`, box, first.CanonicalID, now).Scan(&raw); err != nil {
		t.Fatal(err)
	}
	var criteria []evaluate.CriterionResult
	if err := json.Unmarshal(raw, &criteria); err != nil {
		t.Fatal(err)
	}
	if len(criteria) != 1 || criteria[0].Status != evaluate.StatusUnverified || criteria[0].DataQualityStatus != quality.ScrapedWeb || criteria[0].Citation == nil || criteria[0].Citation.SourceRecordID != second.SourceRecordID {
		t.Fatalf("quality lost after merge: %s", raw)
	}
	if n := countSQL(t, pool, `SELECT count(*) FROM buy_box_evaluations WHERE canonical_company_id=$1`, second.CanonicalID); n != 0 {
		t.Fatal("empty company's evaluation retained")
	}
	if err := review(t, pool, second.ReviewCandidateID, "merge", now.Add(time.Hour)); err != nil {
		t.Fatal(err)
	}
	if n := countSQL(t, pool, `SELECT count(*) FROM possible_match_candidates WHERE id=$1 AND status='merged' AND resolved_by='reviewer' AND resolved_at=$2`, second.ReviewCandidateID, now); n != 1 {
		t.Fatal("merge replay wrote resolution metadata")
	}
	if err := review(t, pool, second.ReviewCandidateID, "reject", now); !errors.Is(err, ErrAlreadyResolved) {
		t.Fatalf("conflicting replay: %v", err)
	}
}

func TestResolveReviewReject(t *testing.T) {
	pool := testdb.New(t)
	w := seedWorkspace(t, pool)
	first, second, in := seedReview(t, pool, w)
	// Rejection must pair against every active target record, but never split links.
	third := ingest(t, pool, recordInput(w, "third", RecordFacts{LegalName: "Acme", State: "TX", Principal: "Lee"}))
	execSQL(t, pool, `UPDATE entity_merge_links SET canonical_company_id=$1 WHERE source_record_id=$2`, first.CanonicalID, third.SourceRecordID)
	if err := review(t, pool, second.ReviewCandidateID, "reject", resolverNow); err != nil {
		t.Fatal(err)
	}
	for _, target := range []string{first.SourceRecordID, third.SourceRecordID} {
		a, b := OrderedPair(second.SourceRecordID, target)
		if n := countSQL(t, pool, `SELECT count(*) FROM do_not_merge_pairs WHERE record_a_id=$1 AND record_b_id=$2 AND reason='review_rejected' AND created_by='reviewer'`, a, b); n != 1 {
			t.Fatal("ordered rejection pair missing")
		}
	}
	if n := countSQL(t, pool, `SELECT count(*) FROM canonical_companies WHERE id=$1 AND NOT pending_review`, second.CanonicalID); n != 1 {
		t.Fatal("pending_review not cleared")
	}
	if err := review(t, pool, second.ReviewCandidateID, "reject", resolverNow.Add(time.Hour)); err != nil {
		t.Fatal(err)
	}
	if n := countSQL(t, pool, `SELECT count(*) FROM possible_match_candidates WHERE id=$1 AND status='rejected' AND resolved_at=$2`, second.ReviewCandidateID, resolverNow); n != 1 {
		t.Fatal("reject replay changed resolution")
	}
	if err := review(t, pool, second.ReviewCandidateID, "merge", resolverNow); !errors.Is(err, ErrAlreadyResolved) {
		t.Fatalf("conflicting action: %v", err)
	}
	in.Facts.IDs = map[string]string{"uei": "new"}
	in.Payload = json.RawMessage(`{"changed":true}`)
	updated := ingest(t, pool, in)
	if updated.CanonicalID != second.CanonicalID || countSQL(t, pool, `SELECT count(*) FROM possible_match_candidates WHERE source_record_id=$1 AND status='pending'`, second.SourceRecordID) != 0 {
		t.Fatalf("rejected record re-resolved: %+v", updated)
	}
}

func TestResolveRespectsDoNotMerge(t *testing.T) {
	pool := testdb.New(t)
	w := seedWorkspace(t, pool)
	facts := RecordFacts{IDs: map[string]string{"uei": "same"}}
	first := ingest(t, pool, recordInput(w, "first", facts))
	blocker := ingest(t, pool, recordInput(w, "blocker", RecordFacts{}))
	execSQL(t, pool, `UPDATE entity_merge_links SET canonical_company_id=$1 WHERE source_record_id=$2`, first.CanonicalID, blocker.SourceRecordID)
	newID := seedID(t, pool, `INSERT INTO source_records(workspace_id,source_system,external_ref,payload,content_hash,fetched_at) VALUES ($1,'filing','new','{}','seed',$2) RETURNING id::text`, w, resolverNow)
	a, b := OrderedPair(blocker.SourceRecordID, newID)
	execSQL(t, pool, `INSERT INTO do_not_merge_pairs(record_a_id,record_b_id,reason,created_by) VALUES ($1,$2,'split','actor')`, a, b)
	second := ingest(t, pool, recordInput(w, "new", facts))
	if second.SourceRecordID != newID || first.CanonicalID == second.CanonicalID || second.Decision != Distinct {
		t.Fatalf("do-not-merge ignored: %+v", second)
	}
}

func TestResolveCitationsKeepQuality(t *testing.T) {
	pool := testdb.New(t)
	w := seedWorkspace(t, pool)
	for i, status := range []quality.Status{quality.VerifiedFiling, quality.CommercialEstimate, quality.ScrapedWeb, quality.Missing} {
		in := recordInput(w, string(rune('a'+i)), RecordFacts{})
		citation := stateCitation(status)
		in.Citations = []quality.Citation{citation}
		result := ingest(t, pool, in)
		var got quality.Citation
		if err := pool.QueryRow(context.Background(), `SELECT field_name,field_value,origin_type,data_quality_status,export_policy,source_system,source_ref,table_name,row_key,page_or_span,retrieved_at,source_record_id::text FROM field_citations WHERE canonical_company_id=$1`, result.CanonicalID).Scan(&got.FieldName, &got.Value, &got.Origin, &got.Status, &got.ExportPolicy, &got.SourceSystem, &got.SourceRef, &got.TableName, &got.RowKey, &got.PageOrSpan, &got.RetrievedAt, &got.SourceRecordID); err != nil {
			t.Fatal(err)
		}
		citation.SourceRecordID = result.SourceRecordID
		if !got.RetrievedAt.Equal(citation.RetrievedAt) {
			t.Fatalf("retrieval time changed: %v", got.RetrievedAt)
		}
		got.RetrievedAt = got.RetrievedAt.UTC()
		if !reflect.DeepEqual(got, citation) {
			t.Fatalf("lost provenance: got %+v want %+v", got, citation)
		}
	}
}

func TestResolveChangedContentKeepsLink(t *testing.T) {
	pool := testdb.New(t)
	w := seedWorkspace(t, pool)
	seedID(t, pool, `INSERT INTO buy_boxes(workspace_id,name,criteria) VALUES ($1,'states','{"states_in":["TX"]}') RETURNING id::text`, w)
	in := recordInput(w, "first", RecordFacts{LegalName: "Original", IDs: map[string]string{"uei": "one"}})
	in.Citations = []quality.Citation{stateCitation(quality.ScrapedWeb)}
	first := ingest(t, pool, in)
	other := ingest(t, pool, recordInput(w, "other", RecordFacts{IDs: map[string]string{"uei": "two"}}))
	in.Facts = RecordFacts{LegalName: "Changed", IDs: map[string]string{"uei": "two"}}
	in.Payload = json.RawMessage(`{"raw":"changed"}`)
	in.FetchedAt = resolverNow.Add(time.Hour)
	in.Citations = []quality.Citation{stateCitation(quality.VerifiedFiling)}
	updated := ingest(t, pool, in)
	if updated.CanonicalID != first.CanonicalID || updated.CanonicalID == other.CanonicalID || updated.SourceRecordID != first.SourceRecordID {
		t.Fatalf("changed content re-resolved: %+v", updated)
	}
	if n := countSQL(t, pool, `SELECT count(*) FROM field_citations WHERE source_record_id=$1 AND data_quality_status='verified_filing'`, first.SourceRecordID); n != 1 {
		t.Fatal("citations not replaced")
	}
	if n := countSQL(t, pool, `SELECT count(*) FROM field_citations WHERE source_record_id=$1`, first.SourceRecordID); n != 1 {
		t.Fatal("old citations retained")
	}
	if n := countSQL(t, pool, `SELECT count(*) FROM buy_box_evaluations WHERE canonical_company_id=$1 AND fit='match'`, first.CanonicalID); n != 1 {
		t.Fatal("changed content not reevaluated")
	}
	if n := countSQL(t, pool, `SELECT count(*) FROM source_records WHERE id=$1 AND fetched_at=$2 AND payload->'raw'=$3::jsonb AND payload->'facts'->>'LegalName'='Changed'`, first.SourceRecordID, in.FetchedAt, in.Payload); n != 1 {
		t.Fatal("payload or fetched_at not updated")
	}
}

func TestResolveBlocking(t *testing.T) {
	for _, tc := range []struct {
		name  string
		a, b  RecordFacts
		score float64
	}{
		{"address", RecordFacts{Address1: " 10 Main St suite 2", Zip: "01010"}, RecordFacts{Address1: "10 main st unit 3", Zip: "01010"}, .60},
		{"agent", RecordFacts{RegisteredAgent: "Agent Inc.", State: "TX"}, RecordFacts{RegisteredAgent: "AGENT LLC", State: "tx"}, .58},
		{"dba_to_legal", RecordFacts{DBA: "Trade Co", State: "TX"}, RecordFacts{LegalName: "trade", State: "TX"}, .65},
		{"pobox", RecordFacts{POBox: true, Address1: "PO Box 1", State: "TX"}, RecordFacts{POBox: true, Address1: "po box 1", State: "TX"}, 0},
	} {
		t.Run(tc.name, func(t *testing.T) {
			pool := testdb.New(t)
			w := seedWorkspace(t, pool)
			ingest(t, pool, recordInput(w, "first", tc.a))
			got := ingest(t, pool, recordInput(w, "second", tc.b))
			if got.Score != tc.score || got.Decision != Classify(tc.score) {
				t.Fatalf("blocking mismatch: %+v", got)
			}
		})
	}
}

func TestResolvePayloadAndDisplayName(t *testing.T) {
	pool := testdb.New(t)
	w := seedWorkspace(t, pool)
	in := recordInput(w, "fallback", RecordFacts{DBA: "Acme LLC", State: "TX", IDs: map[string]string{"UEI": " A "}})
	result := ingest(t, pool, in)
	expected, err := json.Marshal(struct {
		Facts     RecordFacts     `json:"facts"`
		Raw       json.RawMessage `json:"raw"`
		BlockKeys []string        `json:"block_keys"`
	}{in.Facts, nil, []string{"id:uei=a", "name:acme|tx"}})
	if err != nil {
		t.Fatal(err)
	}
	hash := sha256.Sum256(expected)
	if n := countSQL(t, pool, `SELECT count(*) FROM source_records WHERE id=$1 AND payload=$2::jsonb AND content_hash=$3`, result.SourceRecordID, expected, hash[:]); n != 1 {
		t.Fatal("payload or content hash mismatch")
	}
	if n := countSQL(t, pool, `SELECT count(*) FROM canonical_companies WHERE id=$1 AND display_name='Acme LLC'`, result.CanonicalID); n != 1 {
		t.Fatal("DBA display fallback missing")
	}
	fallback := ingest(t, pool, recordInput(w, "ref-only", RecordFacts{}))
	if n := countSQL(t, pool, `SELECT count(*) FROM canonical_companies WHERE id=$1 AND display_name='ref-only'`, fallback.CanonicalID); n != 1 {
		t.Fatal("external ref display fallback missing")
	}
}

func TestResolveInvalidCitationRollsBack(t *testing.T) {
	pool := testdb.New(t)
	in := recordInput(seedWorkspace(t, pool), "invalid", RecordFacts{})
	in.Citations = []quality.Citation{stateCitation(quality.VerifiedFiling)}
	in.Citations[0].Status = "bogus"
	err := db.InTx(context.Background(), pool, func(tx pgx.Tx) error {
		_, err := Resolve(context.Background(), tx, in, resolverNow)
		return err
	})
	if err == nil || countSQL(t, pool, `SELECT count(*) FROM source_records`) != 0 || countSQL(t, pool, `SELECT count(*) FROM canonical_companies`) != 0 {
		t.Fatalf("invalid citation not rolled back: %v", err)
	}
}

func TestResolveReviewInvalidRequest(t *testing.T) {
	pool := testdb.New(t)
	_, second, _ := seedReview(t, pool, seedWorkspace(t, pool))
	for _, tc := range []struct{ id, action string }{
		{second.ReviewCandidateID, "invalid"}, {"00000000-0000-0000-0000-000000000001", "merge"},
		{"not-a-uuid", "merge"}, {"", "merge"},
	} {
		if err := review(t, pool, tc.id, tc.action, resolverNow); !errors.Is(err, ErrInvalidRequest) {
			t.Fatalf("expected invalid request: %v", err)
		}
	}
}

func TestResolveReviewRetargetsPendingCandidates(t *testing.T) {
	pool := testdb.New(t)
	w := seedWorkspace(t, pool)
	first, second, _ := seedReview(t, pool, w)
	third := ingest(t, pool, recordInput(w, "third", RecordFacts{}))
	fourth := ingest(t, pool, recordInput(w, "fourth", RecordFacts{}))
	retargeted := seedID(t, pool, `INSERT INTO possible_match_candidates(workspace_id,canonical_company_id,source_record_id,score,match_reasons,status) VALUES ($1,$2,$3,.650,ARRAY['dba_legal_state'],'pending') RETURNING id::text`, w, second.CanonicalID, third.SourceRecordID)
	conflicting := seedID(t, pool, `INSERT INTO possible_match_candidates(workspace_id,canonical_company_id,source_record_id,score,match_reasons,status) VALUES ($1,$2,$3,.650,ARRAY['dba_legal_state'],'pending') RETURNING id::text`, w, second.CanonicalID, fourth.SourceRecordID)
	existing := seedID(t, pool, `INSERT INTO possible_match_candidates(workspace_id,canonical_company_id,source_record_id,score,match_reasons,status) VALUES ($1,$2,$3,.650,ARRAY['dba_legal_state'],'pending') RETURNING id::text`, w, first.CanonicalID, fourth.SourceRecordID)
	if err := review(t, pool, second.ReviewCandidateID, "merge", resolverNow); err != nil {
		t.Fatal(err)
	}
	if n := countSQL(t, pool, `SELECT count(*) FROM possible_match_candidates WHERE id=$1 AND canonical_company_id=$2 AND status='pending'`, retargeted, first.CanonicalID); n != 1 {
		t.Fatal("pending candidate lost on canonical deletion")
	}
	if countSQL(t, pool, `SELECT count(*) FROM possible_match_candidates WHERE id=$1`, conflicting) != 0 || countSQL(t, pool, `SELECT count(*) FROM possible_match_candidates WHERE id=$1`, existing) != 1 {
		t.Fatal("retarget conflict did not preserve existing target candidate")
	}
}

func TestResolveReviewPreservesOtherPendingCandidates(t *testing.T) {
	pool := testdb.New(t)
	w := seedWorkspace(t, pool)
	_, second, _ := seedReview(t, pool, w)
	third := ingest(t, pool, recordInput(w, "third", RecordFacts{}))
	seedID(t, pool, `INSERT INTO possible_match_candidates(workspace_id,canonical_company_id,source_record_id,score,match_reasons,status) VALUES ($1,$2,$3,.600,ARRAY['address_suite_mismatch'],'pending') RETURNING id::text`, w, third.CanonicalID, second.SourceRecordID)
	if err := review(t, pool, second.ReviewCandidateID, "reject", resolverNow); err != nil {
		t.Fatal(err)
	}
	if n := countSQL(t, pool, `SELECT count(*) FROM canonical_companies WHERE id=$1 AND pending_review`, second.CanonicalID); n != 1 {
		t.Fatal("pending flag cleared with another pending review")
	}
}

func TestResolveCandidatesUseCanonicalMaximumAndDeterministicTies(t *testing.T) {
	pool := testdb.New(t)
	w := seedWorkspace(t, pool)
	first := ingest(t, pool, recordInput(w, "first", RecordFacts{Address1: "10 Main", Zip: "01010"}))
	second := ingest(t, pool, recordInput(w, "second", RecordFacts{LegalName: "Acme", Zip: "01010", Domain: "acme.example"}))
	// Only one record blocks; all active facts in its canonical still participate.
	execSQL(t, pool, `UPDATE entity_merge_links SET canonical_company_id=$1 WHERE source_record_id=$2`, first.CanonicalID, second.SourceRecordID)
	third := ingest(t, pool, recordInput(w, "third", RecordFacts{}))
	updated := recordInput(w, "third", RecordFacts{LegalName: "Acme", State: "TX", Zip: "01010", Domain: "acme.example"})
	ingest(t, pool, updated)
	want, _ := OrderedPair(first.CanonicalID, third.CanonicalID)
	got := ingest(t, pool, recordInput(w, "fourth", RecordFacts{LegalName: "Acme", State: "TX", Zip: "01010", Domain: "acme.example", Address1: "10 Main"}))
	if got.CanonicalID != want || got.Score != .92 {
		t.Fatalf("wrong maximum or tie winner: got %+v, want %s", got, want)
	}
}

func TestResolveIgnoresOtherWorkspacesAndSplitLinks(t *testing.T) {
	pool := testdb.New(t)
	w, other := seedWorkspace(t, pool), seedWorkspace(t, pool)
	facts := RecordFacts{IDs: map[string]string{"uei": "same"}}
	ingest(t, pool, recordInput(other, "other-workspace", facts))
	first := ingest(t, pool, recordInput(w, "first", facts))
	if first.Decision != Distinct {
		t.Fatal("cross-workspace match")
	}
	execSQL(t, pool, `UPDATE entity_merge_links SET status='split' WHERE source_record_id=$1`, first.SourceRecordID)
	second := ingest(t, pool, recordInput(w, "second", facts))
	if second.CanonicalID == first.CanonicalID || second.Decision != Distinct {
		t.Fatalf("split record participated in blocking: %+v", second)
	}
	changed := recordInput(w, "first", facts)
	changed.Payload = json.RawMessage(`{"updated":true}`)
	got := ingest(t, pool, changed)
	if got.CanonicalID != first.CanonicalID || countSQL(t, pool, `SELECT count(*) FROM entity_merge_links WHERE source_record_id=$1 AND status='split'`, first.SourceRecordID) != 1 {
		t.Fatal("content update discarded split history")
	}
}

func TestResolveCallerOwnsTransaction(t *testing.T) {
	pool := testdb.New(t)
	w := seedWorkspace(t, pool)
	abort := errors.New("caller rollback")
	err := db.InTx(context.Background(), pool, func(tx pgx.Tx) error {
		if _, err := Resolve(context.Background(), tx, recordInput(w, "rollback", RecordFacts{}), resolverNow); err != nil {
			return err
		}
		return abort
	})
	if !errors.Is(err, abort) || countSQL(t, pool, `SELECT count(*) FROM source_records`) != 0 {
		t.Fatalf("Resolve did not leave transaction ownership to caller: %v", err)
	}
	_, second, _ := seedReview(t, pool, w)
	err = db.InTx(context.Background(), pool, func(tx pgx.Tx) error {
		if err := ResolveReview(context.Background(), tx, second.ReviewCandidateID, "merge", "actor", resolverNow); err != nil {
			return err
		}
		return abort
	})
	if !errors.Is(err, abort) || countSQL(t, pool, `SELECT count(*) FROM possible_match_candidates WHERE id=$1 AND status='pending'`, second.ReviewCandidateID) != 1 || countSQL(t, pool, `SELECT count(*) FROM canonical_companies WHERE id=$1`, second.CanonicalID) != 1 {
		t.Fatalf("ResolveReview did not roll back with caller: %v", err)
	}
}

func TestResolveConcurrentIngest(t *testing.T) {
	pool := testdb.New(t)
	w := seedWorkspace(t, pool)
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	type outcome struct {
		result ResolveResult
		err    error
	}
	results := make(chan outcome, 8)
	start := make(chan struct{})
	for i := 0; i < 8; i++ {
		in := recordInput(w, string(rune('a'+i%4)), RecordFacts{IDs: map[string]string{"uei": "same"}})
		go func() {
			<-start
			var got ResolveResult
			err := db.InTx(ctx, pool, func(tx pgx.Tx) error {
				var err error
				got, err = Resolve(ctx, tx, in, resolverNow)
				return err
			})
			results <- outcome{got, err}
		}()
	}
	close(start)
	var canonical string
	for i := 0; i < 8; i++ {
		got := <-results
		if got.err != nil {
			t.Fatal(got.err)
		}
		if canonical != "" && canonical != got.result.CanonicalID {
			t.Fatal("concurrent hard-ID ingests created separate companies")
		}
		canonical = got.result.CanonicalID
	}
	if countSQL(t, pool, `SELECT count(*) FROM source_records`) != 4 || countSQL(t, pool, `SELECT count(*) FROM canonical_companies`) != 1 || countSQL(t, pool, `SELECT count(*) FROM entity_merge_links`) != 4 {
		t.Fatal("concurrent ingest duplicated rows")
	}
}

func TestResolveConcurrentCallerForeignKeyLocks(t *testing.T) {
	pool := testdb.New(t)
	w := seedWorkspace(t, pool)
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	ready, start, results := make(chan struct{}, 2), make(chan struct{}), make(chan error, 2)
	for i := 0; i < 2; i++ {
		in := recordInput(w, string(rune('a'+i)), RecordFacts{IDs: map[string]string{"uei": "same"}})
		go func() {
			results <- db.InTx(ctx, pool, func(tx pgx.Tx) error {
				// Enrichment runs inserted by callers already hold workspace FK locks.
				if _, err := tx.Exec(ctx, `INSERT INTO enrichment_runs(workspace_id,idempotency_key,status) VALUES ($1,$2,'running')`, w, in.ExternalRef); err != nil {
					return err
				}
				ready <- struct{}{}
				<-start
				_, err := Resolve(ctx, tx, in, resolverNow)
				return err
			})
		}()
	}
	for i := 0; i < 2; i++ {
		select {
		case <-ready:
		case <-ctx.Done():
			t.Fatal(ctx.Err())
		}
	}
	close(start)
	for i := 0; i < 2; i++ {
		if err := <-results; err != nil {
			t.Error(err)
		}
	}
	if n := countSQL(t, pool, `SELECT count(*) FROM source_records`); n != 2 {
		t.Fatalf("caller FK locks lost an ingest: %d", n)
	}
}

func TestResolveReviewRetainedCompanyReevaluated(t *testing.T) {
	pool := testdb.New(t)
	w := seedWorkspace(t, pool)
	seedID(t, pool, `INSERT INTO buy_boxes(workspace_id,name,criteria) VALUES ($1,'states','{"states_in":["TX"]}') RETURNING id::text`, w)
	first, second, in := seedReview(t, pool, w)
	in.Citations[0].Status = quality.VerifiedFiling
	in.Payload = json.RawMessage(`{"changed":true}`)
	ingest(t, pool, in)
	third := ingest(t, pool, recordInput(w, "third", RecordFacts{}))
	execSQL(t, pool, `UPDATE entity_merge_links SET canonical_company_id=$1 WHERE source_record_id=$2`, second.CanonicalID, third.SourceRecordID)
	now := resolverNow.Add(time.Hour)
	if err := review(t, pool, second.ReviewCandidateID, "merge", now); err != nil {
		t.Fatal(err)
	}
	if n := countSQL(t, pool, `SELECT count(*) FROM canonical_companies WHERE id=$1 AND NOT pending_review`, second.CanonicalID); n != 1 {
		t.Fatal("retained company missing or still pending")
	}
	if n := countSQL(t, pool, `SELECT count(*) FROM buy_box_evaluations WHERE canonical_company_id=$1 AND fit='partial_unverified' AND evaluated_at=$2`, second.CanonicalID, now); n != 1 {
		t.Fatal("retained company evaluation still uses moved citation")
	}
	if n := countSQL(t, pool, `SELECT count(*) FROM buy_box_evaluations WHERE canonical_company_id=$1 AND fit='match' AND evaluated_at=$2`, first.CanonicalID, now); n != 1 {
		t.Fatal("target evaluation missing moved citation")
	}
}

func TestResolveReviewConcurrentActions(t *testing.T) {
	pool := testdb.New(t)
	_, second, _ := seedReview(t, pool, seedWorkspace(t, pool))
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	start, results := make(chan struct{}), make(chan error, 2)
	for _, action := range []string{"merge", "reject"} {
		go func() {
			<-start
			results <- db.InTx(ctx, pool, func(tx pgx.Tx) error {
				return ResolveReview(ctx, tx, second.ReviewCandidateID, action, "actor", resolverNow)
			})
		}()
	}
	close(start)
	resolved, conflicted := 0, 0
	for i := 0; i < 2; i++ {
		err := <-results
		if err == nil {
			resolved++
		} else if errors.Is(err, ErrAlreadyResolved) {
			conflicted++
		} else {
			t.Fatal(err)
		}
	}
	if resolved != 1 || conflicted != 1 {
		t.Fatalf("concurrent review results: resolved=%d conflicting=%d", resolved, conflicted)
	}
}
