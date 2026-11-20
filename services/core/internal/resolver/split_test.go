package resolver

import (
	"context"
	"encoding/json"
	"errors"
	"testing"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	"rowan.ai/rowan/services/core/internal/db"
	"rowan.ai/rowan/services/core/internal/quality"
	"rowan.ai/rowan/services/core/internal/testdb"
)

func split(t *testing.T, pool *pgxpool.Pool, canonical string, ids []string) (string, error) {
	t.Helper()
	var id string
	err := db.InTx(context.Background(), pool, func(tx pgx.Tx) error {
		var err error
		id, err = Split(context.Background(), tx, canonical, ids, "tester", "", resolverNow)
		return err
	})
	return id, err
}

func fieldCitation(field, value string, status quality.Status) quality.Citation {
	c := stateCitation(status)
	c.FieldName, c.Value = field, json.RawMessage(value)
	return c
}

// seedABC ingests A,B,C into one canonical; C also carries a hard ID unique to it.
func seedABC(t *testing.T, pool *pgxpool.Pool, w string) (canonical string, a, b, c ResolveResult) {
	t.Helper()
	ain := recordInput(w, "A", RecordFacts{LegalName: "Acme A", IDs: map[string]string{"ein": "1"}})
	ain.Citations = []quality.Citation{fieldCitation("legal_name", `"Acme A"`, quality.ScrapedWeb)}
	a = ingest(t, pool, ain)
	b = ingest(t, pool, recordInput(w, "B", RecordFacts{LegalName: "Acme B", IDs: map[string]string{"ein": "1"}}))
	cin := recordInput(w, "C", RecordFacts{LegalName: "Cee Corp", IDs: map[string]string{"ein": "1", "duns": "c-only"}})
	cin.Citations = []quality.Citation{fieldCitation("legal_name", `"Cee Corp"`, quality.VerifiedFiling)}
	c = ingest(t, pool, cin)
	if a.CanonicalID != b.CanonicalID || a.CanonicalID != c.CanonicalID {
		t.Fatalf("expected one canonical: %+v %+v %+v", a, b, c)
	}
	return a.CanonicalID, a, b, c
}

func linkCanonical(t *testing.T, pool *pgxpool.Pool, record string) string {
	t.Helper()
	return seedID(t, pool, `SELECT canonical_company_id::text FROM entity_merge_links WHERE source_record_id=$1`, record)
}

func TestSplitMovesRecordsAndCitations(t *testing.T) {
	pool := testdb.New(t)
	w := seedWorkspace(t, pool)
	canonical, a, b, c := seedABC(t, pool, w)
	execSQL(t, pool, `UPDATE entity_merge_links SET merge_confidence_score=.912,match_reasons='{x}' WHERE source_record_id=$1`, c.SourceRecordID)
	newID, err := split(t, pool, canonical, []string{c.SourceRecordID})
	if err != nil {
		t.Fatal(err)
	}
	if newID == canonical || linkCanonical(t, pool, c.SourceRecordID) != newID ||
		linkCanonical(t, pool, a.SourceRecordID) != canonical || linkCanonical(t, pool, b.SourceRecordID) != canonical {
		t.Fatal("links not moved correctly")
	}
	var mode, status string
	var score float64
	var reasons []string
	if err := pool.QueryRow(context.Background(), `SELECT merge_mode,status,merge_confidence_score,match_reasons FROM entity_merge_links WHERE source_record_id=$1`, c.SourceRecordID).Scan(&mode, &status, &score, &reasons); err != nil {
		t.Fatal(err)
	}
	if mode != "manual" || status != "active" || score != .912 || len(reasons) != 1 || reasons[0] != "x" {
		t.Fatalf("bad moved link: %s %s %v %v", mode, status, score, reasons)
	}
	if n := countSQL(t, pool, `SELECT count(*) FROM field_citations WHERE canonical_company_id=$1 AND source_record_id=$2`, newID, c.SourceRecordID); n != 1 {
		t.Fatalf("new canonical citations: %d", n)
	}
	if n := countSQL(t, pool, `SELECT count(*) FROM field_citations WHERE canonical_company_id=$1 AND source_record_id=$2`, canonical, c.SourceRecordID); n != 0 {
		t.Fatalf("original kept moved citations: %d", n)
	}
	var name string
	var pending bool
	if err := pool.QueryRow(context.Background(), `SELECT display_name,pending_review FROM canonical_companies WHERE id=$1`, newID).Scan(&name, &pending); err != nil {
		t.Fatal(err)
	}
	if name != "Cee Corp" || pending {
		t.Fatalf("bad new canonical: %q %v", name, pending)
	}
	if n := seedID(t, pool, `SELECT display_name FROM canonical_companies WHERE id=$1`, canonical); n != "Acme A" {
		t.Fatalf("original display name: %q", n)
	}
}

func TestSplitWritesDoNotMergePairs(t *testing.T) {
	pool := testdb.New(t)
	w := seedWorkspace(t, pool)
	canonical, a, b, c := seedABC(t, pool, w)
	if _, err := split(t, pool, canonical, []string{c.SourceRecordID}); err != nil {
		t.Fatal(err)
	}
	for _, other := range []string{a.SourceRecordID, b.SourceRecordID} {
		x, y := OrderedPair(c.SourceRecordID, other)
		var reason, by string
		if err := pool.QueryRow(context.Background(), `SELECT reason,created_by FROM do_not_merge_pairs WHERE record_a_id=$1 AND record_b_id=$2`, x, y).Scan(&reason, &by); err != nil {
			t.Fatal(err)
		}
		if reason != "manual_split" || by != "tester" {
			t.Fatalf("bad pair: %s %s", reason, by)
		}
	}
	if n := countSQL(t, pool, `SELECT count(*) FROM do_not_merge_pairs`); n != 2 {
		t.Fatalf("pairs: %d", n)
	}
}

func TestSplitPreventsRemerge(t *testing.T) {
	pool := testdb.New(t)
	w := seedWorkspace(t, pool)
	canonical, _, _, c := seedABC(t, pool, w)
	newID, err := split(t, pool, canonical, []string{c.SourceRecordID})
	if err != nil {
		t.Fatal(err)
	}
	// A new record matching only C by hard ID joins C's canonical, not the original.
	d := ingest(t, pool, recordInput(w, "D", RecordFacts{LegalName: "Dee", IDs: map[string]string{"duns": "c-only"}}))
	if d.Decision != AutoMerge || d.CanonicalID != newID {
		t.Fatalf("new record should merge into split canonical: %+v", d)
	}
	// A new record matching both sides must not rejoin the original blindly: it only
	// qualifies for canonicals with no do-not-merge pair against it, so C still stays put.
	cin := recordInput(w, "C", RecordFacts{LegalName: "Cee Corp 2", IDs: map[string]string{"ein": "1", "duns": "c-only"}})
	again := ingest(t, pool, cin)
	if again.CanonicalID != newID || linkCanonical(t, pool, c.SourceRecordID) != newID {
		t.Fatalf("re-ingested C left split canonical: %+v", again)
	}
}

func TestSplitReevaluates(t *testing.T) {
	pool := testdb.New(t)
	w := seedWorkspace(t, pool)
	box := seedID(t, pool, `INSERT INTO buy_boxes(workspace_id,name,criteria) VALUES ($1,'b','{"revenue_min_cents":20}') RETURNING id::text`, w)
	a := recordInput(w, "A", RecordFacts{LegalName: "Acme A", IDs: map[string]string{"ein": "1"}})
	a.Citations = []quality.Citation{fieldCitation("revenue_cents", `30`, quality.ScrapedWeb)}
	ra := ingest(t, pool, a)
	cin := recordInput(w, "C", RecordFacts{LegalName: "Cee", IDs: map[string]string{"ein": "1"}})
	cin.Citations = []quality.Citation{fieldCitation("revenue_cents", `30`, quality.VerifiedFiling)}
	rc := ingest(t, pool, cin)
	status := func(canonical string) string {
		var raw []byte
		if err := pool.QueryRow(context.Background(), `SELECT criteria_breakdown FROM buy_box_evaluations WHERE buy_box_id=$1 AND canonical_company_id=$2`, box, canonical).Scan(&raw); err != nil {
			t.Fatal(err)
		}
		var rs []struct{ Status string }
		if err := json.Unmarshal(raw, &rs); err != nil || len(rs) != 1 {
			t.Fatalf("bad breakdown %s %v", raw, err)
		}
		return rs[0].Status
	}
	if got := status(ra.CanonicalID); got != "MET" {
		t.Fatalf("before split: %s", got)
	}
	newID, err := split(t, pool, ra.CanonicalID, []string{rc.SourceRecordID})
	if err != nil {
		t.Fatal(err)
	}
	if got := status(ra.CanonicalID); got != "UNVERIFIED_LOW_CONFIDENCE" {
		t.Fatalf("original: %s", got)
	}
	if got := status(newID); got != "MET" {
		t.Fatalf("new: %s", got)
	}
}

func TestSplitRejectsInvalidSelection(t *testing.T) {
	const missing = "00000000-0000-0000-0000-000000000001"
	for _, tc := range []struct {
		name string
		ids  func(a, b, c, other string) []string
	}{
		{"all", func(a, b, c, _ string) []string { return []string{a, b, c} }},
		{"unknown", func(a, b, c, _ string) []string { return []string{missing} }},
		{"other canonical", func(a, b, c, o string) []string { return []string{c, o} }},
		{"duplicate", func(a, b, c, _ string) []string { return []string{c, c} }},
		{"empty", func(a, b, c, _ string) []string { return nil }},
		{"malformed", func(a, b, c, _ string) []string { return []string{"nope"} }},
	} {
		t.Run(tc.name, func(t *testing.T) {
			pool := testdb.New(t)
			w := seedWorkspace(t, pool)
			canonical, a, b, c := seedABC(t, pool, w)
			o := ingest(t, pool, recordInput(w, "O", RecordFacts{LegalName: "Other Co"}))
			counts := func() [3]int {
				return [3]int{countSQL(t, pool, `SELECT count(*) FROM canonical_companies`), countSQL(t, pool, `SELECT count(*) FROM entity_merge_links`), countSQL(t, pool, `SELECT count(*) FROM do_not_merge_pairs`)}
			}
			before := counts()
			_, err := split(t, pool, canonical, tc.ids(a.SourceRecordID, b.SourceRecordID, c.SourceRecordID, o.SourceRecordID))
			if !errors.Is(err, ErrInvalidSplit) || counts() != before {
				t.Fatalf("err=%v counts %v -> %v", err, before, counts())
			}
		})
	}
	t.Run("unknown canonical and empty actor", func(t *testing.T) {
		pool := testdb.New(t)
		w := seedWorkspace(t, pool)
		canonical, _, _, c := seedABC(t, pool, w)
		if _, err := split(t, pool, missing, []string{c.SourceRecordID}); !errors.Is(err, ErrInvalidSplit) {
			t.Fatal(err)
		}
		err := db.InTx(context.Background(), pool, func(tx pgx.Tx) error {
			_, err := Split(context.Background(), tx, canonical, []string{c.SourceRecordID}, "", "", resolverNow)
			return err
		})
		if !errors.Is(err, ErrInvalidSplit) {
			t.Fatal(err)
		}
	})
}
