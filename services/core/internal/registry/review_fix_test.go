package registry

import (
	"context"
	"strings"
	"testing"

	"rowan.ai/rowan/services/core/internal/enrich"
	"rowan.ai/rowan/services/core/internal/testdb"
)

func TestSunbizInvalidUTF8Loads(t *testing.T) {
	pool := testdb.New(t)
	row := fixedWidth(map[int]string{1: "P26000099999", 13: "CAF\xc9 LLC", 305: "MIA\xc9MI", 333: "F\xc9", 335: "33\xc901"})
	stats, err := LoadSunbizCorp(context.Background(), pool, sunbizTempFile(t, row+"\n"), "s", sunbizLoadTime)
	if err != nil || stats.Inserted != 1 {
		t.Fatalf("stats=%+v err=%v", stats, err)
	}
	if got := sunbizStored(t, pool, "P26000099999").Facts.LegalName; got != "CAF LLC" {
		t.Fatalf("name=%q", got)
	}
}

func TestSAMInvalidUTF8Loads(t *testing.T) {
	pool := testdb.New(t)
	data := strings.Replace(samFixture(t), "ACME HVAC LLC", "ACME \xc9 LLC", 1)
	stats, err := LoadSAM(context.Background(), pool, samTempFile(t, data), "s", samLoadTime)
	if err != nil || stats.Inserted != 3 {
		t.Fatalf("stats=%+v err=%v", stats, err)
	}
}

func TestSAMNPDYRemovesExistingRow(t *testing.T) {
	pool := testdb.New(t)
	ctx := context.Background()
	if _, err := LoadSAM(ctx, pool, samTempFile(t, samFixture(t)), "s", samLoadTime); err != nil {
		t.Fatal(err)
	}
	lines := strings.Split(samFixture(t), "\n")
	var acme int
	for i, l := range lines {
		if strings.HasPrefix(l, "ABCDEF123456|") {
			acme = i
		}
	}
	fields := strings.Split(lines[acme], "|")
	fields[118] = "NPDY"
	lines[acme] = strings.Join(fields, "|")
	stats, err := LoadSAM(ctx, pool, samTempFile(t, strings.Join(lines, "\n")), "s2", samLoadTime)
	if err != nil || stats.Skipped != 2 || stats.Read != 4 || stats.Unchanged != 2 {
		t.Fatalf("stats=%+v err=%v", stats, err)
	}
	var n int
	if err := pool.QueryRow(ctx, `SELECT count(*) FROM registry_entities WHERE external_ref='ABCDEF123456'`).Scan(&n); err != nil || n != 0 {
		t.Fatalf("row remains: n=%d err=%v", n, err)
	}
	recs, err := (&Provider{Pool: pool}).Lookup(ctx, enrich.Candidate{Ref: "c", IDs: map[string]string{"uei": "ABCDEF123456"}, LegalName: "ACME HVAC LLC"})
	if err != nil || len(recs) != 0 {
		t.Fatalf("recs=%v err=%v", recs, err)
	}
}

func TestLoadDuplicateKeepsLastNoDiffAgainstSameBatch(t *testing.T) {
	pool := testdb.New(t)
	e := entities(1)[0]
	changed := e
	changed.Facts.Principal = "New Principal"
	calls := 0
	diff := func(old, new Entity, meta LoadMeta) []Signal { calls++; return []Signal{changeSignal(new, meta)} }
	// First sight: duplicates differ, last wins, no signals.
	stats, err := Load(context.Background(), pool, loadMeta(), sliceNext([]Entity{e, changed}), diff)
	if err != nil || stats.Inserted != 1 || stats.Skipped != 1 || stats.Updated != 0 || stats.Signals != 0 || calls != 0 {
		t.Fatalf("first = %+v, %v calls=%d", stats, err, calls)
	}
	// Reload in the same order: last occurrence is unchanged, so no churn.
	stats, err = Load(context.Background(), pool, loadMeta(), sliceNext([]Entity{e, changed}), diff)
	if err != nil || stats.Unchanged != 1 || stats.Skipped != 1 || stats.Updated != 0 || stats.Signals != 0 || calls != 0 {
		t.Fatalf("reload = %+v, %v calls=%d", stats, err, calls)
	}
	var principal string
	if err := pool.QueryRow(context.Background(), `SELECT entity->'facts'->>'Principal' FROM registry_entities`).Scan(&principal); err != nil || principal != "New Principal" {
		t.Fatalf("principal=%q err=%v", principal, err)
	}
}

func TestLoadTombstoneDeletes(t *testing.T) {
	pool := testdb.New(t)
	rows := entities(2)
	if _, err := Load(context.Background(), pool, loadMeta(), sliceNext(rows), nil); err != nil {
		t.Fatal(err)
	}
	rows[0].Tombstone = true
	stats, err := Load(context.Background(), pool, loadMeta(), sliceNext(rows), nil)
	if err != nil || stats.Skipped != 1 || stats.Unchanged != 1 {
		t.Fatalf("stats=%+v err=%v", stats, err)
	}
	var n int
	if err := pool.QueryRow(context.Background(), `SELECT count(*) FROM registry_entities`).Scan(&n); err != nil || n != 1 {
		t.Fatalf("n=%d err=%v", n, err)
	}
}

func TestSunbizRejectsLongLine(t *testing.T) {
	long := strings.Repeat("x", 70*1024)
	_, err := LoadSunbizCorp(context.Background(), testdb.New(t), sunbizTempFile(t, long), "s", sunbizLoadTime)
	if err == nil || !strings.Contains(err.Error(), "line 1: line too long") {
		t.Fatalf("corp err=%v", err)
	}
	_, err = LoadSunbizEvents(context.Background(), testdb.New(t), eventTempFile(t, long), "s", sunbizLoadTime)
	if err == nil || !strings.Contains(err.Error(), "line 1: line too long") {
		t.Fatalf("events err=%v", err)
	}
}

func TestEventsBlankDocNumberUnmapped(t *testing.T) {
	pool := testdb.New(t)
	rows := eventFixtureRows(t)
	blank := eventField(rows[0], 1, 12, "")
	stats, err := LoadSunbizEvents(context.Background(), pool, eventTempFile(t, blank+"\n"), "s", sunbizLoadTime)
	if err != nil || stats.Signals != 0 || stats.Unmapped != 1 {
		t.Fatalf("stats=%+v err=%v", stats, err)
	}
	var n int
	if err := pool.QueryRow(context.Background(), `SELECT count(*) FROM registry_signals`).Scan(&n); err != nil || n != 0 {
		t.Fatalf("signals=%d err=%v", n, err)
	}
}

func TestLookupIDMissFallsBackToName(t *testing.T) {
	p := &Provider{Pool: providerPool(t), MaxPerSource: 3}
	got := lookupSources(t, p, enrich.Candidate{Ref: "c", IDs: map[string]string{"uei": "NOSUCHUEI000"}, LegalName: "Acme HVAC, LLC", State: "FL"})
	if got["sam"] != 1 || got["fl_sunbiz"] != 1 || got["fl_dbpr"] != 1 {
		t.Fatalf("got %v", got)
	}
}

func TestCitationsIncludeDBPRPrimaryStatus(t *testing.T) {
	e := Entity{SourceSystem: "fl_dbpr", ExternalRef: "L1", Extra: map[string]any{"primary_status": "Current", "status": "x"}}
	e.Facts.LegalName = "A"
	found := false
	for _, c := range citations("fl_dbpr", "f.csv", sunbizLoadTime, e) {
		if c.FieldName == "primary_status" && string(c.Value) == `"Current"` {
			found = true
		}
	}
	if !found {
		t.Fatal("primary_status not cited")
	}
}
