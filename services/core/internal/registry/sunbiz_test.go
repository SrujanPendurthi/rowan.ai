package registry

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"

	"rowan.ai/rowan/services/core/internal/proxy"
	"rowan.ai/rowan/services/core/internal/quality"
	"rowan.ai/rowan/services/core/internal/testdb"
)

var sunbizLoadTime = time.Date(2026, 11, 20, 12, 0, 0, 0, time.UTC)

// fixedWidth pads 1-based field starts to the published 1440-byte record size.
func fixedWidth(fields map[int]string) string {
	row := []byte(strings.Repeat(" ", 1440))
	for start, value := range fields {
		copy(row[start-1:], value)
	}
	return string(row)
}

func sunbizFixtureRows(q2 bool) []string {
	anchor := map[int]string{
		1: "P26000012345", 13: "ACME HVAC LLC", 205: "A", 206: "DOMP",
		221: "100 MAIN ST", 263: "SUITE 2", 305: "TAMPA", 333: "FL", 335: "33602-1234", 345: "US",
		473: "20260102", 481: "59-1234567", 495: "N", 496: "20260115",
		545: "REGISTERED AGENTS INC", 587: "C", 588: "200 AGENT ST", 630: "TAMPA", 658: "FL", 660: "336021234",
		669: "P", 673: "P", 674: "JOHN SMITH", 716: "100 MAIN ST", 758: "TAMPA", 786: "FL", 788: "33602",
		797: "S", 801: "P", 802: "MARY SMITH", 844: "100 MAIN ST", 886: "TAMPA", 914: "FL", 916: "33602",
	}
	second := map[int]string{
		1: "P26000023456", 13: "BAY ELECTRIC INC", 205: "A", 206: "DOMP",
		221: "200 OAK AVE", 305: "TAMPA", 333: "FL", 335: "33603", 473: "01102026",
		481: "00-0000000", 496: "01152026", 545: "OLD AGENT LLC", 669: "MGR", 674: "ALICE JONES",
	}
	third := map[int]string{
		1: "L26000034567", 13: "GULF PLUMBING LLC", 205: "I", 206: "FLAL",
		221: "300 PINE ST", 305: "MIAMI", 333: "FL", 335: "33101", 481: "12345678", 496: "00000000",
		669 + 128*5: "S", 674 + 128*5: "ROBERT LEE", 758 + 128*5: "MIAMI", 786 + 128*5: "FL",
	}
	if q2 {
		anchor[674], anchor[496] = "ALEX SMITH", "20260415"
		second[545], second[496] = "NEW AGENTS INC", "04152026"
		third[13] = "GULF PLUMBING SERVICES LLC"
	}
	return []string{fixedWidth(anchor), fixedWidth(second), fixedWidth(third)}
}

func sunbizTempFile(t *testing.T, data string) string {
	t.Helper()
	path := filepath.Join(t.TempDir(), "cordata.txt")
	if err := os.WriteFile(path, []byte(data), 0600); err != nil {
		t.Fatal(err)
	}
	return path
}

func sunbizStored(t *testing.T, pool *pgxpool.Pool, ref string) Entity {
	t.Helper()
	var e Entity
	if err := pool.QueryRow(context.Background(), `SELECT entity FROM registry_entities
		WHERE source_system = 'fl_sunbiz' AND external_ref = $1`, ref).Scan(&e); err != nil {
		t.Fatal(err)
	}
	return e
}

func TestParseSunbizDate(t *testing.T) {
	for _, input := range []string{"20260415", "04152026", " 20260415 "} {
		got := parseSunbizDate(input)
		want := time.Date(2026, 4, 15, 0, 0, 0, 0, time.UTC)
		if got == nil || !got.Equal(want) {
			t.Errorf("parseSunbizDate(%q) = %v, want %v", input, got, want)
		}
	}
	for _, input := range []string{"00000000", "        ", "", "20260230", "13312026", "2026041", "2026abcd"} {
		if got := parseSunbizDate(input); got != nil {
			t.Errorf("parseSunbizDate(%q) = %v, want nil", input, got)
		}
	}
	for _, input := range []string{"19000101", "21001231", "01011899"} {
		if got := parseSunbizDate(input); got == nil {
			t.Errorf("parseSunbizDate(%q) = nil", input)
		}
	}
}

func TestSunbizFixtures(t *testing.T) {
	for _, tc := range []struct {
		name string
		q2   bool
	}{{"cordata_q1.txt", false}, {"cordata_q2.txt", true}} {
		data, err := os.ReadFile(filepath.Join("testdata", tc.name))
		if err != nil {
			t.Fatal(err)
		}
		if want := strings.Join(sunbizFixtureRows(tc.q2), "\n") + "\n"; string(data) != want {
			t.Fatalf("%s differs from the fixed-width fixture helper", tc.name)
		}
	}
}

func TestLoadSunbizCorpFields(t *testing.T) {
	pool := testdb.New(t)
	ctx := context.Background()
	stats, err := LoadSunbizCorp(ctx, pool, "testdata/cordata_q1.txt", "snapshots/q1", sunbizLoadTime)
	if err != nil || stats.Read != 3 || stats.Inserted != 3 || stats.Signals != 0 {
		t.Fatalf("load = %+v, %v", stats, err)
	}
	e := sunbizStored(t, pool, "P26000012345")
	if e.SourceSystem != "fl_sunbiz" || e.ExternalRef != "P26000012345" || e.Line != 1 ||
		e.Facts.LegalName != "ACME HVAC LLC" || e.Facts.Address1 != "100 MAIN ST" ||
		e.Facts.State != "FL" || e.Facts.Zip != "33602" || e.Facts.Principal != "JOHN SMITH" ||
		e.Facts.RegisteredAgent != "REGISTERED AGENTS INC" ||
		!reflect.DeepEqual(e.Facts.IDs, map[string]string{"filing:fl": "P26000012345", "fein": "591234567"}) {
		t.Fatalf("anchor entity = %+v", e)
	}
	wantOfficers := []any{
		map[string]any{"title": "P", "type": "P", "name": "JOHN SMITH", "city": "TAMPA", "state": "FL"},
		map[string]any{"title": "S", "type": "P", "name": "MARY SMITH", "city": "TAMPA", "state": "FL"},
	}
	if !reflect.DeepEqual(e.Extra["officers"], wantOfficers) {
		t.Fatalf("officers = %#v", e.Extra["officers"])
	}
	for key, want := range map[string]string{
		"status": "A", "filing_type": "DOMP", "file_date": "20260102", "last_transaction_date": "20260115",
	} {
		if e.Extra[key] != want {
			t.Errorf("Extra[%q] = %v, want %q", key, e.Extra[key], want)
		}
	}
	if e.SourceUpdatedAt == nil || e.SourceUpdatedAt.Format("2006-01-02") != "2026-01-15" || e.Raw["city"] != "TAMPA" {
		t.Fatalf("date/raw fields = %+v", e)
	}
	for _, tc := range []struct{ ref, principal string }{{"P26000023456", "ALICE JONES"}, {"L26000034567", "ROBERT LEE"}} {
		e := sunbizStored(t, pool, tc.ref)
		if _, ok := e.Facts.IDs["fein"]; ok || e.Facts.Principal != tc.principal {
			t.Errorf("invalid FEI/fallback principal = %+v", e)
		}
	}
	var file, snapshot string
	var loaded, updated time.Time
	if err := pool.QueryRow(ctx, `SELECT file_name, snapshot_ref, loaded_at, source_updated_at
		FROM registry_entities WHERE external_ref = 'P26000012345'`).Scan(&file, &snapshot, &loaded, &updated); err != nil {
		t.Fatal(err)
	}
	if file != "cordata_q1.txt" || snapshot != "snapshots/q1" || !loaded.Equal(sunbizLoadTime) || !updated.Equal(*e.SourceUpdatedAt) {
		t.Fatalf("metadata = %s, %s, %v, %v", file, snapshot, loaded, updated)
	}
}

func assertSunbizSignal(t *testing.T, kind proxy.SignalType, ref, field, date string) quality.Citation {
	t.Helper()
	pool := testdb.New(t)
	ctx := context.Background()
	for _, quarter := range []string{"q1", "q2"} {
		stats, err := LoadSunbizCorp(ctx, pool, "testdata/cordata_"+quarter+".txt", "snapshots/"+quarter, sunbizLoadTime)
		if err != nil || (quarter == "q2" && (stats.Updated != 3 || stats.Signals != 3)) {
			t.Fatalf("%s load = %+v, %v", quarter, stats, err)
		}
	}
	var count int
	if err := pool.QueryRow(ctx, `SELECT count(*) FROM registry_signals WHERE signal_type = $1`, kind).Scan(&count); err != nil || count != 1 {
		t.Fatalf("%s signals = %d, %v", kind, count, err)
	}
	var observed time.Time
	var citation quality.Citation
	if err := pool.QueryRow(ctx, `SELECT observed_at, citation FROM registry_signals
		WHERE source_system = 'fl_sunbiz' AND external_ref = $1 AND signal_type = $2`, ref, kind).Scan(&observed, &citation); err != nil {
		t.Fatal(err)
	}
	line := map[string]string{"P26000012345": "line 1", "P26000023456": "line 2", "L26000034567": "line 3"}[ref]
	if observed.UTC().Format("2006-01-02") != date || citation.TableName != "cordata" || citation.RowKey != ref ||
		citation.Status != quality.VerifiedFiling || citation.Origin != quality.RawSource || citation.ExportPolicy != quality.Allow ||
		citation.SourceSystem != "fl_sunbiz" || citation.SourceRef != "cordata_q2.txt" || citation.PageOrSpan != line ||
		citation.FieldName != field || !citation.RetrievedAt.Equal(sunbizLoadTime) {
		t.Fatalf("signal = %v, %+v", observed, citation)
	}
	if err := citation.Validate(); err != nil {
		t.Fatal(err)
	}
	return citation
}

func TestLoadSunbizOfficerChangeSignal(t *testing.T) {
	citation := assertSunbizSignal(t, proxy.OfficerChange, "P26000012345", "officers", "2026-04-15")
	var officers []map[string]string
	if err := json.Unmarshal(citation.Value, &officers); err != nil || len(officers) != 2 || officers[0]["name"] != "ALEX SMITH" {
		t.Fatalf("officer citation = %s, %v", citation.Value, err)
	}
}

func TestLoadSunbizAgentChangeSignal(t *testing.T) {
	citation := assertSunbizSignal(t, proxy.RegisteredAgentChange, "P26000023456", "registered_agent", "2026-04-15")
	if string(citation.Value) != `"NEW AGENTS INC"` {
		t.Fatalf("agent citation = %s", citation.Value)
	}
}

func TestLoadSunbizLegalNameChangeSignal(t *testing.T) {
	citation := assertSunbizSignal(t, proxy.EntityChange, "L26000034567", "legal_name", "2026-11-20")
	if string(citation.Value) != `"GULF PLUMBING SERVICES LLC"` {
		t.Fatalf("name citation = %s", citation.Value)
	}
}

func TestLoadSunbizIdempotent(t *testing.T) {
	pool := testdb.New(t)
	ctx := context.Background()
	for _, quarter := range []string{"q1", "q2"} {
		if _, err := LoadSunbizCorp(ctx, pool, "testdata/cordata_"+quarter+".txt", quarter, sunbizLoadTime); err != nil {
			t.Fatal(err)
		}
	}
	stats, err := LoadSunbizCorp(ctx, pool, "testdata/cordata_q2.txt", "different-snapshot", sunbizLoadTime.Add(time.Hour))
	if err != nil || stats.Unchanged != 3 || stats.Signals != 0 || stats.Updated != 0 || stats.Inserted != 0 || len(stats.SignalRefs) != 0 {
		t.Fatalf("repeat = %+v, %v", stats, err)
	}
	var count int
	if err := pool.QueryRow(ctx, `SELECT count(*) FROM registry_signals`).Scan(&count); err != nil || count != 3 {
		t.Fatalf("signal count = %d, %v", count, err)
	}
}

func TestLoadSunbizRejectsMalformed(t *testing.T) {
	for _, size := range []int{0, 900, 1439} {
		t.Run(fmt.Sprintf("%d bytes", size), func(t *testing.T) {
			pool := testdb.New(t)
			data := sunbizFixtureRows(false)[0] + "\r\n" + strings.Repeat(" ", size) + "\r\n"
			stats, err := LoadSunbizCorp(context.Background(), pool, sunbizTempFile(t, data), "snapshot", sunbizLoadTime)
			if err == nil || !strings.Contains(err.Error(), "line 2") || stats.Inserted != 0 {
				t.Fatalf("malformed load = %+v, %v", stats, err)
			}
			var count int
			if err := pool.QueryRow(context.Background(), `SELECT count(*) FROM registry_entities`).Scan(&count); err != nil || count != 0 {
				t.Fatalf("discarded batch count = %d, %v", count, err)
			}
		})
	}
}

func TestLoadSunbizLineBoundaries(t *testing.T) {
	pool := testdb.New(t)
	rows := sunbizFixtureRows(false)
	// Longer lines, CRLF, and a final line without a newline preserve physical lines.
	data := rows[0] + strings.Repeat("x", 60000) + "\r\n" + rows[1] + "\r\n" + rows[2]
	stats, err := LoadSunbizCorp(context.Background(), pool, sunbizTempFile(t, data), "snapshot", sunbizLoadTime)
	if err != nil || stats.Inserted != 3 {
		t.Fatalf("boundary load = %+v, %v", stats, err)
	}
	for i, ref := range []string{"P26000012345", "P26000023456", "L26000034567"} {
		if e := sunbizStored(t, pool, ref); e.Line != i+1 || e.Facts.State != "FL" {
			t.Errorf("line %d entity = %+v", i+1, e)
		}
	}
}

func TestLoadSunbizNormalizedChanges(t *testing.T) {
	pool := testdb.New(t)
	ctx := context.Background()
	if _, err := LoadSunbizCorp(ctx, pool, "testdata/cordata_q1.txt", "q1", sunbizLoadTime); err != nil {
		t.Fatal(err)
	}
	row := fixedWidth(map[int]string{
		1: "P26000012345", 13: "acme hvac, llc.", 205: "A", 206: "DOMP",
		221: "100 MAIN ST", 305: "TAMPA", 333: "FL", 335: "33602", 481: "59.1234567", 496: "20260415",
		545: "registered agents, inc.",
		669: "S", 673: "P", 674: "mary smith.", 758: "TAMPA", 786: "FL",
		797: "VP", 801: "P", 802: "john smith.", 886: "TAMPA", 914: "FL",
	})
	stats, err := LoadSunbizCorp(ctx, pool, sunbizTempFile(t, row+"\n"), "q2", sunbizLoadTime)
	if err != nil || stats.Updated != 1 || stats.Signals != 0 {
		t.Fatalf("cosmetic/order changes = %+v, %v", stats, err)
	}
	if e := sunbizStored(t, pool, "P26000012345"); e.Facts.Principal != "john smith." || e.Facts.IDs["fein"] != "591234567" {
		t.Fatalf("president/FEI = %+v", e)
	}
}

func TestSunbizDiffOfficerSets(t *testing.T) {
	entity := func(names ...string) Entity {
		officers := []any{}
		for _, name := range names {
			officers = append(officers, map[string]any{"name": name, "title": "P"})
		}
		return Entity{SourceSystem: "fl_sunbiz", ExternalRef: "P26000012345", Line: 2,
			Extra: map[string]any{"officers": officers}}
	}
	meta := LoadMeta{SourceSystem: "fl_sunbiz", FileName: "cordata.txt", TableName: "cordata", LoadedAt: sunbizLoadTime}
	for _, tc := range []struct {
		name     string
		old, new []string
		want     int
	}{
		{"reorder and duplicate", []string{"JOHN SMITH", "MARY SMITH"}, []string{"mary smith.", "john smith", "JOHN SMITH"}, 0},
		{"replacement", []string{"JOHN SMITH"}, []string{"ALEX SMITH"}, 1},
		{"addition", []string{"JOHN SMITH"}, []string{"JOHN SMITH", "ALEX SMITH"}, 1},
		{"removal", []string{"JOHN SMITH", "MARY SMITH"}, []string{"JOHN SMITH"}, 1},
		{"old empty", nil, []string{"JOHN SMITH"}, 0},
		{"new empty", []string{"JOHN SMITH"}, nil, 0},
		{"empty names", []string{" "}, []string{"JOHN SMITH"}, 0},
	} {
		t.Run(tc.name, func(t *testing.T) {
			sigs := sunbizDiff(entity(tc.old...), entity(tc.new...), meta)
			if len(sigs) != tc.want || (len(sigs) > 0 && sigs[0].Type != proxy.OfficerChange) {
				t.Fatalf("diff = %+v, want %d officer signals", sigs, tc.want)
			}
		})
	}
}

func TestSunbizDiffObservedAt(t *testing.T) {
	meta := LoadMeta{SourceSystem: "fl_sunbiz", FileName: "cordata.txt", TableName: "cordata", LoadedAt: sunbizLoadTime}
	for _, tc := range []struct{ name, old, new, want string }{
		{"later", "20260101", "20260415", "2026-04-15"},
		{"same", "20260415", "20260415", "2026-11-20"},
		{"earlier", "20260501", "20260415", "2026-11-20"},
		{"old missing", "", "20260415", "2026-04-15"},
		{"new missing", "20260415", "", "2026-11-20"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			old, new := Entity{SourceUpdatedAt: parseSunbizDate(tc.old)}, Entity{SourceUpdatedAt: parseSunbizDate(tc.new)}
			old.Facts.LegalName, new.Facts.LegalName = "OLD LLC", "NEW LLC"
			sigs := sunbizDiff(old, new, meta)
			if len(sigs) != 1 || sigs[0].ObservedAt.Format("2006-01-02") != tc.want {
				t.Fatalf("diff = %+v, want date %s", sigs, tc.want)
			}
		})
	}
	for _, names := range [][2]string{{"", "NEW AGENT"}, {"OLD AGENT", ""}, {"Agent, Inc.", "AGENT INC"}} {
		old, new := Entity{}, Entity{}
		old.Facts.RegisteredAgent, new.Facts.RegisteredAgent = names[0], names[1]
		if sigs := sunbizDiff(old, new, meta); len(sigs) != 0 {
			t.Errorf("agent %q -> %q: %+v", names[0], names[1], sigs)
		}
	}
}
