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

	"rowan.ai/rowan/services/core/internal/proxy"
	"rowan.ai/rowan/services/core/internal/quality"
	"rowan.ai/rowan/services/core/internal/testdb"
)

func eventFixtureRows(t *testing.T) []string {
	t.Helper()
	data, err := os.ReadFile("testdata/corevent.txt")
	if err != nil {
		t.Fatal(err)
	}
	return strings.Split(strings.TrimSuffix(string(data), "\n"), "\n")
}

func eventField(row string, start, length int, value string) string {
	data := []byte(row)
	copy(data[start-1:start-1+length], strings.Repeat(" ", length))
	copy(data[start-1:start-1+length], value)
	return string(data)
}

func eventTempFile(t *testing.T, data string) string {
	t.Helper()
	path := filepath.Join(t.TempDir(), "corevent.txt")
	if err := os.WriteFile(path, []byte(data), 0600); err != nil {
		t.Fatal(err)
	}
	return path
}

func TestClassifyEvent(t *testing.T) {
	for _, tc := range []struct {
		desc string
		name bool
		want proxy.SignalType
		ok   bool
	}{
		{"CHANGE OF REGISTERED AGENT", false, proxy.RegisteredAgentChange, true},
		{"CHANGE OF REG. AGENT", false, proxy.RegisteredAgentChange, true},
		{"MERGER", false, proxy.EntityChange, true},
		{"CONVERSION", false, proxy.EntityChange, true},
		{"NAME CHANGE", false, proxy.EntityChange, true},
		{"OFFICER CHANGE", false, proxy.OfficerChange, true},
		{"DIRECTOR CHANGE", false, proxy.OfficerChange, true},
		{"MANAGER CHANGE", false, proxy.OfficerChange, true},
		{"MEMBER CHANGE", false, proxy.OfficerChange, true},
		{"AMENDED ANNUAL REPORT", false, "", false},
		{"AMENDMENT", true, proxy.EntityChange, true},
		{"REGISTERED AGENT MERGER OFFICER", true, proxy.EntityChange, true},
		{"REGISTERED AGENT MERGER OFFICER", false, proxy.RegisteredAgentChange, true},
		{"MERGER OFFICER", false, proxy.EntityChange, true},
		{"change of registered agent", false, proxy.RegisteredAgentChange, true},
	} {
		t.Run(fmt.Sprintf("%s/name=%t", tc.desc, tc.name), func(t *testing.T) {
			got, ok := classifyEvent(tc.desc, tc.name)
			if got != tc.want || ok != tc.ok {
				t.Fatalf("classifyEvent = %q, %t; want %q, %t", got, ok, tc.want, tc.ok)
			}
		})
	}
}

func TestLoadSunbizEvents(t *testing.T) {
	pool := testdb.New(t)
	ctx := context.Background()
	stats, err := LoadSunbizEvents(ctx, pool, "testdata/corevent.txt", "snapshots/events", sunbizLoadTime)
	wantRefs := []string{"L26000034567", "P26000012345", "P26000023456"}
	if err != nil || stats.Read != 4 || stats.Signals != 3 || stats.Unmapped != 1 ||
		!reflect.DeepEqual(stats.TopUnmapped, []string{"ANNUAL REPORT (1)"}) || !reflect.DeepEqual(stats.SignalRefs, wantRefs) {
		t.Fatalf("load = %+v, %v", stats, err)
	}
	for _, tc := range []struct {
		ref, seq, code, desc, date string
		kind                       proxy.SignalType
		line                       int
	}{
		{"P26000012345", "00001", "RA", "CHANGE OF REGISTERED AGENT", "2026-04-10", proxy.RegisteredAgentChange, 1},
		{"P26000023456", "00002", "OFF", "CHANGE OF MANAGER", "2026-03-01", proxy.OfficerChange, 2},
		{"L26000034567", "00003", "MERG", "MERGER", "2026-02-15", proxy.EntityChange, 3},
	} {
		var observed time.Time
		var citation quality.Citation
		if err := pool.QueryRow(ctx, `SELECT observed_at, citation FROM registry_signals
			WHERE source_system = 'fl_sunbiz' AND external_ref = $1 AND signal_type = $2`, tc.ref, tc.kind).Scan(&observed, &citation); err != nil {
			t.Fatal(err)
		}
		if observed.UTC().Format("2006-01-02") != tc.date || citation.TableName != "corevent" || citation.RowKey != tc.ref ||
			citation.FieldName != "event" || citation.SourceRef != "corevent.txt" || citation.SourceSystem != "fl_sunbiz" ||
			citation.PageOrSpan != fmt.Sprintf("line %d", tc.line) || !citation.RetrievedAt.Equal(sunbizLoadTime) ||
			citation.Origin != quality.RawSource || citation.Status != quality.VerifiedFiling || citation.ExportPolicy != quality.Allow {
			t.Fatalf("signal = %v, %+v", observed, citation)
		}
		if err := citation.Validate(); err != nil {
			t.Fatal(err)
		}
		var value map[string]string
		if err := json.Unmarshal(citation.Value, &value); err != nil || value["seq"] != tc.seq || value["code"] != tc.code || value["description"] != tc.desc {
			t.Fatalf("event value = %s, %v", citation.Value, err)
		}
	}
	var entities int
	if err := pool.QueryRow(ctx, `SELECT count(*) FROM registry_entities`).Scan(&entities); err != nil || entities != 0 {
		t.Fatalf("registry entities = %d, %v", entities, err)
	}
}

func TestEventsDedupe(t *testing.T) {
	pool := testdb.New(t)
	ctx := context.Background()
	if _, err := LoadSunbizEvents(ctx, pool, "testdata/corevent.txt", "snapshot", sunbizLoadTime); err != nil {
		t.Fatal(err)
	}
	stats, err := LoadSunbizEvents(ctx, pool, "testdata/corevent.txt", "different-snapshot", sunbizLoadTime.Add(time.Hour))
	if err != nil || stats.Signals != 0 || len(stats.SignalRefs) != 0 || stats.Read != 4 || stats.Unmapped != 1 {
		t.Fatalf("repeat = %+v, %v", stats, err)
	}
	rows := eventFixtureRows(t)
	// Same document, type and date: only a different sequence makes a new event.
	row := eventField(rows[0], 13, 5, "00005")
	path := eventTempFile(t, rows[0]+"\n"+row+"\n"+row+"\n")
	stats, err = LoadSunbizEvents(ctx, pool, path, "next", sunbizLoadTime)
	if err != nil || stats.Signals != 1 || !reflect.DeepEqual(stats.SignalRefs, []string{"P26000012345"}) {
		t.Fatalf("distinct sequence = %+v, %v", stats, err)
	}
	var count int
	if err := pool.QueryRow(ctx, `SELECT count(*) FROM registry_signals`).Scan(&count); err != nil || count != 4 {
		t.Fatalf("signal count = %d, %v", count, err)
	}
}

func TestLoadSunbizEventsRejectsMalformed(t *testing.T) {
	row := eventFixtureRows(t)[0]
	for _, size := range []int{0, 400, 661} {
		t.Run(fmt.Sprintf("%d bytes", size), func(t *testing.T) {
			pool := testdb.New(t)
			path := eventTempFile(t, row+"\r\n"+strings.Repeat(" ", size)+"\r\n")
			stats, err := LoadSunbizEvents(context.Background(), pool, path, "snapshot", sunbizLoadTime)
			if err == nil || !strings.Contains(err.Error(), "line 2") || stats.Signals != 0 || len(stats.SignalRefs) != 0 {
				t.Fatalf("malformed = %+v, %v", stats, err)
			}
			var count int
			if err := pool.QueryRow(context.Background(), `SELECT count(*) FROM registry_signals`).Scan(&count); err != nil || count != 0 {
				t.Fatalf("discarded batch signals = %d, %v", count, err)
			}
		})
	}
}

func TestLoadSunbizEventsFieldsAndLines(t *testing.T) {
	pool := testdb.New(t)
	rows := eventFixtureRows(t)
	// Sanitize each byte slice after extracting it, preserving fixed-width offsets.
	row := eventField(rows[0], 1, 12, "p26000012345\xff")
	row = eventField(row, 13, 5, "00\xff01")
	row = eventField(row, 18, 20, "R\xffA")
	row = eventField(row, 38, 40, "CHANGE OF REGISTERED AGENT\xff")
	row = eventField(row, 78, 8, "20260409")
	row = eventField(row, 86, 8, "INVALID\xff")
	nameChange := eventField(rows[2], 38, 40, "AMENDMENT")
	nameChange = eventField(nameChange, 413, 1, "Y")
	// Long records and the final missing newline must retain physical line numbers.
	path := eventTempFile(t, row+strings.Repeat("x", 70000)+"\r\n"+rows[3]+"\r\n"+nameChange)
	stats, err := LoadSunbizEvents(context.Background(), pool, path, "snapshot", sunbizLoadTime)
	if err != nil || stats.Read != 3 || stats.Signals != 2 || stats.Unmapped != 1 {
		t.Fatalf("fields = %+v, %v", stats, err)
	}
	var citation quality.Citation
	var date time.Time
	if err := pool.QueryRow(context.Background(), `SELECT citation, observed_at FROM registry_signals
		WHERE external_ref = 'P26000012345'`).Scan(&citation, &date); err != nil {
		t.Fatal(err)
	}
	var value map[string]string
	if err := json.Unmarshal(citation.Value, &value); err != nil || value["seq"] != "0001" || value["code"] != "RA" ||
		value["description"] != "CHANGE OF REGISTERED AGENT" || citation.PageOrSpan != "line 1" || date.UTC().Format("2006-01-02") != "2026-04-09" {
		t.Fatalf("sanitized citation/date = %+v, %v, %v", citation, date, err)
	}
	if err := pool.QueryRow(context.Background(), `SELECT citation FROM registry_signals
		WHERE external_ref = 'L26000034567' AND signal_type = 'entity_change'`).Scan(&citation); err != nil || citation.PageOrSpan != "line 3" {
		t.Fatalf("name-change citation = %+v, %v", citation, err)
	}
}

func TestLoadSunbizEventsUnmapped(t *testing.T) {
	pool := testdb.New(t)
	rows := eventFixtureRows(t)
	// Classified rows with neither date must be unmapped instead of using load time.
	missing := eventField(rows[0], 78, 8, "00000000")
	missing = eventField(missing, 86, 8, "20260230")
	data := []string{missing}
	for i := 20; i >= 0; i-- {
		data = append(data, eventField(rows[3], 38, 40, fmt.Sprintf("UNMAPPED %02d", i)))
	}
	data = append(data, eventField(rows[3], 38, 40, "unmapped 20"))
	stats, err := LoadSunbizEvents(context.Background(), pool, eventTempFile(t, strings.Join(data, "\n")), "snapshot", sunbizLoadTime)
	want := []string{"UNMAPPED 20 (2)", "CHANGE OF REGISTERED AGENT (1)"}
	for i := 0; i < 18; i++ {
		want = append(want, fmt.Sprintf("UNMAPPED %02d (1)", i))
	}
	if err != nil || stats.Read != 23 || stats.Unmapped != 23 || stats.Signals != 0 || len(stats.SignalRefs) != 0 ||
		!reflect.DeepEqual(stats.TopUnmapped, want) {
		t.Fatalf("unmapped = %+v, %v; want %v", stats, err, want)
	}
}

func TestLoadSunbizEventsBatchRecovery(t *testing.T) {
	pool := testdb.New(t)
	ctx := context.Background()
	rows := eventFixtureRows(t)
	data := strings.Repeat(rows[0]+"\n", 5000) + rows[2] + "\n"
	path := eventTempFile(t, data+"short\n")
	stats, err := LoadSunbizEvents(ctx, pool, path, "snapshot", sunbizLoadTime)
	if err == nil || !strings.Contains(err.Error(), "line 5002") || stats.Read != 5001 || stats.Signals != 1 ||
		!reflect.DeepEqual(stats.SignalRefs, []string{"P26000012345"}) {
		t.Fatalf("partial load = %+v, %v", stats, err)
	}
	if err := os.WriteFile(path, []byte(data), 0600); err != nil {
		t.Fatal(err)
	}
	stats, err = LoadSunbizEvents(ctx, pool, path, "snapshot", sunbizLoadTime)
	if err != nil || stats.Read != 5001 || stats.Signals != 1 || !reflect.DeepEqual(stats.SignalRefs, []string{"L26000034567"}) {
		t.Fatalf("recovery = %+v, %v", stats, err)
	}
	var count int
	if err := pool.QueryRow(ctx, `SELECT count(*) FROM registry_signals`).Scan(&count); err != nil || count != 2 {
		t.Fatalf("committed signals = %d, %v", count, err)
	}
}

func TestLoadSunbizEventsTransactionRollback(t *testing.T) {
	pool := testdb.New(t)
	ctx := context.Background()
	if _, err := pool.Exec(ctx, `ALTER TABLE registry_signals ADD CONSTRAINT reject_event
		CHECK (external_ref <> 'P26000023456')`); err != nil {
		t.Fatal(err)
	}
	stats, err := LoadSunbizEvents(ctx, pool, "testdata/corevent.txt", "snapshot", sunbizLoadTime)
	if err == nil || stats.Signals != 0 || len(stats.SignalRefs) != 0 {
		t.Fatalf("failed transaction = %+v, %v", stats, err)
	}
	var count int
	if err := pool.QueryRow(ctx, `SELECT count(*) FROM registry_signals`).Scan(&count); err != nil || count != 0 {
		t.Fatalf("rolled back signals = %d, %v", count, err)
	}
}
