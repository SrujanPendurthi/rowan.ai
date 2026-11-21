package registry

import (
	"context"
	"encoding/csv"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"testing"
	"time"
	"unicode/utf8"

	"github.com/jackc/pgx/v5/pgxpool"

	"rowan.ai/rowan/services/core/internal/quality"
	"rowan.ai/rowan/services/core/internal/testdb"
)

var dbprLoadTime = time.Date(2026, 11, 20, 12, 0, 0, 0, time.UTC)

func dbprFixture(t *testing.T, name string) string {
	t.Helper()
	data, err := os.ReadFile(filepath.Join("testdata", name))
	if err != nil {
		t.Fatal(err)
	}
	return string(data)
}

func dbprTempFile(t *testing.T, data string) string {
	t.Helper()
	path := filepath.Join(t.TempDir(), "licenses.csv")
	if err := os.WriteFile(path, []byte(data), 0600); err != nil {
		t.Fatal(err)
	}
	return path
}

func dbprStored(t *testing.T, pool *pgxpool.Pool, ref string) Entity {
	t.Helper()
	var e Entity
	if err := pool.QueryRow(context.Background(), `SELECT entity FROM registry_entities
		WHERE source_system = 'fl_dbpr' AND external_ref = $1`, ref).Scan(&e); err != nil {
		t.Fatal(err)
	}
	return e
}

func TestLoadDBPRFixtures(t *testing.T) {
	pool := testdb.New(t)
	ctx := context.Background()
	for _, name := range []string{"dbpr_construction.csv", "dbpr_electrical.csv"} {
		rows, err := csv.NewReader(strings.NewReader(dbprFixture(t, name))).ReadAll()
		if err != nil || len(rows) != 2 {
			t.Fatalf("fixture %s = %v, %v", name, rows, err)
		}
		for _, row := range rows {
			if len(row) != 22 {
				t.Fatalf("fixture %s has %d fields, want 22", name, len(row))
			}
		}
		stats, err := LoadDBPR(ctx, pool, filepath.Join("testdata", name), "snapshots/"+name, dbprLoadTime)
		if err != nil || stats.Read != 2 || stats.Inserted != 2 || stats.Updated != 0 || stats.Skipped != 0 || stats.Signals != 0 {
			t.Fatalf("%s load = %+v, %v", name, stats, err)
		}
	}
	e := dbprStored(t, pool, "CGC1520001")
	if e.SourceSystem != "fl_dbpr" || e.ExternalRef != "CGC1520001" || e.Line != 1 ||
		e.Facts.LegalName != "ACME HVAC LLC" || e.Facts.DBA != "ACME HVAC LLC" || e.Facts.Principal != "JOHN SMITH" ||
		e.Facts.Address1 != "100 MAIN ST" || e.Facts.State != "FL" || e.Facts.Zip != "33602" ||
		!reflect.DeepEqual(e.Facts.IDs, map[string]string{"license:fl": "CGC1520001"}) {
		t.Fatalf("anchor = %+v", e)
	}
	for key, want := range map[string]string{
		"board": "06", "occupation_code": "0601", "class_code": "CGC", "primary_status": "Current",
		"secondary_status": "Active", "original_date": "04/15/2019", "expiration_date": "08/31/2026", "county_code": "29",
	} {
		if e.Extra[key] != want {
			t.Errorf("Extra[%q] = %v, want %q", key, e.Extra[key], want)
		}
	}
	if e.Raw["city"] != "TAMPA" || e.SourceUpdatedAt == nil || e.SourceUpdatedAt.Format("2006-01-02") != "2019-04-15" {
		t.Fatalf("raw/date = %+v", e)
	}
	var names []string
	var file, snapshot string
	var loaded, updated time.Time
	if err := pool.QueryRow(ctx, `SELECT names, file_name, snapshot_ref, loaded_at, source_updated_at
		FROM registry_entities WHERE source_system = 'fl_dbpr' AND external_ref = $1`, e.ExternalRef).
		Scan(&names, &file, &snapshot, &loaded, &updated); err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(names, []string{"acme hvac", "john smith"}) || file != "dbpr_construction.csv" ||
		snapshot != "snapshots/dbpr_construction.csv" || !loaded.Equal(dbprLoadTime) || !updated.Equal(*e.SourceUpdatedAt) {
		t.Fatalf("projection = %v, %s, %s, %v, %v", names, file, snapshot, loaded, updated)
	}
	for _, tc := range []struct{ ref, name, principal, zip string }{
		{"CBC1260002", "BAY BUILDERS LLC", "JONES, ALICE", "33603"},
		{"EC13001234", "GULF ELECTRIC INC", "ROBERT LEE", "33101"},
		{"ER13005678", "SMITH JANE M", "SMITH JANE M", "32801"},
	} {
		got := dbprStored(t, pool, tc.ref)
		if got.Facts.LegalName != tc.name || got.Facts.Principal != tc.principal || got.Facts.Zip != tc.zip {
			t.Errorf("entity %s = %+v", tc.ref, got)
		}
		if tc.ref == "ER13005678" && (got.Facts.DBA != "" || got.SourceUpdatedAt != nil) {
			t.Errorf("empty DBA/invalid date = %+v", got)
		}
	}
	citation := Cite(LoadMeta{SourceSystem: "fl_dbpr", TableName: "dbpr_license", FileName: file, LoadedAt: loaded}, e, "legal_name", e.Facts.LegalName)
	if err := citation.Validate(); err != nil {
		t.Fatal(err)
	}
	if citation.Origin != quality.RawSource || citation.Status != quality.VerifiedFiling || citation.ExportPolicy != quality.Allow ||
		citation.SourceSystem != "fl_dbpr" || citation.SourceRef != file || citation.TableName != "dbpr_license" ||
		citation.RowKey != e.ExternalRef || citation.PageOrSpan != "line 1" || !citation.RetrievedAt.Equal(dbprLoadTime) {
		t.Fatalf("citation = %+v", citation)
	}
}

func TestLoadDBPRDates(t *testing.T) {
	want := time.Date(2019, 4, 15, 0, 0, 0, 0, time.UTC)
	for _, input := range []string{"04/15/2019", "20190415", " 04/15/2019 "} {
		if got := parseDBPRDate(input); got == nil || !got.Equal(want) {
			t.Errorf("parseDBPRDate(%q) = %v, want %v", input, got, want)
		}
	}
	for _, input := range []string{"garbage", "", " ", "00000000", "02/30/2019", "20190230"} {
		if got := parseDBPRDate(input); got != nil {
			t.Errorf("parseDBPRDate(%q) = %v, want nil", input, got)
		}
	}
}

func TestLoadDBPRRejectsMalformed(t *testing.T) {
	for _, count := range []int{12, 20} {
		t.Run(fmt.Sprintf("%d fields", count), func(t *testing.T) {
			pool := testdb.New(t)
			row := strings.SplitN(dbprFixture(t, "dbpr_construction.csv"), "\n", 2)[0]
			stats, err := LoadDBPR(context.Background(), pool, dbprTempFile(t, row+"\n"+strings.Repeat("\"x\",", count-1)+"\"x\"\n"), "snapshot", dbprLoadTime)
			if err == nil || !strings.Contains(err.Error(), "line 2") || stats.Read != 1 || stats.Inserted != 0 {
				t.Fatalf("malformed load = %+v, %v", stats, err)
			}
			var stored int
			if err := pool.QueryRow(context.Background(), `SELECT count(*) FROM registry_entities`).Scan(&stored); err != nil || stored != 0 {
				t.Fatalf("discarded batch = %d, %v", stored, err)
			}
		})
	}
}

func TestLoadDBPRPhysicalLines(t *testing.T) {
	pool := testdb.New(t)
	data := dbprFixture(t, "dbpr_construction.csv")
	data = strings.Replace(data, `"JOHN SMITH"`, "\"JOHN\nSMITH\"", 1)
	// Blank physical lines and all-empty records do not count toward stats.
	data = "\r\n  \r\n\"\",\" \"\r\n" + strings.ReplaceAll(data, "\n", "\r\n")
	data = strings.TrimSuffix(data, "\r\n")
	stats, err := LoadDBPR(context.Background(), pool, dbprTempFile(t, data), "snapshot", dbprLoadTime)
	if err != nil || stats.Read != 2 || stats.Inserted != 2 || stats.Skipped != 0 {
		t.Fatalf("physical lines = %+v, %v", stats, err)
	}
	if e := dbprStored(t, pool, "CGC1520001"); e.Line != 4 || e.Facts.Principal != "JOHN\nSMITH" {
		t.Fatalf("multiline entity = %+v", e)
	}
	if e := dbprStored(t, pool, "CBC1260002"); e.Line != 6 {
		t.Fatalf("second entity line = %d, want 6", e.Line)
	}
	// A malformed row after a multiline record reports its physical start line.
	bad := strings.SplitN(data, `"06","0602"`, 2)[0] + `"short","row"`
	if _, err := LoadDBPR(context.Background(), nil, dbprTempFile(t, bad), "snapshot", dbprLoadTime); err == nil || !strings.Contains(err.Error(), "line 6") {
		t.Fatalf("multiline error = %v", err)
	}
}

func TestLoadDBPRFieldsAndEncoding(t *testing.T) {
	pool := testdb.New(t)
	rows := strings.Split(strings.TrimSpace(dbprFixture(t, "dbpr_construction.csv")), "\n")
	// Accept exactly 21 fields, padded lower-case IDs, stray bytes, and lazy quotes.
	rows[0] = strings.TrimSuffix(rows[0], `,""`)
	rows[0] = strings.Replace(rows[0], `"CGC1520001"`, `" cgc1520001 "`, 1)
	rows[0] = strings.Replace(rows[0], `"JOHN SMITH"`, "\" JOHN \xffSMITH \"", 1)
	rows[0] = strings.Replace(rows[0], `"100 MAIN ST"`, `"100 "MAIN" ST"`, 1)
	stats, err := LoadDBPR(context.Background(), pool, dbprTempFile(t, strings.Join(rows, "\n")), "snapshot", dbprLoadTime)
	if err != nil || stats.Inserted != 2 {
		t.Fatalf("encoding load = %+v, %v", stats, err)
	}
	e := dbprStored(t, pool, "CGC1520001")
	if e.Facts.IDs["license:fl"] != "CGC1520001" || e.Facts.Principal != "JOHN �SMITH" || e.Facts.Address1 != `100 "MAIN" ST` {
		t.Fatalf("cleaned entity = %+v", e)
	}
	for key, value := range e.Raw {
		if !utf8.ValidString(value) {
			t.Errorf("Raw[%q] is invalid UTF-8", key)
		}
	}
}

func TestLoadDBPRIdempotentAndNoSignals(t *testing.T) {
	pool := testdb.New(t)
	ctx := context.Background()
	path := "testdata/dbpr_construction.csv"
	if _, err := LoadDBPR(ctx, pool, path, "first", dbprLoadTime); err != nil {
		t.Fatal(err)
	}
	stats, err := LoadDBPR(ctx, pool, path, "second", dbprLoadTime.Add(time.Hour))
	if err != nil || stats.Read != 2 || stats.Unchanged != 2 || stats.Inserted != 0 || stats.Updated != 0 || stats.Signals != 0 {
		t.Fatalf("repeat = %+v, %v", stats, err)
	}
	data := strings.Replace(dbprFixture(t, "dbpr_construction.csv"), "JOHN SMITH", "ALEX SMITH", 1)
	stats, err = LoadDBPR(ctx, pool, dbprTempFile(t, data), "third", dbprLoadTime.Add(2*time.Hour))
	if err != nil || stats.Updated != 1 || stats.Unchanged != 1 || stats.Signals != 0 || len(stats.SignalRefs) != 0 {
		t.Fatalf("changed licensee = %+v, %v", stats, err)
	}
	var count int
	if err := pool.QueryRow(ctx, `SELECT count(*) FROM registry_signals`).Scan(&count); err != nil || count != 0 {
		t.Fatalf("signals = %d, %v", count, err)
	}
}

func TestLoadDBPRCanceled(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	stats, err := LoadDBPR(ctx, nil, "testdata/dbpr_construction.csv", "snapshot", dbprLoadTime)
	if !errors.Is(err, context.Canceled) || stats.Read != 0 || stats.Inserted != 0 {
		t.Fatalf("canceled load = %+v, %v", stats, err)
	}
}
