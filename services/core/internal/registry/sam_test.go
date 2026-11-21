package registry

import (
	"archive/zip"
	"context"
	"encoding/json"
	"errors"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"

	"rowan.ai/rowan/services/core/internal/quality"
	"rowan.ai/rowan/services/core/internal/testdb"
)

var samLoadTime = time.Date(2026, 11, 20, 12, 0, 0, 0, time.UTC)

func samFixture(t *testing.T) string {
	t.Helper()
	data, err := os.ReadFile("testdata/sam_public_v2.dat")
	if err != nil {
		t.Fatal(err)
	}
	return string(data)
}

func samTempFile(t *testing.T, data string) string {
	t.Helper()
	path := filepath.Join(t.TempDir(), "sam.dat")
	if err := os.WriteFile(path, []byte(data), 0600); err != nil {
		t.Fatal(err)
	}
	return path
}

func assertSAMFixture(t *testing.T, pool *pgxpool.Pool, path string) {
	t.Helper()
	ctx := context.Background()
	stats, err := LoadSAM(ctx, pool, path, "snapshots/sam", samLoadTime)
	if err != nil || stats.Read != 4 || stats.Inserted != 3 || stats.Skipped != 1 || stats.Signals != 0 {
		t.Fatalf("load = %+v, %v", stats, err)
	}
	var e Entity
	var ids map[string]string
	var file, snapshot string
	var updated, loaded time.Time
	err = pool.QueryRow(ctx, `SELECT entity, ids, file_name, snapshot_ref, source_updated_at, loaded_at
		FROM registry_entities WHERE source_system = 'sam' AND external_ref = 'ABCDEF123456'`).Scan(
		&e, &ids, &file, &snapshot, &updated, &loaded,
	)
	if err != nil {
		t.Fatal(err)
	}
	wantIDs := map[string]string{"uei": "ABCDEF123456", "cage": "1A2B3"}
	if !reflect.DeepEqual(ids, wantIDs) || !reflect.DeepEqual(e.Facts.IDs, wantIDs) ||
		e.SourceSystem != "sam" || e.ExternalRef != "ABCDEF123456" || e.Line != 2 ||
		e.Facts.LegalName != "ACME HVAC LLC" || e.Facts.DBA != "" ||
		e.Facts.Address1 != "100 MAIN ST" || e.Facts.State != "FL" || e.Facts.Zip != "33602" ||
		e.Facts.Domain != "acmehvac.example" || e.Facts.Principal != "JOHN SMITH" {
		t.Fatalf("entity = %+v, ids = %v", e, ids)
	}
	for key, want := range map[string]string{
		"primary_naics": "238220", "extract_code": "", "registration_date": "",
		"entity_start_date": "", "country": "",
	} {
		if e.Extra[key] != want {
			t.Errorf("Extra[%q] = %v, want %s", key, e.Extra[key], want)
		}
	}
	if !reflect.DeepEqual(e.Extra["naics"], []any{}) {
		t.Errorf("NAICS = %v", e.Extra["naics"])
	}
	wantUpdated := time.Date(2026, 11, 1, 0, 0, 0, 0, time.UTC)
	if !updated.Equal(wantUpdated) || e.SourceUpdatedAt == nil || !e.SourceUpdatedAt.Equal(wantUpdated) ||
		file != filepath.Base(path) || snapshot != "snapshots/sam" || !loaded.Equal(samLoadTime) {
		t.Fatalf("metadata: updated=%v, entity=%+v, file=%s, snapshot=%s, loaded=%v", updated, e, file, snapshot, loaded)
	}
	// The loader retains the raw filing value and physical line for later citations.
	if e.Raw["legal_business_name"] != "ACME HVAC LLC" || e.Raw["last_update_date"] != "20261101" {
		t.Fatalf("raw values = %v", e.Raw)
	}
	citation := Cite(LoadMeta{SourceSystem: "sam", FileName: file, TableName: "sam_public_v2", LoadedAt: loaded},
		e, "legal_name", e.Raw["legal_business_name"])
	if err := citation.Validate(); err != nil {
		t.Fatal(err)
	}
	if citation.Origin != quality.RawSource || citation.Status != quality.VerifiedFiling || citation.ExportPolicy != quality.Allow ||
		citation.SourceSystem != "sam" || citation.SourceRef != file || citation.TableName != "sam_public_v2" ||
		citation.RowKey != "ABCDEF123456" || citation.PageOrSpan != "line 2" || !citation.RetrievedAt.Equal(samLoadTime) {
		t.Fatalf("citation = %+v", citation)
	}
	var count int
	if err := pool.QueryRow(ctx, `SELECT count(*) FROM registry_entities
		WHERE external_ref = 'HIDDEN123456' OR entity->'raw'->>'no_public_display' = 'NPDY'`).Scan(&count); err != nil || count != 0 {
		t.Fatalf("NPDY rows = %d, %v", count, err)
	}
	stats, err = LoadSAM(ctx, pool, path, "snapshots/sam", samLoadTime.Add(time.Hour))
	if err != nil || stats.Read != 4 || stats.Unchanged != 3 || stats.Skipped != 1 ||
		stats.Inserted != 0 || stats.Updated != 0 || stats.Signals != 0 {
		t.Fatalf("repeat load = %+v, %v", stats, err)
	}
}

func TestLoadSAMFixture(t *testing.T) {
	assertSAMFixture(t, testdb.New(t), "testdata/sam_public_v2.dat")
}

func TestLoadSAMZip(t *testing.T) {
	path := filepath.Join(t.TempDir(), "sam_public_v2.zip")
	file, err := os.Create(path)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = file.Close() })
	writer := zip.NewWriter(file)
	// Ignore non-DAT entries and select the first DAT, including nested paths.
	for _, entry := range []struct{ name, data string }{
		{"README.txt", "not an extract"}, {"extract/sam_public_v2.dat", samFixture(t)}, {"second.dat", "invalid"},
	} {
		w, err := writer.Create(entry.name)
		if err != nil {
			t.Fatal(err)
		}
		if _, err := w.Write([]byte(entry.data)); err != nil {
			t.Fatal(err)
		}
	}
	if err := writer.Close(); err != nil {
		t.Fatal(err)
	}
	if err := file.Close(); err != nil {
		t.Fatal(err)
	}
	assertSAMFixture(t, testdb.New(t), path)
}

func TestLoadSAMRejectsMalformed(t *testing.T) {
	lines := strings.Split(strings.TrimSpace(samFixture(t)), "\n")
	for _, tc := range []struct {
		name, data, line string
		unknown          bool
	}{
		{"missing BOF", strings.Join(lines[1:], "\n"), "line 1", true},
		{"empty file", "", "line 1", true},
		{"short row", lines[0] + "\n" + lines[1] + "\n" + strings.Repeat("|", 49) + "\nEOF\n", "line 3", false},
		{"wrong delimiter", strings.ReplaceAll(samFixture(t), "|", ","), "line 2", true},
		{"missing end field", strings.Replace(samFixture(t), "!end", "", 1), "line 2", true},
		{"missing EOF", strings.Join(lines[:5], "\n"), "line 6", true},
		{"data after EOF", samFixture(t) + lines[1] + "\n", "line 7", true},
		{"invalid update date", strings.Replace(samFixture(t), "20261101", "20260230", 1), "line 2", false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			pool := testdb.New(t)
			stats, err := LoadSAM(context.Background(), pool, samTempFile(t, tc.data), "snapshot", samLoadTime)
			if err == nil || !strings.Contains(err.Error(), tc.line) || (tc.unknown && !errors.Is(err, ErrUnknownFormat)) {
				t.Fatalf("load = %+v, %v; want error at %s, unknown=%v", stats, err, tc.line, tc.unknown)
			}
			var count int
			if err := pool.QueryRow(context.Background(), "SELECT count(*) FROM registry_entities").Scan(&count); err != nil || count != 0 {
				t.Fatalf("discarded batch rows = %d, %v", count, err)
			}
			if stats.Inserted != 0 || stats.Updated != 0 {
				t.Fatalf("discarded batch stats = %+v", stats)
			}
		})
	}
}

func TestLoadSAMTrimsFields(t *testing.T) {
	pool := testdb.New(t)
	lines := strings.Split(strings.TrimSpace(samFixture(t)), "\n")
	fields := strings.Split(lines[1], "|")
	fields[0], fields[3] = " abcdef123456 ", " "
	fields[11], fields[19] = " CAFÉ HVAC LLC ", " 33602-1234 "
	fields[26], fields[34] = " https://www.ACMEHVAC.example:443/about?q=1 ", " 238220 ~ 001234 "
	fields[5], fields[7], fields[12], fields[21], fields[24] = "A", "20100401", "CAFÉ HVAC", "USA", "20090315"
	for i := range fields {
		fields[i] = " " + fields[i] + " "
	}
	path := samTempFile(t, lines[0]+"\n"+strings.Join(fields, "|")+"\nEOF\n")
	stats, err := LoadSAM(context.Background(), pool, path, "snapshot", samLoadTime)
	if err != nil || stats.Read != 1 || stats.Inserted != 1 {
		t.Fatalf("load = %+v, %v", stats, err)
	}
	var e Entity
	if err := pool.QueryRow(context.Background(), "SELECT entity FROM registry_entities").Scan(&e); err != nil {
		t.Fatal(err)
	}
	if e.ExternalRef != "ABCDEF123456" || !reflect.DeepEqual(e.Facts.IDs, map[string]string{"uei": "ABCDEF123456"}) ||
		e.Facts.LegalName != "CAFÉ HVAC LLC" || e.Facts.DBA != "CAFÉ HVAC" || e.Facts.Zip != "33602" || e.Facts.Domain != "acmehvac.example" ||
		e.Facts.Principal != "JOHN SMITH" || !reflect.DeepEqual(e.Extra["naics"], []any{"238220", "001234"}) {
		t.Fatalf("normalized entity = %+v", e)
	}
	for key, want := range map[string]string{
		"extract_code": "A", "registration_date": "20100401", "entity_start_date": "20090315", "country": "USA",
	} {
		if e.Extra[key] != want {
			t.Errorf("Extra[%q] = %v, want %s", key, e.Extra[key], want)
		}
	}
	for field, value := range e.Raw {
		if value != strings.TrimSpace(value) {
			t.Errorf("untrimmed raw field %s = %q", field, value)
		}
	}
}

func TestLoadSAMLongUTF8Line(t *testing.T) {
	pool := testdb.New(t)
	lines := strings.Split(strings.TrimSpace(samFixture(t)), "\n")
	fields := strings.Split(lines[1], "|")
	fields[11] = "CAFÉ " + strings.Repeat("é", 40000)
	stats, err := LoadSAM(context.Background(), pool,
		samTempFile(t, lines[0]+"\n"+strings.Join(fields, "|")+"\nEOF\n"), "snapshot", samLoadTime)
	if err != nil || stats.Inserted != 1 {
		t.Fatalf("long UTF-8 row = %+v, %v", stats, err)
	}
	var name string
	if err := pool.QueryRow(context.Background(), "SELECT legal_name FROM registry_entities").Scan(&name); err != nil || name != fields[11] {
		t.Fatalf("UTF-8 name preserved = %t, %v", name == fields[11], err)
	}
}

func TestLoadSAMFieldAndLineLimits(t *testing.T) {
	lines := strings.Split(strings.TrimSpace(samFixture(t)), "\n")
	base := strings.Split(lines[1], "|")
	for _, tc := range []struct {
		name string
		row  string
		fail bool
	}{
		{"minimum fields", strings.Join(append(append([]string{}, base[:118]...), "!end"), "|"), false},
		{"one MiB", samPaddedRow(base, 1<<20), false},
		{"over one MiB", samPaddedRow(base, (1<<20)+1), true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			pool := testdb.New(t)
			path := samTempFile(t, lines[0]+"\r\n"+tc.row+"\r\nEOF\r\n")
			stats, err := LoadSAM(context.Background(), pool, path, "snapshot", samLoadTime)
			if tc.fail {
				if err == nil || !strings.Contains(err.Error(), "line 2") || stats.Inserted != 0 {
					t.Fatalf("oversized line = %+v, %v", stats, err)
				}
			} else if err != nil || stats.Read != 1 || stats.Inserted != 1 {
				t.Fatalf("valid boundary = %+v, %v", stats, err)
			}
		})
	}
}

func samPaddedRow(base []string, size int) string {
	fields := append([]string{}, base...)
	// Pad an unmapped column without changing the projected identity facts.
	fields[1] = strings.Repeat("x", size-len(strings.Join(fields, "|")))
	return strings.Join(fields, "|")
}

func TestLoadSAMSkippedBeforeError(t *testing.T) {
	pool := testdb.New(t)
	lines := strings.Split(strings.TrimSpace(samFixture(t)), "\n")
	data := strings.Join([]string{lines[0], lines[1], lines[3], "bad|row", "EOF"}, "\n")
	stats, err := LoadSAM(context.Background(), pool, samTempFile(t, data), "snapshot", samLoadTime)
	if err == nil || !strings.Contains(err.Error(), "line 4") || stats.Read != 2 || stats.Skipped != 1 || stats.Inserted != 0 {
		t.Fatalf("skipped before error = %+v, %v", stats, err)
	}
}

func TestLoadSAMEmptyZip(t *testing.T) {
	path := filepath.Join(t.TempDir(), "empty.zip")
	file, err := os.Create(path)
	if err != nil {
		t.Fatal(err)
	}
	writer := zip.NewWriter(file)
	if err := writer.Close(); err != nil {
		t.Fatal(err)
	}
	if err := file.Close(); err != nil {
		t.Fatal(err)
	}
	if _, err := LoadSAM(context.Background(), nil, path, "snapshot", samLoadTime); !errors.Is(err, ErrUnknownFormat) {
		t.Fatalf("ZIP without DAT = %v", err)
	}
}

func TestLoadSAMCanceled(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	stats, err := LoadSAM(ctx, nil, "testdata/sam_public_v2.dat", "snapshot", samLoadTime)
	if !errors.Is(err, context.Canceled) || stats.Read != 0 || stats.Inserted != 0 {
		t.Fatalf("canceled load = %+v, %v", stats, err)
	}
}

func TestLoadSAMRedactsEntityURL(t *testing.T) {
	pool := testdb.New(t)
	data := strings.Replace(samFixture(t), "https://acmehvac.example",
		"https://acmehvac.example/about?api_key=SECRET&key=SECRET&token=SECRET", 1)
	stats, err := LoadSAM(context.Background(), pool, samTempFile(t, data), "snapshot", samLoadTime)
	if err != nil || stats.Inserted != 3 {
		t.Fatalf("load = %+v, %v", stats, err)
	}
	var e Entity
	if err := pool.QueryRow(context.Background(), `SELECT entity FROM registry_entities WHERE external_ref = 'ABCDEF123456'`).Scan(&e); err != nil {
		t.Fatal(err)
	}
	encoded, err := json.Marshal(e)
	if err != nil {
		t.Fatal(err)
	}
	if strings.Contains(string(encoded), "SECRET") ||
		e.Raw["entity_url"] != "https://acmehvac.example/about?api_key=REDACTED&key=REDACTED&token=REDACTED" {
		t.Fatal("entity URL credentials were not redacted")
	}
}
