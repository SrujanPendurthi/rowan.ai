package registry

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"reflect"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"

	"rowan.ai/rowan/services/core/internal/db"
	"rowan.ai/rowan/services/core/internal/proxy"
	"rowan.ai/rowan/services/core/internal/quality"
	"rowan.ai/rowan/services/core/internal/resolver"
	"rowan.ai/rowan/services/core/internal/testdb"
)

func loadMeta() LoadMeta {
	return LoadMeta{
		SourceSystem: "fl_sunbiz", FileName: "sunbiz.csv", TableName: "corporations",
		SnapshotRef: "snapshots/sunbiz.csv", LoadedAt: time.Date(2026, 11, 20, 12, 0, 0, 0, time.UTC),
	}
}

func entities(n int) []Entity {
	rows := make([]Entity, n)
	for i := range rows {
		rows[i] = Entity{
			SourceSystem: "fl_sunbiz", ExternalRef: fmt.Sprintf("P%07d", i), Line: i + 2,
			Facts: resolver.RecordFacts{
				LegalName: fmt.Sprintf("Company %d LLC", i), State: "FL", Zip: "33101",
				Principal: "Old Principal", IDs: map[string]string{"filing:fl": fmt.Sprintf("P%07d", i)},
			},
			Extra: map[string]any{"status": "active"}, Raw: map[string]string{"status": "active"},
		}
	}
	return rows
}

func sliceNext(rows []Entity) func() (Entity, bool, error) {
	i := 0
	return func() (Entity, bool, error) {
		if i == len(rows) {
			return Entity{}, false, nil
		}
		e := rows[i]
		i++
		return e, true, nil
	}
}

func changeSignal(e Entity, meta LoadMeta) Signal {
	return Signal{
		SourceSystem: e.SourceSystem, ExternalRef: e.ExternalRef, Type: proxy.OfficerChange,
		ObservedAt: meta.LoadedAt, Citation: Cite(meta, e, "principal", e.Facts.Principal),
	}
}

func TestLoadInsertsThenNoops(t *testing.T) {
	pool := testdb.New(t)
	rows := entities(3)
	diff := func(old, new Entity, meta LoadMeta) []Signal {
		t.Fatal("diff called for a baseline or unchanged row")
		return nil
	}
	stats, err := Load(context.Background(), pool, loadMeta(), sliceNext(rows), diff)
	if err != nil || stats.Read != 3 || stats.Inserted != 3 {
		t.Fatalf("initial load = %+v, %v", stats, err)
	}
	stats, err = Load(context.Background(), pool, loadMeta(), sliceNext(rows), diff)
	if err != nil || stats.Read != 3 || stats.Unchanged != 3 || stats.Inserted != 0 || stats.Updated != 0 {
		t.Fatalf("repeat load = %+v, %v", stats, err)
	}
}

func TestLoadDiffOnChangeOnly(t *testing.T) {
	pool := testdb.New(t)
	ctx := context.Background()
	rows := entities(3)
	if _, err := Load(ctx, pool, loadMeta(), sliceNext(rows), nil); err != nil {
		t.Fatal(err)
	}
	rows[1].Facts.Principal = "New Principal"
	calls := 0
	diff := func(old, new Entity, meta LoadMeta) []Signal {
		calls++
		if old.ExternalRef != rows[1].ExternalRef || old.Facts.Principal != "Old Principal" || new.Facts.Principal != "New Principal" {
			t.Errorf("unexpected diff: old=%+v new=%+v", old, new)
		}
		return []Signal{changeSignal(new, meta)}
	}
	stats, err := Load(ctx, pool, loadMeta(), sliceNext(rows), diff)
	if err != nil || stats.Updated != 1 || stats.Unchanged != 2 || stats.Signals != 1 || calls != 1 {
		t.Fatalf("changed load = %+v, %v; calls=%d", stats, err, calls)
	}
	if !reflect.DeepEqual(stats.SignalRefs, []string{rows[1].ExternalRef}) {
		t.Fatalf("signal refs = %v", stats.SignalRefs)
	}
	var got Signal
	if err := pool.QueryRow(ctx, "SELECT source_system, external_ref, signal_type, observed_at, citation FROM registry_signals").Scan(
		&got.SourceSystem, &got.ExternalRef, &got.Type, &got.ObservedAt, &got.Citation,
	); err != nil {
		t.Fatal(err)
	}
	want := changeSignal(rows[1], loadMeta())
	if !got.ObservedAt.Equal(want.ObservedAt) {
		t.Fatalf("observed_at = %v, want %v", got.ObservedAt, want.ObservedAt)
	}
	got.ObservedAt = want.ObservedAt
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("stored signal = %+v", got)
	}
}

func TestLoadBaselineNoSignals(t *testing.T) {
	pool := testdb.New(t)
	stats, err := Load(context.Background(), pool, loadMeta(), sliceNext(entities(1)), func(old, new Entity, meta LoadMeta) []Signal {
		t.Fatal("diff called on baseline")
		return nil
	})
	if err != nil || stats.Inserted != 1 || stats.Signals != 0 || len(stats.SignalRefs) != 0 {
		t.Fatalf("baseline = %+v, %v", stats, err)
	}
}

func TestInsertSignalsDedupe(t *testing.T) {
	pool := testdb.New(t)
	signal := changeSignal(entities(1)[0], loadMeta())
	err := db.InTx(context.Background(), pool, func(tx pgx.Tx) error {
		for i, want := range []int{1, 0} {
			got, err := InsertSignals(context.Background(), tx, []Signal{signal, signal})
			if err != nil || got != want {
				t.Fatalf("insert %d = %d, %v; want %d", i, got, err, want)
			}
		}
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
}

func TestLoadBatches(t *testing.T) {
	pool := testdb.New(t)
	stats, err := Load(context.Background(), pool, loadMeta(), sliceNext(entities(12001)), nil)
	if err != nil || stats.Read != 12001 || stats.Inserted != 12001 {
		t.Fatalf("batch load = %+v, %v", stats, err)
	}
	var count int
	if err := pool.QueryRow(context.Background(), "SELECT count(*) FROM registry_entities").Scan(&count); err != nil {
		t.Fatal(err)
	}
	if count != 12001 {
		t.Fatalf("row count = %d, want 12001", count)
	}
}

func TestNormID(t *testing.T) {
	key, value := NormID(" Filing:FL ", " p0400012 ")
	if key != "filing:fl" || value != "P0400012" {
		t.Fatalf("NormID = (%q, %q)", key, value)
	}
}

func TestCite(t *testing.T) {
	e := entities(1)[0]
	meta := loadMeta()
	c := Cite(meta, e, "officers", []string{"Jane Doe", "John Doe"})
	if err := c.Validate(); err != nil {
		t.Fatal(err)
	}
	want := quality.Citation{
		FieldName: "officers", Value: json.RawMessage(`["Jane Doe","John Doe"]`),
		Origin: quality.RawSource, Status: quality.VerifiedFiling, ExportPolicy: quality.Allow,
		SourceSystem: meta.SourceSystem, SourceRef: meta.FileName, TableName: meta.TableName,
		RowKey: e.ExternalRef, PageOrSpan: "line 2", RetrievedAt: meta.LoadedAt,
	}
	if !reflect.DeepEqual(c, want) {
		t.Fatalf("citation = %+v, want %+v", c, want)
	}
}

func TestLoadContentHashAndProjections(t *testing.T) {
	pool := testdb.New(t)
	ctx := context.Background()
	e := entities(1)[0]
	e.Facts.DBA = "Company 0, Inc."
	updated := time.Date(2026, 11, 19, 0, 0, 0, 0, time.UTC)
	e.SourceUpdatedAt = &updated
	e.Extra = map[string]any{"z": "last", "a": "first"}
	meta := loadMeta()
	if _, err := Load(ctx, pool, meta, sliceNext([]Entity{e}), nil); err != nil {
		t.Fatal(err)
	}
	var names []string
	var ids map[string]string
	var stored Entity
	var hash []byte
	var legal, state, zip, file, snapshot string
	var loadedAt, sourceUpdated time.Time
	if err := pool.QueryRow(ctx, `SELECT legal_name, names, state, zip5, ids, entity,
		content_hash, file_name, snapshot_ref, source_updated_at, loaded_at FROM registry_entities`).Scan(
		&legal, &names, &state, &zip, &ids, &stored, &hash, &file, &snapshot, &sourceUpdated, &loadedAt,
	); err != nil {
		t.Fatal(err)
	}
	if legal != e.Facts.LegalName || state != "FL" || zip != "33101" || !reflect.DeepEqual(names, []string{"company 0"}) ||
		!reflect.DeepEqual(ids, e.Facts.IDs) || !reflect.DeepEqual(stored, e) || file != meta.FileName || snapshot != meta.SnapshotRef ||
		!loadedAt.Equal(meta.LoadedAt) || !sourceUpdated.Equal(updated) {
		t.Fatalf("incorrect projection: names=%v ids=%v entity=%+v file=%s snapshot=%s", names, ids, stored, file, snapshot)
	}
	canonical := e
	canonical.Line = 0
	encoded, err := json.Marshal(canonical)
	if err != nil {
		t.Fatal(err)
	}
	wantHash := sha256.Sum256(encoded)
	if !reflect.DeepEqual(hash, wantHash[:]) {
		t.Fatalf("content hash = %x, want %x", hash, wantHash)
	}
	// Map insertion order, a different line, and load metadata are not changes.
	e.Extra = map[string]any{"a": "first", "z": "last"}
	e.Line = 999
	meta.FileName = "new-file.csv"
	meta.LoadedAt = meta.LoadedAt.Add(24 * time.Hour)
	stats, err := Load(ctx, pool, meta, sliceNext([]Entity{e}), func(old, new Entity, meta LoadMeta) []Signal {
		t.Fatal("diff called for a provenance-only change")
		return nil
	})
	if err != nil || stats.Unchanged != 1 {
		t.Fatalf("provenance-only load = %+v, %v", stats, err)
	}
}

func TestLoadNextErrorKeepsEarlierBatches(t *testing.T) {
	for _, n := range []int{1, 5001} {
		t.Run(fmt.Sprint(n), func(t *testing.T) {
			pool := testdb.New(t)
			rows := entities(n)
			next := sliceNext(rows)
			boom := errors.New("input read failed")
			stats, err := Load(context.Background(), pool, loadMeta(), func() (Entity, bool, error) {
				e, ok, err := next()
				if !ok {
					return Entity{}, false, boom
				}
				return e, ok, err
			}, nil)
			want := n / 5000 * 5000
			if !errors.Is(err, boom) || stats.Read != n || stats.Inserted != want {
				t.Fatalf("failed load = %+v, %v; want %d committed", stats, err, want)
			}
			var count int
			if err := pool.QueryRow(context.Background(), "SELECT count(*) FROM registry_entities").Scan(&count); err != nil {
				t.Fatal(err)
			}
			if count != want {
				t.Fatalf("row count = %d, want %d", count, want)
			}
		})
	}
}

func TestLoadSignalFailureRollsBackBatch(t *testing.T) {
	pool := testdb.New(t)
	ctx := context.Background()
	rows := entities(2)
	if _, err := Load(ctx, pool, loadMeta(), sliceNext(rows), nil); err != nil {
		t.Fatal(err)
	}
	for i := range rows {
		rows[i].Facts.Principal = "New Principal"
	}
	stats, err := Load(ctx, pool, loadMeta(), sliceNext(rows), func(old, new Entity, meta LoadMeta) []Signal {
		sig := changeSignal(new, meta)
		if new.ExternalRef == rows[1].ExternalRef {
			sig.Type = "invalid"
		}
		return []Signal{sig}
	})
	if err == nil || stats.Updated != 0 || stats.Signals != 0 || len(stats.SignalRefs) != 0 {
		t.Fatalf("failed signal load = %+v, %v", stats, err)
	}
	var count int
	if err := pool.QueryRow(ctx, "SELECT count(*) FROM registry_signals").Scan(&count); err != nil || count != 0 {
		t.Fatalf("signal count = %d, %v", count, err)
	}
	stats, err = Load(ctx, pool, loadMeta(), sliceNext(entities(2)), nil)
	if err != nil || stats.Unchanged != 2 {
		t.Fatalf("stored rows changed after rollback: %+v, %v", stats, err)
	}
}

func TestLoadSignalRefsOnlyNewSorted(t *testing.T) {
	pool := testdb.New(t)
	ctx := context.Background()
	rows := entities(3)
	if _, err := Load(ctx, pool, loadMeta(), sliceNext(rows), nil); err != nil {
		t.Fatal(err)
	}
	for i := range rows {
		rows[i].Facts.Principal = "New Principal"
	}
	if err := db.InTx(ctx, pool, func(tx pgx.Tx) error {
		_, err := InsertSignals(ctx, tx, []Signal{changeSignal(rows[1], loadMeta())})
		return err
	}); err != nil {
		t.Fatal(err)
	}
	stats, err := Load(ctx, pool, loadMeta(), sliceNext([]Entity{rows[2], rows[1], rows[0]}), func(old, new Entity, meta LoadMeta) []Signal {
		sig := changeSignal(new, meta)
		return []Signal{sig, sig}
	})
	if err != nil || stats.Updated != 3 || stats.Signals != 2 ||
		!reflect.DeepEqual(stats.SignalRefs, []string{rows[0].ExternalRef, rows[2].ExternalRef}) {
		t.Fatalf("signal refs load = %+v, %v", stats, err)
	}
}

func TestInsertSignalsDedupeKey(t *testing.T) {
	pool := testdb.New(t)
	ctx := context.Background()
	base := changeSignal(entities(1)[0], loadMeta())
	err := db.InTx(ctx, pool, func(tx pgx.Tx) error {
		if n, err := InsertSignals(ctx, tx, []Signal{base}); err != nil || n != 1 {
			t.Fatalf("baseline signal = %d, %v", n, err)
		}
		var key string
		if err := tx.QueryRow(ctx, "SELECT dedupe_key FROM registry_signals").Scan(&key); err != nil {
			return err
		}
		hash := sha256.Sum256([]byte(`fl_sunbiz|P0000000|officer_change|2026-11-20|"Old Principal"`))
		if key != hex.EncodeToString(hash[:]) {
			t.Fatalf("dedupe key = %s, want %x", key, hash)
		}
		for _, tc := range []struct {
			name   string
			mutate func(*Signal)
			want   int
		}{
			{"same UTC date", func(s *Signal) { s.ObservedAt = s.ObservedAt.Add(time.Hour) }, 0},
			{"time zone", func(s *Signal) { s.ObservedAt = s.ObservedAt.In(time.FixedZone("+13", 13*3600)) }, 0},
			{"citation metadata", func(s *Signal) { s.Citation.SourceRef = "another-file.csv" }, 0},
			{"date", func(s *Signal) { s.ObservedAt = s.ObservedAt.Add(24 * time.Hour) }, 1},
			{"value", func(s *Signal) { s.Citation.Value = json.RawMessage(`"Other Principal"`) }, 1},
			{"type", func(s *Signal) { s.Type = proxy.EntityChange }, 1},
			{"ref", func(s *Signal) { s.ExternalRef = "P0000001" }, 1},
			{"source", func(s *Signal) { s.SourceSystem = "sam" }, 1},
		} {
			sig := base
			tc.mutate(&sig)
			if n, err := InsertSignals(ctx, tx, []Signal{sig}); err != nil || n != tc.want {
				t.Fatalf("%s = %d, %v; want %d", tc.name, n, err, tc.want)
			}
		}
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
}

func TestLoadSkipsMissingIdentity(t *testing.T) {
	pool := testdb.New(t)
	rows := entities(4)
	rows[0].ExternalRef = " "
	rows[1].Facts.LegalName = " "
	rows[2].Facts.LegalName = "LLC"
	rows[3].Facts.IDs = nil
	stats, err := Load(context.Background(), pool, loadMeta(), sliceNext(rows), nil)
	if err != nil || stats.Read != 4 || stats.Skipped != 3 || stats.Inserted != 1 {
		t.Fatalf("missing identity load = %+v, %v", stats, err)
	}
	var ids map[string]string
	if err := pool.QueryRow(context.Background(), "SELECT ids FROM registry_entities").Scan(&ids); err != nil {
		t.Fatal(err)
	}
	if ids == nil || len(ids) != 0 {
		t.Fatalf("missing IDs = %v, want empty object", ids)
	}
}

func TestInsertSignalsRejectsInvalidCitation(t *testing.T) {
	pool := testdb.New(t)
	sig := changeSignal(entities(1)[0], loadMeta())
	sig.Citation.Status = "guess"
	err := db.InTx(context.Background(), pool, func(tx pgx.Tx) error {
		_, err := InsertSignals(context.Background(), tx, []Signal{sig})
		return err
	})
	if err == nil {
		t.Fatal("invalid citation accepted")
	}
}
