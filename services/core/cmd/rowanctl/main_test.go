package main

import (
	"archive/zip"
	"bytes"
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"os"
	"strings"
	"testing"
	"time"

	"rowan.ai/rowan/services/core/internal/api"
	"rowan.ai/rowan/services/core/internal/ledger"
	"rowan.ai/rowan/services/core/internal/testdb"
)

func TestSeedFlow(t *testing.T) {
	ctx := context.Background()
	pool := testdb.New(t)
	now := func() time.Time { return time.Date(2026, 11, 20, 0, 0, 0, 0, time.UTC) }
	do := func(args ...string) string {
		t.Helper()
		var out bytes.Buffer
		if err := run(ctx, pool, args, &out, now); err != nil {
			t.Fatalf("%v: %v", args, err)
		}
		return out.String()
	}
	field := func(out, name string) string {
		for _, line := range strings.Split(out, "\n") {
			if v, ok := strings.CutPrefix(line, name+"="); ok {
				return v
			}
		}
		t.Fatalf("no %s in %q", name, out)
		return ""
	}

	ws := field(do("workspace", "create", "--name", "acme", "--tier", "solo"), "workspace_id")
	keyOut := do("key", "create", "--workspace", ws, "--cap", "50", "--rate", "30")
	token := field(keyOut, "token")
	var stored int
	if err := pool.QueryRow(ctx, `SELECT count(*) FROM api_keys WHERE workspace_id=$1 AND key_hash=$2 AND monthly_spend_cap=50 AND rate_per_minute=30`,
		ws, api.HashKey(token)).Scan(&stored); err != nil || stored != 1 {
		t.Fatalf("key not stored with hash: %d, %v", stored, err)
	}
	do("credits", "grant-period", "--workspace", ws, "--period", "2026-11-01")
	do("credits", "grant-period", "--workspace", ws, "--period", "2026-11-01") // idempotent
	do("credits", "topup", "--workspace", ws, "--credits", "25")
	if n, err := ledger.Available(ctx, pool, ws, now()); err != nil || n != 525 {
		t.Fatalf("available = %d, %v; want 525", n, err)
	}

	for _, bad := range [][]string{
		{}, {"workspace", "create", "--name", "x", "--tier", "gold"},
		{"credits", "grant-period", "--workspace", ws, "--period", "2026-11-15"},
		{"credits", "topup", "--workspace", ws, "--credits", "0"},
		{"key", "create", "--workspace", ws, "--rate", "0"},
	} {
		if err := run(ctx, pool, bad, &bytes.Buffer{}, now); err == nil {
			t.Fatalf("%v: want error", bad)
		}
	}
}

const fixtures = "../../internal/registry/testdata/"

func TestLoadDBPRCommand(t *testing.T) {
	pool := testdb.New(t)
	var out bytes.Buffer
	if err := run(context.Background(), pool, []string{"load", "dbpr", fixtures + "dbpr_construction.csv"}, &out, time.Now); err != nil {
		t.Fatal(err)
	}
	var stats struct{ Inserted int }
	if err := json.Unmarshal(out.Bytes(), &stats); err != nil || stats.Inserted != 2 {
		t.Fatalf("out = %q, %v", out.String(), err)
	}
}

func TestFetchSAMRequiresKey(t *testing.T) {
	t.Setenv("SAM_API_KEY", "")
	err := run(context.Background(), testdb.New(t), []string{"fetch", "sam", "--month", "11/2026"}, &bytes.Buffer{}, time.Now)
	if err == nil || !strings.Contains(err.Error(), "SAM_API_KEY is required") {
		t.Fatalf("err = %v", err)
	}
	t.Setenv("SAM_API_KEY", "k")
	if err := run(context.Background(), testdb.New(t), []string{"fetch", "sam", "--month", "2026-11"}, &bytes.Buffer{}, time.Now); err == nil {
		t.Fatal("bad month: want error")
	}
}

func TestFetchDBPRUsesFetcher(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		name := map[string]string{"/sto/file_download/extracts//CONSTRUCTIONLICENSE_1.csv": "dbpr_construction.csv",
			"/sto/file_download/extracts/lic08el.csv": "dbpr_electrical.csv"}[r.URL.Path]
		if name == "" {
			http.NotFound(w, r)
			return
		}
		http.ServeFile(w, r, fixtures+name)
	}))
	defer srv.Close()
	old := dbprBase
	dbprBase = srv.URL
	defer func() { dbprBase = old }()
	t.Setenv("ROWAN_SNAPSHOT_DIR", t.TempDir())
	pool := testdb.New(t)
	var out bytes.Buffer
	if err := run(context.Background(), pool, []string{"fetch", "dbpr"}, &out, time.Now); err != nil {
		t.Fatal(err)
	}
	for _, name := range []string{"CONSTRUCTIONLICENSE_1.csv", "lic08el.csv"} {
		var n int
		if err := pool.QueryRow(context.Background(), `SELECT count(*) FROM registry_entities WHERE source_system='fl_dbpr' AND file_name=$1`, name).Scan(&n); err != nil || n != 2 {
			t.Fatalf("%s entities = %d, %v; out %s", name, n, err, out.String())
		}
	}
	if lines := strings.Count(strings.TrimSpace(out.String()), "\n") + 1; lines != 2 {
		t.Fatalf("want one JSON line per file, got %q", out.String())
	}
}

func TestFetchSAMLoads(t *testing.T) {
	dat, err := os.ReadFile(fixtures + "sam_public_v2.dat")
	if err != nil {
		t.Fatal(err)
	}
	var zb bytes.Buffer
	zw := zip.NewWriter(&zb)
	w, _ := zw.Create("SAM_PUBLIC_MONTHLY_V2_20261101.dat")
	w.Write(dat)
	zw.Close()
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/data-services/v1/extracts" || r.URL.Query().Get("date") != "11/2026" {
			http.NotFound(w, r)
			return
		}
		w.Write(zb.Bytes())
	}))
	defer srv.Close()
	old := samBase
	samBase = srv.URL
	defer func() { samBase = old }()
	t.Setenv("SAM_API_KEY", "secret")
	t.Setenv("ROWAN_SNAPSHOT_DIR", t.TempDir())
	pool := testdb.New(t)
	var out bytes.Buffer
	if err := run(context.Background(), pool, []string{"fetch", "sam", "--month", "11/2026"}, &out, time.Now); err != nil {
		t.Fatal(err)
	}
	var n int
	if err := pool.QueryRow(context.Background(), `SELECT count(*) FROM registry_entities WHERE source_system='sam' AND file_name='SAM_PUBLIC_MONTHLY_V2_202611.zip'`).Scan(&n); err != nil || n == 0 {
		t.Fatalf("entities = %d, %v; out %s", n, err, out.String())
	}
	if strings.Contains(out.String(), "secret") {
		t.Fatal("key leaked")
	}
}
