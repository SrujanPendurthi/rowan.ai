package main

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/url"
	"os"
	"path"
	"path/filepath"
	"regexp"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"

	"rowan.ai/rowan/services/core/internal/fetch"
	"rowan.ai/rowan/services/core/internal/proxy"
	"rowan.ai/rowan/services/core/internal/registry"
)

// Overridable in tests.
var (
	dbprBase = "https://www2.myfloridalicense.com"
	samBase  = "https://api.sam.gov"
)

const (
	dbprConstructionPath = "/sto/file_download/extracts//CONSTRUCTIONLICENSE_1.csv"
	dbprElectricalPath   = "/sto/file_download/extracts/lic08el.csv"
)

var monthRE = regexp.MustCompile(`^(0[1-9]|1[0-2])/\d{4}$`)

// loader loads one file and returns its stats and the refs that gained signals.
type loader func(ctx context.Context, pool *pgxpool.Pool, path, snapshotRef string, now time.Time) (stats any, system string, refs []string, err error)

var loaders = map[string]loader{
	"sam": func(ctx context.Context, pool *pgxpool.Pool, path, ref string, now time.Time) (any, string, []string, error) {
		s, err := registry.LoadSAM(ctx, pool, path, ref, now)
		return s, "sam", s.SignalRefs, err
	},
	"sunbiz-corp": func(ctx context.Context, pool *pgxpool.Pool, path, ref string, now time.Time) (any, string, []string, error) {
		s, err := registry.LoadSunbizCorp(ctx, pool, path, ref, now)
		return s, "fl_sunbiz", s.SignalRefs, err
	},
	"sunbiz-events": func(ctx context.Context, pool *pgxpool.Pool, path, ref string, now time.Time) (any, string, []string, error) {
		s, err := registry.LoadSunbizEvents(ctx, pool, path, ref, now)
		return s, "fl_sunbiz", s.SignalRefs, err
	},
	"dbpr": func(ctx context.Context, pool *pgxpool.Pool, path, ref string, now time.Time) (any, string, []string, error) {
		s, err := registry.LoadDBPR(ctx, pool, path, ref, now)
		return s, "fl_dbpr", s.SignalRefs, err
	},
}

// loadFile loads path with ld, recomputes proxy summaries for refs with new signals, and prints one JSON line.
func loadFile(ctx context.Context, pool *pgxpool.Pool, ld loader, path, snapshotRef string, out io.Writer, now func() time.Time) error {
	stats, system, refs, err := ld(ctx, pool, path, snapshotRef, now())
	if err != nil {
		return fmt.Errorf("load %s: %w", path, err)
	}
	if _, err := proxy.RecomputeForRefs(ctx, pool, system, refs, now()); err != nil {
		return fmt.Errorf("recompute proxy: %w", err)
	}
	return json.NewEncoder(out).Encode(stats)
}

func fileSnapshotRef(path string) (string, error) {
	f, err := os.Open(path)
	if err != nil {
		return "", err
	}
	defer f.Close()
	h := sha256.New()
	if _, err := io.Copy(h, f); err != nil {
		return "", err
	}
	return "sha256:" + hex.EncodeToString(h.Sum(nil)), nil
}

func loadCmd(ctx context.Context, pool *pgxpool.Pool, args []string, out io.Writer, now func() time.Time) error {
	ld := loaders[args[0]]
	if ld == nil || len(args) != 2 {
		return errors.New(usage)
	}
	ref, err := fileSnapshotRef(args[1])
	if err != nil {
		return fmt.Errorf("hash %s: %w", args[1], err)
	}
	return loadFile(ctx, pool, ld, args[1], ref, out, now)
}

func fetchCmd(ctx context.Context, pool *pgxpool.Pool, args []string, out io.Writer, now func() time.Time) error {
	type target struct{ url, name string }
	var targets []target
	var ld loader
	switch args[0] {
	case "dbpr":
		for _, p := range []string{dbprConstructionPath, dbprElectricalPath} {
			targets = append(targets, target{dbprBase + p, path.Base(p)})
		}
		ld = loaders["dbpr"]
	case "sam":
		fs := newFlags("fetch sam")
		month := fs.String("month", "", "extract month, MM/YYYY")
		if err := fs.Parse(args[1:]); err != nil {
			return err
		}
		key := os.Getenv("SAM_API_KEY")
		if key == "" {
			return errors.New("SAM_API_KEY is required")
		}
		if !monthRE.MatchString(*month) {
			return errors.New("--month must be MM/YYYY")
		}
		q := url.Values{"api_key": {key}, "fileType": {"ENTITY"}, "sensitivity": {"PUBLIC"},
			"frequency": {"MONTHLY"}, "date": {*month}, "charset": {"UTF8"}}
		// ponytail: SAM file name is synthesized from --month; the API's real file name isn't captured.
		name := "SAM_PUBLIC_MONTHLY_V2_" + (*month)[3:] + (*month)[:2] + ".zip"
		targets = append(targets, target{samBase + "/data-services/v1/extracts?" + q.Encode(), name})
		ld = loaders["sam"]
	default:
		return errors.New(usage)
	}
	dir := os.Getenv("ROWAN_SNAPSHOT_DIR")
	if dir == "" {
		dir = "./data/snapshots"
	}
	f := &fetch.Fetcher{Pool: pool, SnapshotDir: dir, Now: now}
	for _, t := range targets {
		res, err := f.Get(ctx, t.url) // error already names the redacted URL
		if err != nil {
			return err
		}
		if err := loadNamed(ctx, pool, ld, res, t.name, out, now); err != nil {
			return err
		}
	}
	return nil
}

// loadNamed symlinks the extensionless snapshot under its source name so the
// loader sees the right extension and cites the source file name.
func loadNamed(ctx context.Context, pool *pgxpool.Pool, ld loader, res fetch.Result, name string, out io.Writer, now func() time.Time) error {
	dir, err := os.MkdirTemp("", "rowanctl-")
	if err != nil {
		return err
	}
	defer os.RemoveAll(dir)
	link := filepath.Join(dir, name)
	if err := os.Symlink(res.Path, link); err != nil {
		return fmt.Errorf("link snapshot: %w", err)
	}
	return loadFile(ctx, pool, ld, link, res.SnapshotRef, out, now)
}
