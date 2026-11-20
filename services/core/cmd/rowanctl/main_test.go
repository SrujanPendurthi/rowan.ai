package main

import (
	"bytes"
	"context"
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
