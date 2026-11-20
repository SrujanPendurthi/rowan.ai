// Command rowanctl seeds workspaces, API keys, and credits. DATABASE_URL selects the database.
package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"io"
	"log"
	"os"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"

	"rowan.ai/rowan/services/core/internal/api"
	"rowan.ai/rowan/services/core/internal/db"
	"rowan.ai/rowan/services/core/internal/ledger"
)

const usage = `usage:
  rowanctl workspace create --name NAME --tier solo|deal_team|fund
  rowanctl key create --workspace ID --cap CREDITS --rate PER_MINUTE [--label LABEL]
  rowanctl credits grant-period --workspace ID --period YYYY-MM-01
  rowanctl credits topup --workspace ID --credits N`

func main() {
	ctx := context.Background()
	pool, err := db.Open(ctx, os.Getenv("DATABASE_URL"))
	if err != nil {
		log.Fatal(err)
	}
	defer pool.Close()
	if err := db.Migrate(ctx, pool); err != nil {
		log.Fatal(err)
	}
	if err := run(ctx, pool, os.Args[1:], os.Stdout, time.Now); err != nil {
		fmt.Fprintln(os.Stderr, "rowanctl:", err)
		os.Exit(1)
	}
}

func run(ctx context.Context, pool *pgxpool.Pool, args []string, out io.Writer, now func() time.Time) error {
	if len(args) < 2 {
		return errors.New(usage)
	}
	switch args[0] + " " + args[1] {
	case "workspace create":
		return workspaceCreate(ctx, pool, args[2:], out)
	case "key create":
		return keyCreate(ctx, pool, args[2:], out)
	case "credits grant-period":
		return grantPeriod(ctx, pool, args[2:], out)
	case "credits topup":
		return topup(ctx, pool, args[2:], out, now)
	}
	return errors.New(usage)
}

func newFlags(name string) *flag.FlagSet {
	fs := flag.NewFlagSet(name, flag.ContinueOnError)
	fs.SetOutput(io.Discard)
	return fs
}

func workspaceCreate(ctx context.Context, pool *pgxpool.Pool, args []string, out io.Writer) error {
	fs := newFlags("workspace create")
	name := fs.String("name", "", "workspace name")
	tier := fs.String("tier", "", "solo, deal_team, or fund")
	if err := fs.Parse(args); err != nil {
		return err
	}
	if *name == "" {
		return errors.New("--name is required")
	}
	if inc, _, _ := ledger.TierSpec(ledger.Tier(*tier)); inc == 0 {
		return fmt.Errorf("--tier must be solo, deal_team, or fund, got %q", *tier)
	}
	var id string
	if err := pool.QueryRow(ctx, `INSERT INTO workspaces(name,tier) VALUES ($1,$2) RETURNING id::text`, *name, *tier).Scan(&id); err != nil {
		return fmt.Errorf("create workspace: %w", err)
	}
	_, err := fmt.Fprintf(out, "workspace_id=%s\n", id)
	return err
}

func keyCreate(ctx context.Context, pool *pgxpool.Pool, args []string, out io.Writer) error {
	fs := newFlags("key create")
	ws := fs.String("workspace", "", "workspace id")
	spendCap := fs.Int64("cap", 1000, "monthly spend cap in credits")
	rate := fs.Int("rate", 120, "requests per minute")
	label := fs.String("label", "cli", "key label")
	if err := fs.Parse(args); err != nil {
		return err
	}
	if *ws == "" || *spendCap < 0 || *rate <= 0 {
		return errors.New("--workspace is required; --cap must be >= 0; --rate must be > 0")
	}
	token, err := api.NewToken()
	if err != nil {
		return err
	}
	var id string
	err = pool.QueryRow(ctx, `INSERT INTO api_keys(workspace_id,key_hash,label,monthly_spend_cap,rate_per_minute)
		VALUES ($1,$2,$3,$4,$5) RETURNING id::text`, *ws, api.HashKey(token), *label, *spendCap, *rate).Scan(&id)
	if err != nil {
		return fmt.Errorf("create key: %w", err)
	}
	// The plaintext token is never stored; this is the only time it is shown.
	_, err = fmt.Fprintf(out, "key_id=%s\ntoken=%s\n", id, token)
	return err
}

func grantPeriod(ctx context.Context, pool *pgxpool.Pool, args []string, out io.Writer) error {
	fs := newFlags("credits grant-period")
	ws := fs.String("workspace", "", "workspace id")
	period := fs.String("period", "", "period start, YYYY-MM-01")
	if err := fs.Parse(args); err != nil {
		return err
	}
	start, err := time.Parse("2006-01-02", *period)
	if err != nil || start.Day() != 1 || *ws == "" {
		return errors.New("--workspace is required; --period must be YYYY-MM-01")
	}
	if err := ledger.GrantPeriod(ctx, pool, *ws, start); err != nil {
		return err
	}
	_, err = fmt.Fprintf(out, "granted workspace_id=%s period=%s\n", *ws, *period)
	return err
}

func topup(ctx context.Context, pool *pgxpool.Pool, args []string, out io.Writer, now func() time.Time) error {
	fs := newFlags("credits topup")
	ws := fs.String("workspace", "", "workspace id")
	credits := fs.Int64("credits", 0, "credits to add")
	if err := fs.Parse(args); err != nil {
		return err
	}
	if *ws == "" || *credits <= 0 {
		return errors.New("--workspace is required; --credits must be > 0")
	}
	if err := ledger.GrantTopup(ctx, pool, *ws, *credits, now()); err != nil {
		return err
	}
	_, err := fmt.Fprintf(out, "topped_up workspace_id=%s credits=%d\n", *ws, *credits)
	return err
}
