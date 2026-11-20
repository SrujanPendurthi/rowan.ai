package ledger

import (
	"context"
	"errors"
	"fmt"
	"math"
	"sync"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"

	"rowan.ai/rowan/services/core/internal/testdb"
)

var testNow = time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)

func seedWorkspace(t *testing.T, pool *pgxpool.Pool, tier Tier) string {
	t.Helper()
	var id string
	err := pool.QueryRow(context.Background(), `INSERT INTO workspaces (name, tier, created_at)
		VALUES ('ledger test', $1, $2) RETURNING id::text`, string(tier), testNow).Scan(&id)
	if err != nil {
		t.Fatal(err)
	}
	return id
}

func seedAPIKey(t *testing.T, pool *pgxpool.Pool, workspaceID string, cap int64) string {
	t.Helper()
	var id string
	err := pool.QueryRow(context.Background(), `INSERT INTO api_keys
		(workspace_id, key_hash, label, monthly_spend_cap, created_at)
		VALUES ($1, decode(md5(gen_random_uuid()::text), 'hex'), 'ledger test', $2, $3) RETURNING id::text`, workspaceID, cap, testNow).Scan(&id)
	if err != nil {
		t.Fatal(err)
	}
	return id
}

func seedBucket(t *testing.T, pool *pgxpool.Pool, workspace, kind string, amount int64, expiry time.Time) int64 {
	t.Helper()
	var id int64
	err := pool.QueryRow(context.Background(), `INSERT INTO credit_buckets
		(workspace_id, bucket_type, granted, balance_remaining, granted_at, expires_at)
		VALUES ($1, $2, $3, $3, $4, $5) RETURNING id`, workspace, kind, amount, testNow, expiry).Scan(&id)
	if err != nil {
		t.Fatal(err)
	}
	return id
}

func bucketBalance(t *testing.T, pool *pgxpool.Pool, id, want int64) {
	t.Helper()
	var got int64
	if err := pool.QueryRow(context.Background(), `SELECT balance_remaining FROM credit_buckets WHERE id=$1`, id).Scan(&got); err != nil {
		t.Fatal(err)
	}
	if got != want {
		t.Fatalf("bucket %d balance = %d, want %d", id, got, want)
	}
}

func countRows(t *testing.T, pool *pgxpool.Pool, query string, want int64, args ...any) {
	t.Helper()
	var got int64
	if err := pool.QueryRow(context.Background(), query, args...).Scan(&got); err != nil {
		t.Fatal(err)
	}
	if got != want {
		t.Fatalf("%s = %d, want %d", query, got, want)
	}
}

func TestTierSpec(t *testing.T) {
	for _, tt := range []struct {
		tier     Tier
		included int64
		days     int
		cents    int64
	}{{Solo, 500, 60, 60}, {DealTeam, 2500, 90, 45}, {Fund, 7500, 90, 35}} {
		t.Run(string(tt.tier), func(t *testing.T) {
			included, days, cents := TierSpec(tt.tier)
			if included != tt.included || days != tt.days || cents != tt.cents {
				t.Fatalf("TierSpec = (%d,%d,%d), want (%d,%d,%d)", included, days, cents, tt.included, tt.days, tt.cents)
			}
		})
	}
}

func TestGrantPeriodRollsIncludedToRollover(t *testing.T) {
	pool := testdb.New(t)
	ctx := context.Background()
	workspace := seedWorkspace(t, pool, Solo)
	if err := GrantPeriod(ctx, pool, workspace, testNow); err != nil {
		t.Fatal(err)
	}
	reservation, err := Reserve(ctx, pool, ReserveRequest{WorkspaceID: workspace, IdempotencyKey: "jan", Amount: 100}, testNow)
	if err != nil {
		t.Fatal(err)
	}
	// Task 5 supplies settlement; emulate a completed reservation for this grant fixture.
	if _, err := pool.Exec(ctx, `UPDATE credit_reservations SET status='committed', committed_amount=amount, settled_at=$2 WHERE id=$1`, reservation.ID, testNow); err != nil {
		t.Fatal(err)
	}
	feb := testNow.AddDate(0, 1, 0)
	if err := GrantPeriod(ctx, pool, workspace, feb); err != nil {
		t.Fatal(err)
	}
	countRows(t, pool, `SELECT count(*) FROM credit_buckets WHERE bucket_type='rollover' AND balance_remaining=400 AND expires_at=$1 AND granted_at=$2`, 1, testNow.AddDate(0, 0, 60), testNow)
	countRows(t, pool, `SELECT count(*) FROM credit_buckets WHERE bucket_type='included' AND balance_remaining=500 AND expires_at=$1 AND granted_at=$2`, 1, feb.AddDate(0, 0, 60), feb)
	countRows(t, pool, `SELECT count(*) FROM credit_ledger_entries WHERE entry_type='grant' AND amount=500 AND reservation_id IS NULL AND created_at IN ($1,$2)`, 2, testNow, feb)
}

func TestGrantPeriodIdempotent(t *testing.T) {
	pool := testdb.New(t)
	workspace := seedWorkspace(t, pool, Solo)
	for range 2 {
		if err := GrantPeriod(context.Background(), pool, workspace, testNow); err != nil {
			t.Fatal(err)
		}
	}
	countRows(t, pool, `SELECT count(*) FROM credit_buckets`, 1)
	countRows(t, pool, `SELECT count(*) FROM credit_ledger_entries`, 1)
}

func TestReserveFIFOOrder(t *testing.T) {
	pool := testdb.New(t)
	workspace := seedWorkspace(t, pool, Solo)
	// Insert out of FIFO order to distinguish lock order from consumption order.
	topup := seedBucket(t, pool, workspace, "topup", 100, testNow.AddDate(0, 0, 365))
	included := seedBucket(t, pool, workspace, "included", 500, testNow.AddDate(0, 0, 60))
	rollover := seedBucket(t, pool, workspace, "rollover", 50, testNow.AddDate(0, 0, 10))
	got, err := Reserve(context.Background(), pool, ReserveRequest{WorkspaceID: workspace, IdempotencyKey: "fifo", Amount: 120}, testNow)
	if err != nil {
		t.Fatal(err)
	}
	if got.ID == "" || got.Amount != 120 || got.CommittedAmount != 0 || got.Status != "reserved" {
		t.Fatalf("reservation = %+v", got)
	}
	bucketBalance(t, pool, rollover, 0)
	bucketBalance(t, pool, included, 430)
	bucketBalance(t, pool, topup, 100)
	countRows(t, pool, `SELECT count(*) FROM credit_ledger_entries WHERE reservation_id=$1 AND entry_type='reserve' AND created_at=$2 AND ((bucket_id=$3 AND amount=50) OR (bucket_id=$4 AND amount=70))`, 2, got.ID, testNow, rollover, included)
}

func TestReserveFIFOExpiryAndID(t *testing.T) {
	pool := testdb.New(t)
	workspace := seedWorkspace(t, pool, Solo)
	later := seedBucket(t, pool, workspace, "included", 10, testNow.AddDate(0, 0, 30))
	first := seedBucket(t, pool, workspace, "included", 10, testNow.AddDate(0, 0, 10))
	second := seedBucket(t, pool, workspace, "included", 10, testNow.AddDate(0, 0, 10))
	if _, err := Reserve(context.Background(), pool, ReserveRequest{WorkspaceID: workspace, IdempotencyKey: "ties", Amount: 15}, testNow); err != nil {
		t.Fatal(err)
	}
	bucketBalance(t, pool, first, 0)
	bucketBalance(t, pool, second, 5)
	bucketBalance(t, pool, later, 10)
}

func TestReserveSkipsExpired(t *testing.T) {
	pool := testdb.New(t)
	workspace := seedWorkspace(t, pool, Solo)
	expired := seedBucket(t, pool, workspace, "rollover", 50, testNow.Add(-time.Second))
	boundary := seedBucket(t, pool, workspace, "included", 50, testNow)
	live := seedBucket(t, pool, workspace, "topup", 20, testNow.Add(time.Hour))
	other := seedWorkspace(t, pool, Solo)
	seedBucket(t, pool, other, "topup", 100, testNow.Add(time.Hour))
	available, err := Available(context.Background(), pool, workspace, testNow)
	if err != nil || available != 20 {
		t.Fatalf("Available = %d, %v", available, err)
	}
	if _, err := Reserve(context.Background(), pool, ReserveRequest{WorkspaceID: workspace, IdempotencyKey: "live", Amount: 20}, testNow); err != nil {
		t.Fatal(err)
	}
	bucketBalance(t, pool, expired, 50)
	bucketBalance(t, pool, boundary, 50)
	bucketBalance(t, pool, live, 0)
}

func TestReserveInsufficientNoPartial(t *testing.T) {
	pool := testdb.New(t)
	workspace := seedWorkspace(t, pool, Solo)
	id := seedBucket(t, pool, workspace, "topup", 100, testNow.Add(time.Hour))
	_, err := Reserve(context.Background(), pool, ReserveRequest{WorkspaceID: workspace, IdempotencyKey: "short", Amount: 101}, testNow)
	if !errors.Is(err, ErrInsufficientCredits) {
		t.Fatalf("error = %v", err)
	}
	bucketBalance(t, pool, id, 100)
	countRows(t, pool, `SELECT count(*) FROM credit_reservations`, 0)
	countRows(t, pool, `SELECT count(*) FROM credit_ledger_entries`, 0)
}

func TestReserveIdempotentReplay(t *testing.T) {
	pool := testdb.New(t)
	workspace := seedWorkspace(t, pool, Solo)
	id := seedBucket(t, pool, workspace, "topup", 10, testNow.Add(time.Hour))
	req := ReserveRequest{WorkspaceID: workspace, IdempotencyKey: "replay", Amount: 10}
	first, err := Reserve(context.Background(), pool, req, testNow)
	if err != nil {
		t.Fatal(err)
	}
	// Replays return the current reservation, even after settlement and bucket expiry.
	if _, err := pool.Exec(context.Background(), `UPDATE credit_reservations SET status='committed', committed_amount=7, settled_at=$2 WHERE id=$1`, first.ID, testNow); err != nil {
		t.Fatal(err)
	}
	got, err := Reserve(context.Background(), pool, req, testNow.AddDate(0, 1, 0))
	if err != nil || got.ID != first.ID || got.Status != "committed" || got.CommittedAmount != 7 {
		t.Fatalf("replay = %+v, %v", got, err)
	}
	bucketBalance(t, pool, id, 0)
	countRows(t, pool, `SELECT count(*) FROM credit_ledger_entries`, 1)
}

func TestReserveIdempotencyConflict(t *testing.T) {
	pool := testdb.New(t)
	workspace := seedWorkspace(t, pool, Solo)
	id := seedBucket(t, pool, workspace, "topup", 100, testNow.Add(time.Hour))
	req := ReserveRequest{WorkspaceID: workspace, IdempotencyKey: "conflict", Amount: 10}
	if _, err := Reserve(context.Background(), pool, req, testNow); err != nil {
		t.Fatal(err)
	}
	req.Amount++
	if _, err := Reserve(context.Background(), pool, req, testNow); !errors.Is(err, ErrIdempotencyConflict) {
		t.Fatalf("error = %v", err)
	}
	bucketBalance(t, pool, id, 90)
}

func concurrentReserves(t *testing.T, pool *pgxpool.Pool, requests []ReserveRequest) ([]Reservation, []error) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 8*time.Second)
	defer cancel()
	results := make([]Reservation, len(requests))
	errs := make([]error, len(requests))
	start := make(chan struct{})
	var wg sync.WaitGroup
	for i, req := range requests {
		wg.Go(func() {
			<-start
			results[i], errs[i] = Reserve(ctx, pool, req, testNow)
		})
	}
	close(start)
	wg.Wait()
	return results, errs
}

func TestReserveSameKeyConcurrent(t *testing.T) {
	for _, balance := range []int64{500, 10} {
		t.Run(fmt.Sprint(balance), func(t *testing.T) {
			pool := testdb.New(t)
			workspace := seedWorkspace(t, pool, Solo)
			id := seedBucket(t, pool, workspace, "topup", balance, testNow.Add(time.Hour))
			requests := make([]ReserveRequest, 50)
			for i := range requests {
				requests[i] = ReserveRequest{WorkspaceID: workspace, IdempotencyKey: "same", Amount: 10}
			}
			results, errs := concurrentReserves(t, pool, requests)
			for i, err := range errs {
				if err != nil || results[i].ID == "" || results[i].ID != results[0].ID {
					t.Fatalf("caller %d = %+v, %v", i, results[i], err)
				}
			}
			bucketBalance(t, pool, id, balance-10)
			countRows(t, pool, `SELECT count(*) FROM credit_reservations`, 1)
			countRows(t, pool, `SELECT count(*) FROM credit_ledger_entries`, 1)
		})
	}
}

func TestReserveConcurrentNoOverdraft(t *testing.T) {
	pool := testdb.New(t)
	workspace := seedWorkspace(t, pool, Solo)
	id := seedBucket(t, pool, workspace, "topup", 100, testNow.Add(time.Hour))
	requests := make([]ReserveRequest, 30)
	for i := range requests {
		requests[i] = ReserveRequest{WorkspaceID: workspace, IdempotencyKey: fmt.Sprint(i), Amount: 10}
	}
	_, errs := concurrentReserves(t, pool, requests)
	success, insufficient := 0, 0
	for _, err := range errs {
		switch {
		case err == nil:
			success++
		case errors.Is(err, ErrInsufficientCredits):
			insufficient++
		default:
			t.Fatal(err)
		}
	}
	if success != 10 || insufficient != 20 {
		t.Fatalf("success=%d insufficient=%d", success, insufficient)
	}
	bucketBalance(t, pool, id, 0)
	countRows(t, pool, `SELECT count(*) FROM credit_reservations`, 10)
	countRows(t, pool, `SELECT count(*) FROM credit_ledger_entries`, 10)
}

func TestSpendCapExceeded(t *testing.T) {
	pool := testdb.New(t)
	workspace := seedWorkspace(t, pool, Solo)
	key := seedAPIKey(t, pool, workspace, 50)
	id := seedBucket(t, pool, workspace, "topup", 100, testNow.AddDate(0, 0, 365))
	req := ReserveRequest{WorkspaceID: workspace, APIKeyID: key, IdempotencyKey: "first", Amount: 40}
	if _, err := Reserve(context.Background(), pool, req, testNow); err != nil {
		t.Fatal(err)
	}
	req.IdempotencyKey, req.Amount = "blocked", 20
	if _, err := Reserve(context.Background(), pool, req, testNow); !errors.Is(err, ErrSpendCapExceeded) {
		t.Fatalf("error = %v", err)
	}
	bucketBalance(t, pool, id, 60)
	if _, err := Reserve(context.Background(), pool, req, testNow.AddDate(0, 1, 0)); err != nil {
		t.Fatal(err)
	}
	bucketBalance(t, pool, id, 40)
}

func TestSpendCapConcurrent(t *testing.T) {
	for _, sameKey := range []bool{false, true} {
		t.Run(fmt.Sprint(sameKey), func(t *testing.T) {
			pool := testdb.New(t)
			workspace := seedWorkspace(t, pool, Solo)
			key := seedAPIKey(t, pool, workspace, 10)
			id := seedBucket(t, pool, workspace, "topup", 100, testNow.Add(time.Hour))
			requests := make([]ReserveRequest, 20)
			for i := range requests {
				idem := fmt.Sprint(i)
				if sameKey {
					idem = "same"
				}
				requests[i] = ReserveRequest{WorkspaceID: workspace, APIKeyID: key, IdempotencyKey: idem, Amount: 10}
			}
			results, errs := concurrentReserves(t, pool, requests)
			success := 0
			for i, err := range errs {
				if err == nil {
					success++
					if sameKey && results[i].ID != results[0].ID {
						t.Fatal("different replay IDs")
					}
				} else if sameKey || !errors.Is(err, ErrSpendCapExceeded) {
					t.Fatal(err)
				}
			}
			want := 1
			if sameKey {
				want = len(requests)
			}
			if success != want {
				t.Fatalf("success=%d, want %d", success, want)
			}
			bucketBalance(t, pool, id, 90)
		})
	}
}

func TestGrantPeriodTierAndTopup(t *testing.T) {
	for _, tier := range []Tier{Solo, DealTeam, Fund} {
		t.Run(string(tier), func(t *testing.T) {
			pool := testdb.New(t)
			workspace := seedWorkspace(t, pool, tier)
			ctx := context.Background()
			if err := GrantPeriod(ctx, pool, workspace, testNow); err != nil {
				t.Fatal(err)
			}
			included, days, _ := TierSpec(tier)
			countRows(t, pool, `SELECT count(*) FROM credit_buckets WHERE bucket_type='included' AND granted=$1 AND balance_remaining=$1 AND granted_at=$2 AND expires_at=$3 AND period_start=$2::date`, 1, included, testNow, testNow.AddDate(0, 0, days))
			at := testNow.Add(7 * time.Hour)
			if err := GrantTopup(ctx, pool, workspace, 73, at); err != nil {
				t.Fatal(err)
			}
			countRows(t, pool, `SELECT count(*) FROM credit_buckets WHERE bucket_type='topup' AND granted=73 AND balance_remaining=73 AND granted_at=$1 AND expires_at=$2 AND period_start IS NULL`, 1, at, at.AddDate(0, 0, 365))
			countRows(t, pool, `SELECT count(*) FROM credit_ledger_entries WHERE entry_type='grant' AND amount=73 AND reservation_id IS NULL AND created_at=$1`, 1, at)
		})
	}
}

func TestGrantPeriodConcurrentAndReplayAfterRollover(t *testing.T) {
	pool := testdb.New(t)
	workspace := seedWorkspace(t, pool, Solo)
	ctx, cancel := context.WithTimeout(context.Background(), 8*time.Second)
	defer cancel()
	errs := make([]error, 20)
	var wg sync.WaitGroup
	for i := range errs {
		wg.Go(func() { errs[i] = GrantPeriod(ctx, pool, workspace, testNow) })
	}
	wg.Wait()
	for _, err := range errs {
		if err != nil {
			t.Fatal(err)
		}
	}
	if err := GrantPeriod(ctx, pool, workspace, testNow.AddDate(0, 1, 0)); err != nil {
		t.Fatal(err)
	}
	if err := GrantPeriod(ctx, pool, workspace, testNow); err != nil {
		t.Fatal(err)
	}
	countRows(t, pool, `SELECT count(*) FROM credit_buckets`, 2)
	countRows(t, pool, `SELECT count(*) FROM credit_buckets WHERE bucket_type='included'`, 1)
	countRows(t, pool, `SELECT count(*) FROM credit_ledger_entries`, 2)
}

func TestSpendCapUTCMonthAndStatuses(t *testing.T) {
	pool := testdb.New(t)
	workspace := seedWorkspace(t, pool, Solo)
	key := seedAPIKey(t, pool, workspace, 50)
	seedBucket(t, pool, workspace, "topup", 100, testNow.AddDate(0, 0, 365))
	ctx := context.Background()
	// At this instant February has begun in UTC, but local date is still January.
	now := time.Date(2026, 1, 31, 20, 0, 0, 0, time.FixedZone("west", -5*3600))
	monthStart := time.Date(2026, 2, 1, 0, 0, 0, 0, time.UTC)
	for i, fixture := range []struct {
		at                time.Time
		status            string
		amount, committed int64
	}{
		{monthStart.Add(-time.Nanosecond * 1000), "reserved", 50, 0},
		{monthStart, "committed", 40, 20},
		{monthStart, "refunded", 50, 0},
		{monthStart.AddDate(0, 1, 0), "reserved", 50, 0},
	} {
		_, err := pool.Exec(ctx, `INSERT INTO credit_reservations (workspace_id, api_key_id, idempotency_key, amount, committed_amount, status, created_at) VALUES ($1,$2,$3,$4,$5,$6,$7)`, workspace, key, fmt.Sprint(i), fixture.amount, fixture.committed, fixture.status, fixture.at)
		if err != nil {
			t.Fatal(err)
		}
	}
	req := ReserveRequest{WorkspaceID: workspace, APIKeyID: key, IdempotencyKey: "at-cap", Amount: 30}
	if _, err := Reserve(ctx, pool, req, now); err != nil {
		t.Fatal(err)
	}
	req.IdempotencyKey, req.Amount = "past-cap", 1
	if _, err := Reserve(ctx, pool, req, now); !errors.Is(err, ErrSpendCapExceeded) {
		t.Fatalf("error = %v", err)
	}
	countRows(t, pool, `SELECT count(*) FROM credit_reservations WHERE idempotency_key='at-cap' AND created_at=$1`, 1, now)
}

func TestReserveLargeBalances(t *testing.T) {
	pool := testdb.New(t)
	workspace := seedWorkspace(t, pool, Solo)
	first := seedBucket(t, pool, workspace, "rollover", math.MaxInt64, testNow.Add(time.Hour))
	second := seedBucket(t, pool, workspace, "topup", math.MaxInt64, testNow.Add(time.Hour))
	if _, err := Reserve(context.Background(), pool, ReserveRequest{WorkspaceID: workspace, IdempotencyKey: "large", Amount: math.MaxInt64}, testNow); err != nil {
		t.Fatal(err)
	}
	bucketBalance(t, pool, first, 0)
	bucketBalance(t, pool, second, math.MaxInt64)
}

func TestReserveRejectsInvalidInput(t *testing.T) {
	pool := testdb.New(t)
	workspace := seedWorkspace(t, pool, Solo)
	other := seedWorkspace(t, pool, Solo)
	key := seedAPIKey(t, pool, other, 100)
	for _, req := range []ReserveRequest{
		{WorkspaceID: workspace, IdempotencyKey: "zero", Amount: 0},
		{WorkspaceID: workspace, IdempotencyKey: "negative", Amount: -1},
		{WorkspaceID: workspace, Amount: 1},
		{WorkspaceID: workspace, APIKeyID: key, IdempotencyKey: "foreign-key", Amount: 1},
	} {
		if _, err := Reserve(context.Background(), pool, req, testNow); err == nil {
			t.Fatalf("accepted invalid request: %+v", req)
		}
	}
	if err := GrantTopup(context.Background(), pool, workspace, 0, testNow); err == nil {
		t.Fatal("accepted zero topup")
	}
	countRows(t, pool, `SELECT count(*) FROM credit_reservations`, 0)
	countRows(t, pool, `SELECT count(*) FROM credit_ledger_entries`, 0)
}

func TestLedgerWritesRollbackOnEntryFailure(t *testing.T) {
	for _, operation := range []string{"period", "topup", "reserve"} {
		t.Run(operation, func(t *testing.T) {
			pool := testdb.New(t)
			ctx := context.Background()
			workspace := seedWorkspace(t, pool, Solo)
			id := seedBucket(t, pool, workspace, "included", 100, testNow.AddDate(0, 0, 60))
			if _, err := pool.Exec(ctx, `UPDATE credit_buckets SET period_start=$2 WHERE id=$1`, id, testNow); err != nil {
				t.Fatal(err)
			}
			// Fail after bucket changes to prove the transaction restores every write.
			_, err := pool.Exec(ctx, `CREATE FUNCTION reject_entry() RETURNS trigger LANGUAGE plpgsql AS
				$$ BEGIN RAISE EXCEPTION 'test ledger write failure'; END $$;
				CREATE TRIGGER reject_entry BEFORE INSERT ON credit_ledger_entries
				FOR EACH ROW EXECUTE FUNCTION reject_entry()`)
			if err != nil {
				t.Fatal(err)
			}
			switch operation {
			case "period":
				err = GrantPeriod(ctx, pool, workspace, testNow.AddDate(0, 1, 0))
			case "topup":
				err = GrantTopup(ctx, pool, workspace, 20, testNow)
			case "reserve":
				_, err = Reserve(ctx, pool, ReserveRequest{WorkspaceID: workspace, IdempotencyKey: "rollback", Amount: 20}, testNow)
			}
			if err == nil {
				t.Fatal("expected ledger entry failure")
			}
			bucketBalance(t, pool, id, 100)
			countRows(t, pool, `SELECT count(*) FROM credit_buckets WHERE bucket_type='included'`, 1)
			countRows(t, pool, `SELECT count(*) FROM credit_buckets`, 1)
			countRows(t, pool, `SELECT count(*) FROM credit_reservations`, 0)
			countRows(t, pool, `SELECT count(*) FROM credit_ledger_entries`, 0)
		})
	}
}

func TestSpendCapZeroAndBigintBoundary(t *testing.T) {
	for _, cap := range []int64{0, math.MaxInt64} {
		t.Run(fmt.Sprint(cap), func(t *testing.T) {
			pool := testdb.New(t)
			workspace := seedWorkspace(t, pool, Solo)
			key := seedAPIKey(t, pool, workspace, cap)
			id := seedBucket(t, pool, workspace, "topup", math.MaxInt64, testNow.Add(time.Hour))
			if cap > 0 {
				_, err := pool.Exec(context.Background(), `INSERT INTO credit_reservations
					(workspace_id, api_key_id, idempotency_key, amount, status, created_at)
					VALUES ($1,$2,'previous',$3,'reserved',$4)`, workspace, key, cap, testNow)
				if err != nil {
					t.Fatal(err)
				}
			}
			_, err := Reserve(context.Background(), pool, ReserveRequest{WorkspaceID: workspace, APIKeyID: key, IdempotencyKey: "overflow", Amount: 1}, testNow)
			if !errors.Is(err, ErrSpendCapExceeded) {
				t.Fatalf("error = %v", err)
			}
			bucketBalance(t, pool, id, math.MaxInt64)
		})
	}
}
