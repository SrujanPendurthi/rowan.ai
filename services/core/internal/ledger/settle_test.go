package ledger

import (
	"context"
	"errors"
	"fmt"
	"math"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"

	"rowan.ai/rowan/services/core/internal/testdb"
)

func assertInvariants(t *testing.T, pool *pgxpool.Pool, workspace string) {
	t.Helper()
	if err := CheckInvariants(context.Background(), pool, workspace); err != nil {
		t.Fatal(err)
	}
}

func reserveForSettlement(t *testing.T, pool *pgxpool.Pool, workspace, key string, amount int64, now time.Time) Reservation {
	t.Helper()
	r, err := Reserve(context.Background(), pool, ReserveRequest{WorkspaceID: workspace, IdempotencyKey: key, Amount: amount}, now)
	if err != nil {
		t.Fatal(err)
	}
	return r
}

func TestSettlePartialRefundsRemainder(t *testing.T) {
	pool := testdb.New(t)
	workspace := seedWorkspace(t, pool, Solo)
	// Bucket IDs deliberately oppose consumption order.
	included := seedBucket(t, pool, workspace, "included", 6, testNow.Add(time.Hour))
	rollover := seedBucket(t, pool, workspace, "rollover", 4, testNow.Add(2*time.Hour))
	r := reserveForSettlement(t, pool, workspace, "partial", 10, testNow)
	now := testNow.Add(time.Minute)
	got, err := Settle(context.Background(), pool, r.ID, 7, now)
	if err != nil || got.ID != r.ID || got.Amount != 10 || got.Status != "committed" || got.CommittedAmount != 7 {
		t.Fatalf("Settle = %+v, %v", got, err)
	}
	bucketBalance(t, pool, rollover, 0)
	bucketBalance(t, pool, included, 3)
	countRows(t, pool, `SELECT count(*) FROM credit_ledger_entries WHERE reservation_id=$1 AND entry_type='commit'
		AND ((bucket_id=$2 AND amount=4) OR (bucket_id=$3 AND amount=3)) AND created_at=$4`, 2, r.ID, rollover, included, now)
	countRows(t, pool, `SELECT count(*) FROM credit_ledger_entries WHERE reservation_id=$1 AND entry_type='refund' AND bucket_id=$2 AND amount=3`, 1, r.ID, included)
	countRows(t, pool, `SELECT count(*) FROM credit_reservations WHERE id=$1 AND settled_at=$2`, 1, r.ID, now)
	assertInvariants(t, pool, workspace)
}

func TestSettleZeroIsRefund(t *testing.T) {
	pool := testdb.New(t)
	workspace := seedWorkspace(t, pool, Solo)
	first := seedBucket(t, pool, workspace, "rollover", 4, testNow.Add(time.Hour))
	last := seedBucket(t, pool, workspace, "included", 6, testNow.Add(time.Hour))
	r := reserveForSettlement(t, pool, workspace, "zero", 10, testNow)
	got, err := Settle(context.Background(), pool, r.ID, 0, testNow)
	if err != nil || got.Status != "refunded" || got.CommittedAmount != 0 {
		t.Fatalf("Settle = %+v, %v", got, err)
	}
	bucketBalance(t, pool, first, 4)
	bucketBalance(t, pool, last, 6)
	countRows(t, pool, `SELECT count(*) FROM credit_ledger_entries WHERE entry_type='commit'`, 0)
	var firstRefund int64
	if err := pool.QueryRow(context.Background(), `SELECT bucket_id FROM credit_ledger_entries WHERE entry_type='refund' ORDER BY id LIMIT 1`).Scan(&firstRefund); err != nil {
		t.Fatal(err)
	}
	if firstRefund != last {
		t.Fatalf("first refund lot = %d, want %d", firstRefund, last)
	}
	if replay, err := Refund(context.Background(), pool, r.ID, testNow.Add(time.Minute)); err != nil || replay != got {
		t.Fatalf("Refund replay = %+v, %v", replay, err)
	}
	if _, err := Settle(context.Background(), pool, r.ID, 1, testNow); !errors.Is(err, ErrIdempotencyConflict) {
		t.Fatalf("refunded mismatch = %v", err)
	}
	countRows(t, pool, `SELECT count(*) FROM credit_ledger_entries WHERE entry_type='refund'`, 2)
	assertInvariants(t, pool, workspace)
}

func TestSettleIdempotent(t *testing.T) {
	pool := testdb.New(t)
	workspace := seedWorkspace(t, pool, Solo)
	seedBucket(t, pool, workspace, "included", 10, testNow.Add(time.Hour))
	r := reserveForSettlement(t, pool, workspace, "replay", 10, testNow)
	first, err := Settle(context.Background(), pool, r.ID, 7, testNow)
	if err != nil {
		t.Fatal(err)
	}
	got, err := Settle(context.Background(), pool, r.ID, 7, testNow.Add(time.Minute))
	if err != nil || got != first {
		t.Fatalf("replay = %+v, %v", got, err)
	}
	for _, used := range []int64{5, 0} {
		if _, err := Settle(context.Background(), pool, r.ID, used, testNow); !errors.Is(err, ErrIdempotencyConflict) {
			t.Fatalf("mismatch used %d: %v", used, err)
		}
	}
	countRows(t, pool, `SELECT count(*) FROM credit_ledger_entries WHERE entry_type IN ('commit','refund')`, 2)
	countRows(t, pool, `SELECT count(*) FROM credit_reservations WHERE id=$1 AND settled_at=$2`, 1, r.ID, testNow)
	assertInvariants(t, pool, workspace)
}

func TestSettleRejectsOverUse(t *testing.T) {
	pool := testdb.New(t)
	workspace := seedWorkspace(t, pool, Solo)
	id := seedBucket(t, pool, workspace, "included", 10, testNow.Add(time.Hour))
	r := reserveForSettlement(t, pool, workspace, "invalid", 10, testNow)
	for _, used := range []int64{-1, 11} {
		if _, err := Settle(context.Background(), pool, r.ID, used, testNow); !errors.Is(err, ErrInvalidRequest) {
			t.Fatalf("used %d: %v", used, err)
		}
	}
	for _, id := range []string{"", "malformed", "00000000-0000-0000-0000-000000000000"} {
		if _, err := Settle(context.Background(), pool, id, 0, testNow); !errors.Is(err, ErrInvalidRequest) {
			t.Fatalf("ID %q: %v", id, err)
		}
	}
	bucketBalance(t, pool, id, 0)
	countRows(t, pool, `SELECT count(*) FROM credit_ledger_entries`, 1)
	countRows(t, pool, `SELECT count(*) FROM credit_reservations WHERE id=$1 AND status='reserved' AND settled_at IS NULL`, 1, r.ID)
	assertInvariants(t, pool, workspace)
}

func TestExpireDue(t *testing.T) {
	pool := testdb.New(t)
	workspace := seedWorkspace(t, pool, Solo)
	other := seedWorkspace(t, pool, Solo)
	due := seedBucket(t, pool, workspace, "included", 10, testNow)
	overdue := seedBucket(t, pool, other, "topup", 3, testNow.Add(-time.Minute))
	live := seedBucket(t, pool, workspace, "topup", 7, testNow.Add(time.Microsecond))
	for _, want := range []int64{13, 0} {
		got, err := ExpireDue(context.Background(), pool, testNow)
		if err != nil || got != want {
			t.Fatalf("ExpireDue = %d, %v; want %d", got, err, want)
		}
	}
	bucketBalance(t, pool, due, 0)
	bucketBalance(t, pool, overdue, 0)
	bucketBalance(t, pool, live, 7)
	countRows(t, pool, `SELECT count(*) FROM credit_ledger_entries WHERE entry_type='expire' AND reservation_id IS NULL AND created_at=$1`, 2, testNow)
	assertInvariants(t, pool, other)
	assertInvariants(t, pool, workspace)
}

func TestRefundIntoExpiredLotThenSweep(t *testing.T) {
	pool := testdb.New(t)
	workspace := seedWorkspace(t, pool, Solo)
	id := seedBucket(t, pool, workspace, "included", 10, testNow.Add(time.Minute))
	r := reserveForSettlement(t, pool, workspace, "expired", 10, testNow)
	now := testNow.Add(2 * time.Minute)
	if _, err := Refund(context.Background(), pool, r.ID, now); err != nil {
		t.Fatal(err)
	}
	bucketBalance(t, pool, id, 10)
	available, err := Available(context.Background(), pool, workspace, now)
	if err != nil || available != 0 {
		t.Fatalf("Available = %d, %v", available, err)
	}
	assertInvariants(t, pool, workspace)
	if expired, err := ExpireDue(context.Background(), pool, now); err != nil || expired != 10 {
		t.Fatalf("ExpireDue = %d, %v", expired, err)
	}
	bucketBalance(t, pool, id, 0)
	assertInvariants(t, pool, workspace)
}

func TestReleaseStaleRefunds(t *testing.T) {
	pool := testdb.New(t)
	workspace := seedWorkspace(t, pool, Solo)
	id := seedBucket(t, pool, workspace, "included", 100, testNow.Add(time.Hour))
	stale := reserveForSettlement(t, pool, workspace, "stale", 10, testNow.Add(-20*time.Minute))
	fresh := reserveForSettlement(t, pool, workspace, "fresh", 10, testNow)
	boundary := reserveForSettlement(t, pool, workspace, "boundary", 10, testNow.Add(-15*time.Minute))
	settled := reserveForSettlement(t, pool, workspace, "settled", 10, testNow.Add(-20*time.Minute))
	if _, err := Settle(context.Background(), pool, settled.ID, 10, testNow); err != nil {
		t.Fatal(err)
	}
	for _, want := range []int{1, 0} {
		got, err := ReleaseStale(context.Background(), pool, 15*time.Minute, testNow)
		if err != nil || got != want {
			t.Fatalf("ReleaseStale = %d, %v; want %d", got, err, want)
		}
	}
	countRows(t, pool, `SELECT count(*) FROM credit_reservations WHERE id=$1 AND status='refunded'`, 1, stale.ID)
	countRows(t, pool, `SELECT count(*) FROM credit_reservations WHERE id IN ($1,$2) AND status='reserved'`, 2, fresh.ID, boundary.ID)
	bucketBalance(t, pool, id, 70)
	assertInvariants(t, pool, workspace)
}

func TestConcurrentSettleSameReservation(t *testing.T) {
	pool := testdb.New(t)
	workspace := seedWorkspace(t, pool, Solo)
	id := seedBucket(t, pool, workspace, "included", 10, testNow.Add(time.Hour))
	r := reserveForSettlement(t, pool, workspace, "concurrent", 10, testNow)
	ctx, cancel := context.WithTimeout(context.Background(), 8*time.Second)
	defer cancel()
	var wg sync.WaitGroup
	errs := make([]error, 20)
	for i := range errs {
		wg.Go(func() {
			var got Reservation
			got, errs[i] = Settle(ctx, pool, r.ID, 5, testNow)
			if errs[i] == nil && (got.Status != "committed" || got.CommittedAmount != 5) {
				errs[i] = fmt.Errorf("unexpected settlement: %+v", got)
			}
		})
	}
	wg.Wait()
	for _, err := range errs {
		if err != nil {
			t.Fatal(err)
		}
	}
	bucketBalance(t, pool, id, 5)
	countRows(t, pool, `SELECT count(*) FROM credit_ledger_entries WHERE entry_type IN ('commit','refund')`, 2)
	assertInvariants(t, pool, workspace)
}

func TestSettleRefundsAcrossLotsInReverseOrder(t *testing.T) {
	pool := testdb.New(t)
	workspace := seedWorkspace(t, pool, Solo)
	topup := seedBucket(t, pool, workspace, "topup", 3, testNow.Add(time.Hour))
	included := seedBucket(t, pool, workspace, "included", 3, testNow.Add(time.Hour))
	rollover := seedBucket(t, pool, workspace, "rollover", 4, testNow.Add(time.Hour))
	r := reserveForSettlement(t, pool, workspace, "reverse", 10, testNow)
	if _, err := Settle(context.Background(), pool, r.ID, 5, testNow); err != nil {
		t.Fatal(err)
	}
	bucketBalance(t, pool, rollover, 0)
	bucketBalance(t, pool, included, 2)
	bucketBalance(t, pool, topup, 3)
	var ordered bool
	err := pool.QueryRow(context.Background(), `SELECT array_agg(bucket_id ORDER BY id)=ARRAY[$2::bigint,$3::bigint]
		FROM credit_ledger_entries WHERE reservation_id=$1 AND entry_type='refund'`, r.ID, topup, included).Scan(&ordered)
	if err != nil || !ordered {
		t.Fatalf("refund order correct = %t, %v", ordered, err)
	}
	countRows(t, pool, `SELECT count(*) FROM credit_ledger_entries WHERE reservation_id=$1 AND entry_type='commit'
		AND ((bucket_id=$2 AND amount=4) OR (bucket_id=$3 AND amount=1))`, 2, r.ID, rollover, included)
	assertInvariants(t, pool, workspace)
}

func TestSettlementAndExpiryRollbackOnEntryFailure(t *testing.T) {
	for _, operation := range []string{"settle", "expire"} {
		t.Run(operation, func(t *testing.T) {
			pool := testdb.New(t)
			workspace := seedWorkspace(t, pool, Solo)
			id := seedBucket(t, pool, workspace, "included", 10, testNow.Add(time.Minute))
			r := reserveForSettlement(t, pool, workspace, "rollback", 6, testNow)
			ctx := context.Background()
			_, err := pool.Exec(ctx, `CREATE FUNCTION reject_settlement_entry() RETURNS trigger LANGUAGE plpgsql AS
				$$ BEGIN
					IF NEW.entry_type IN ('refund','expire') THEN
						RAISE EXCEPTION 'test settlement write failure';
					END IF;
					RETURN NEW;
				END $$;
				CREATE TRIGGER reject_settlement_entry BEFORE INSERT ON credit_ledger_entries
				FOR EACH ROW EXECUTE FUNCTION reject_settlement_entry()`)
			if err != nil {
				t.Fatal(err)
			}
			if operation == "settle" {
				_, err = Settle(ctx, pool, r.ID, 3, testNow)
			} else {
				_, err = ExpireDue(ctx, pool, testNow.Add(time.Minute))
			}
			if err == nil {
				t.Fatal("expected ledger entry failure")
			}
			bucketBalance(t, pool, id, 4)
			countRows(t, pool, `SELECT count(*) FROM credit_ledger_entries`, 1)
			countRows(t, pool, `SELECT count(*) FROM credit_reservations WHERE id=$1 AND status='reserved' AND settled_at IS NULL`, 1, r.ID)
			assertInvariants(t, pool, workspace)
		})
	}
}

func TestCheckInvariantsDetectsCorruption(t *testing.T) {
	for _, fixture := range []struct {
		name, status, change, repair string
	}{
		{"lot", "reserved", `UPDATE credit_buckets SET balance_remaining=1`, `UPDATE credit_buckets SET balance_remaining=0`},
		{"reserve amount", "reserved", `UPDATE credit_reservations SET amount=11`, `UPDATE credit_reservations SET amount=10`},
		{"reserved commit", "reserved", `INSERT INTO credit_ledger_entries (workspace_id,bucket_id,reservation_id,entry_type,amount,created_at) SELECT workspace_id,bucket_id,reservation_id,'commit',1,created_at FROM credit_ledger_entries`, `DELETE FROM credit_ledger_entries WHERE entry_type='commit'`},
		{"reserved refund", "reserved", `INSERT INTO credit_ledger_entries (workspace_id,bucket_id,reservation_id,entry_type,amount,created_at) SELECT workspace_id,bucket_id,reservation_id,'refund',1,created_at FROM credit_ledger_entries; UPDATE credit_buckets SET balance_remaining=1`, `DELETE FROM credit_ledger_entries WHERE entry_type='refund'; UPDATE credit_buckets SET balance_remaining=0`},
		{"committed amount", "committed", `UPDATE credit_reservations SET committed_amount=6`, `UPDATE credit_reservations SET committed_amount=7`},
		{"committed refund", "committed", `DELETE FROM credit_ledger_entries WHERE entry_type='refund'; UPDATE credit_buckets SET balance_remaining=0`, `INSERT INTO credit_ledger_entries (workspace_id,bucket_id,reservation_id,entry_type,amount,created_at) SELECT workspace_id,bucket_id,reservation_id,'refund',3,created_at FROM credit_ledger_entries WHERE entry_type='reserve'; UPDATE credit_buckets SET balance_remaining=3`},
		{"refunded amount", "refunded", `UPDATE credit_reservations SET committed_amount=1`, `UPDATE credit_reservations SET committed_amount=0`},
		{"refunded refund", "refunded", `DELETE FROM credit_ledger_entries WHERE entry_type='refund'; UPDATE credit_buckets SET balance_remaining=0`, `INSERT INTO credit_ledger_entries (workspace_id,bucket_id,reservation_id,entry_type,amount,created_at) SELECT workspace_id,bucket_id,reservation_id,'refund',10,created_at FROM credit_ledger_entries WHERE entry_type='reserve'; UPDATE credit_buckets SET balance_remaining=10`},
	} {
		t.Run(fixture.name, func(t *testing.T) {
			pool := testdb.New(t)
			workspace := seedWorkspace(t, pool, Solo)
			other := seedWorkspace(t, pool, Solo)
			id := seedBucket(t, pool, workspace, "included", 10, testNow.Add(time.Hour))
			r := reserveForSettlement(t, pool, workspace, "invariant", 10, testNow)
			if fixture.status != "reserved" {
				used := int64(7)
				if fixture.status == "refunded" {
					used = 0
				}
				if _, err := Settle(context.Background(), pool, r.ID, used, testNow); err != nil {
					t.Fatal(err)
				}
			}
			if _, err := pool.Exec(context.Background(), fixture.change); err != nil {
				t.Fatal(err)
			}
			err := CheckInvariants(context.Background(), pool, workspace)
			want := "reservation " + r.ID
			if fixture.name == "lot" {
				want = fmt.Sprintf("lot %d:", id)
			}
			if err == nil || !strings.Contains(err.Error(), want) {
				t.Fatalf("expected invariant error naming lot/reservation, got %v", err)
			}
			assertInvariants(t, pool, other)
			if _, err := pool.Exec(context.Background(), fixture.repair); err != nil {
				t.Fatal(err)
			}
			assertInvariants(t, pool, workspace)
		})
	}
}

func TestConcurrentStaleReleaseAndCommit(t *testing.T) {
	pool := testdb.New(t)
	workspace := seedWorkspace(t, pool, Solo)
	id := seedBucket(t, pool, workspace, "included", 200, testNow.Add(time.Hour))
	reservations := make([]Reservation, 20)
	for i := range reservations {
		reservations[i] = reserveForSettlement(t, pool, workspace, fmt.Sprint(i), 10, testNow.Add(-20*time.Minute))
	}
	ctx, cancel := context.WithTimeout(context.Background(), 8*time.Second)
	defer cancel()
	var wg sync.WaitGroup
	errs := make([]error, len(reservations)+4)
	counts := make([]int, 4)
	start := make(chan struct{})
	for i, r := range reservations {
		wg.Go(func() {
			<-start
			_, err := Settle(ctx, pool, r.ID, 10, testNow)
			if !errors.Is(err, ErrIdempotencyConflict) {
				errs[i] = err
			}
		})
	}
	for i := range counts {
		wg.Go(func() {
			<-start
			counts[i], errs[len(reservations)+i] = ReleaseStale(ctx, pool, 15*time.Minute, testNow)
		})
	}
	close(start)
	wg.Wait()
	for _, err := range errs {
		if err != nil {
			t.Fatal(err)
		}
	}
	total := 0
	for _, count := range counts {
		total += count
	}
	countRows(t, pool, `SELECT count(*) FROM credit_reservations WHERE status='refunded'`, int64(total))
	countRows(t, pool, `SELECT count(*) FROM credit_reservations WHERE status='reserved'`, 0)
	bucketBalance(t, pool, id, int64(total)*10)
	assertInvariants(t, pool, workspace)
}

func TestConcurrentExpiryAndRefund(t *testing.T) {
	pool := testdb.New(t)
	workspace := seedWorkspace(t, pool, Solo)
	id := seedBucket(t, pool, workspace, "included", 10, testNow.Add(time.Minute))
	r := reserveForSettlement(t, pool, workspace, "expiry-race", 10, testNow)
	ctx, cancel := context.WithTimeout(context.Background(), 8*time.Second)
	defer cancel()
	now := testNow.Add(time.Minute)
	var wg sync.WaitGroup
	errs := make([]error, 20)
	for i := range errs {
		wg.Go(func() {
			if i%2 == 0 {
				_, errs[i] = Refund(ctx, pool, r.ID, now)
			} else {
				_, errs[i] = ExpireDue(ctx, pool, now)
			}
		})
	}
	wg.Wait()
	for _, err := range errs {
		if err != nil {
			t.Fatal(err)
		}
	}
	if _, err := ExpireDue(ctx, pool, now); err != nil {
		t.Fatal(err)
	}
	bucketBalance(t, pool, id, 0)
	countRows(t, pool, `SELECT count(*) FROM credit_ledger_entries WHERE entry_type='refund' AND amount=10`, 1)
	countRows(t, pool, `SELECT count(*) FROM credit_ledger_entries WHERE entry_type='expire' AND amount=10`, 1)
	assertInvariants(t, pool, workspace)
}

func TestSettlementBigintCycles(t *testing.T) {
	pool := testdb.New(t)
	workspace := seedWorkspace(t, pool, Solo)
	id := seedBucket(t, pool, workspace, "included", math.MaxInt64, testNow.Add(time.Hour))
	for i := range 3 {
		r := reserveForSettlement(t, pool, workspace, fmt.Sprint(i), math.MaxInt64, testNow)
		if _, err := Refund(context.Background(), pool, r.ID, testNow); err != nil {
			t.Fatal(err)
		}
	}
	bucketBalance(t, pool, id, math.MaxInt64)
	assertInvariants(t, pool, workspace)
}

func TestExpireDueRejectsTotalOverflow(t *testing.T) {
	pool := testdb.New(t)
	workspace := seedWorkspace(t, pool, Solo)
	first := seedBucket(t, pool, workspace, "included", math.MaxInt64, testNow)
	second := seedBucket(t, pool, workspace, "topup", 1, testNow)
	got, err := ExpireDue(context.Background(), pool, testNow)
	if err == nil || got != 0 {
		t.Fatalf("ExpireDue overflow = %d, %v", got, err)
	}
	bucketBalance(t, pool, first, math.MaxInt64)
	bucketBalance(t, pool, second, 1)
	countRows(t, pool, `SELECT count(*) FROM credit_ledger_entries`, 0)
	assertInvariants(t, pool, workspace)
}

func TestConcurrentReserveAndSettleLockOrder(t *testing.T) {
	pool := testdb.New(t)
	workspace := seedWorkspace(t, pool, Solo)
	key := seedAPIKey(t, pool, workspace, 200)
	seedBucket(t, pool, workspace, "included", 100, testNow.Add(time.Hour))
	seedBucket(t, pool, workspace, "rollover", 100, testNow.Add(2*time.Hour))
	r, err := Reserve(context.Background(), pool, ReserveRequest{
		WorkspaceID: workspace, APIKeyID: key, IdempotencyKey: "initial", Amount: 110,
	}, testNow)
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 8*time.Second)
	defer cancel()
	var wg sync.WaitGroup
	errs := make([]error, 20)
	for i := range errs {
		wg.Go(func() {
			if i%2 == 0 {
				_, errs[i] = Settle(ctx, pool, r.ID, 50, testNow)
			} else {
				_, errs[i] = Reserve(ctx, pool, ReserveRequest{
					WorkspaceID: workspace, APIKeyID: key, IdempotencyKey: fmt.Sprint(i), Amount: 1,
				}, testNow)
			}
		})
	}
	wg.Wait()
	for _, err := range errs {
		if err != nil {
			t.Fatal(err)
		}
	}
	available, err := Available(ctx, pool, workspace, testNow)
	if err != nil || available != 140 {
		t.Fatalf("Available = %d, %v", available, err)
	}
	assertInvariants(t, pool, workspace)
}
