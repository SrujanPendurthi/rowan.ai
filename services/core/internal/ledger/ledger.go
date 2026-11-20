// Package ledger grants and reserves credits in transactional FIFO lots.
package ledger

import (
	"context"
	"errors"
	"fmt"
	"sort"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgxpool"

	"rowan.ai/rowan/services/core/internal/db"
)

var (
	ErrInsufficientCredits = errors.New("insufficient credits")
	ErrSpendCapExceeded    = errors.New("spend cap exceeded")
	ErrIdempotencyConflict = errors.New("idempotency conflict")
	ErrInvalidRequest      = errors.New("invalid ledger request")
	errConcurrentReplay    = errors.New("concurrent reservation exists")
)

type ReserveRequest struct {
	WorkspaceID, APIKeyID, IdempotencyKey string
	Amount                                int64
}

type Reservation struct {
	ID                      string
	Amount, CommittedAmount int64
	Status                  string
}

// GrantPeriod grants a subscription lot once per UTC calendar period date.
func GrantPeriod(ctx context.Context, pool *pgxpool.Pool, workspaceID string, periodStart time.Time) error {
	periodStart = periodStart.UTC()
	periodDate := periodStart.Format("2006-01-02")
	err := db.InTx(ctx, pool, func(tx pgx.Tx) error {
		var tier Tier
		// Serialize grants without blocking reservation foreign-key checks.
		if err := tx.QueryRow(ctx, `SELECT tier FROM workspaces WHERE id=$1 FOR NO KEY UPDATE`, workspaceID).Scan(&tier); err != nil {
			return err
		}
		var exists bool
		if err := tx.QueryRow(ctx, `SELECT EXISTS (SELECT 1 FROM credit_buckets
			WHERE workspace_id=$1 AND period_start=$2 AND bucket_type IN ('included','rollover'))`, workspaceID, periodDate).Scan(&exists); err != nil {
			return err
		}
		if exists {
			return nil
		}
		included, days, _ := TierSpec(tier)
		if included <= 0 {
			return fmt.Errorf("unknown tier %q: %w", tier, ErrInvalidRequest)
		}
		rows, err := tx.Query(ctx, `SELECT id FROM credit_buckets WHERE workspace_id=$1
			AND bucket_type='included' AND balance_remaining>0 AND period_start<$2 ORDER BY id FOR UPDATE`, workspaceID, periodDate)
		if err != nil {
			return err
		}
		ids, err := pgx.CollectRows(rows, pgx.RowTo[int64])
		if err != nil {
			return err
		}
		for _, id := range ids {
			if _, err := tx.Exec(ctx, `UPDATE credit_buckets SET bucket_type='rollover' WHERE id=$1`, id); err != nil {
				return err
			}
		}
		return grant(ctx, tx, workspaceID, "included", included, periodStart, periodStart.AddDate(0, 0, days), periodDate)
	})
	if err != nil {
		return fmt.Errorf("ledger: grant period: %w", invalidRequestError(err))
	}
	return nil
}

// GrantTopup adds a purchased lot that expires after 365 days.
func GrantTopup(ctx context.Context, pool *pgxpool.Pool, workspaceID string, credits int64, at time.Time) error {
	if credits <= 0 {
		return fmt.Errorf("ledger: grant topup: %w", ErrInvalidRequest)
	}
	at = at.UTC()
	if err := db.InTx(ctx, pool, func(tx pgx.Tx) error {
		if err := requireWorkspace(ctx, tx, workspaceID); err != nil {
			return err
		}
		return grant(ctx, tx, workspaceID, "topup", credits, at, at.AddDate(0, 0, 365), nil)
	}); err != nil {
		return fmt.Errorf("ledger: grant topup: %w", invalidRequestError(err))
	}
	return nil
}

// invalidRequestError classifies missing identities and malformed UUID input.
func invalidRequestError(err error) error {
	var pgErr *pgconn.PgError
	if errors.Is(err, pgx.ErrNoRows) || (errors.As(err, &pgErr) && pgErr.Code == "22P02") {
		return fmt.Errorf("%w: %w", ErrInvalidRequest, err)
	}
	return err
}

func requireWorkspace(ctx context.Context, tx pgx.Tx, workspaceID string) error {
	var id string
	return tx.QueryRow(ctx, `SELECT id::text FROM workspaces WHERE id=$1 FOR KEY SHARE`, workspaceID).Scan(&id)
}

func grant(ctx context.Context, tx pgx.Tx, workspaceID, kind string, credits int64, at, expires time.Time, period any) error {
	var id int64
	if err := tx.QueryRow(ctx, `INSERT INTO credit_buckets
		(workspace_id, bucket_type, granted, balance_remaining, granted_at, expires_at, period_start)
		VALUES ($1,$2,$3,$3,$4,$5,$6) RETURNING id`, workspaceID, kind, credits, at, expires, period).Scan(&id); err != nil {
		return err
	}
	_, err := tx.Exec(ctx, `INSERT INTO credit_ledger_entries
		(workspace_id, bucket_id, entry_type, amount, created_at) VALUES ($1,$2,'grant',$3,$4)`, workspaceID, id, credits, at)
	return err
}

type rowQuerier interface {
	QueryRow(context.Context, string, ...any) pgx.Row
}

func lookupReservation(ctx context.Context, q rowQuerier, req ReserveRequest) (Reservation, error) {
	var result Reservation
	err := q.QueryRow(ctx, `SELECT id::text, amount, committed_amount, status FROM credit_reservations
		WHERE workspace_id=$1 AND idempotency_key=$2`, req.WorkspaceID, req.IdempotencyKey).
		Scan(&result.ID, &result.Amount, &result.CommittedAmount, &result.Status)
	if err == nil && result.Amount != req.Amount {
		return Reservation{}, ErrIdempotencyConflict
	}
	return result, err
}

// Reserve deducts available credits once per workspace idempotency key.
func Reserve(ctx context.Context, pool *pgxpool.Pool, req ReserveRequest, now time.Time) (Reservation, error) {
	if req.WorkspaceID == "" || req.IdempotencyKey == "" || req.Amount <= 0 {
		return Reservation{}, fmt.Errorf("ledger: reserve: %w", ErrInvalidRequest)
	}
	now = now.UTC()
	var result Reservation
	err := db.InTx(ctx, pool, func(tx pgx.Tx) error {
		var err error
		result, err = lookupReservation(ctx, tx, req)
		if !errors.Is(err, pgx.ErrNoRows) {
			return err
		}
		if err := requireWorkspace(ctx, tx, req.WorkspaceID); err != nil {
			return err
		}
		if req.APIKeyID != "" {
			if err := checkSpendCap(ctx, tx, req, now); err != nil {
				return err
			}
		}
		// ponytail: Lock all live buckets so reserves in one workspace serialize.
		// If throughput matters, upgrade to a workspace allocator with narrower lot locks.
		buckets, err := lockBuckets(ctx, tx, req.WorkspaceID, now)
		if err != nil {
			return err
		}
		// Subtract from the request rather than summing bigint balances, which can overflow.
		need := req.Amount
		for _, bucket := range buckets {
			if bucket.balance >= need {
				need = 0
				break
			}
			need -= bucket.balance
		}
		if need > 0 {
			return ErrInsufficientCredits
		}
		err = tx.QueryRow(ctx, `INSERT INTO credit_reservations
			(workspace_id, api_key_id, idempotency_key, amount, status, created_at)
			VALUES ($1,NULLIF($2,'')::uuid,$3,$4,'reserved',$5)
			ON CONFLICT (workspace_id, idempotency_key) DO NOTHING
			RETURNING id::text, amount, committed_amount, status`, req.WorkspaceID, req.APIKeyID,
			req.IdempotencyKey, req.Amount, now).Scan(&result.ID, &result.Amount, &result.CommittedAmount, &result.Status)
		if errors.Is(err, pgx.ErrNoRows) {
			return errConcurrentReplay
		}
		if err != nil {
			return err
		}
		return deduct(ctx, tx, req, result.ID, buckets, now)
	})
	if err == nil {
		return result, nil
	}
	if errors.Is(err, errConcurrentReplay) || errors.Is(err, ErrInsufficientCredits) || errors.Is(err, ErrSpendCapExceeded) {
		// A same-key winner can consume the last credits or cap while this call waits.
		// InTx has rolled back before this fresh read, releasing every row lock.
		replay, replayErr := lookupReservation(ctx, pool, req)
		if replayErr == nil {
			return replay, nil
		}
		if !errors.Is(replayErr, pgx.ErrNoRows) {
			return Reservation{}, fmt.Errorf("ledger: reserve: %w", replayErr)
		}
	}
	return Reservation{}, fmt.Errorf("ledger: reserve: %w", invalidRequestError(err))
}

func checkSpendCap(ctx context.Context, tx pgx.Tx, req ReserveRequest, now time.Time) error {
	var cap int64
	if err := tx.QueryRow(ctx, `SELECT monthly_spend_cap FROM api_keys
		WHERE id=$1 AND workspace_id=$2 FOR UPDATE`, req.APIKeyID, req.WorkspaceID).Scan(&cap); err != nil {
		return fmt.Errorf("lock API key: %w", err)
	}
	monthStart := time.Date(now.Year(), now.Month(), 1, 0, 0, 0, 0, time.UTC)
	var exceeded bool
	// PostgreSQL sum(bigint) is numeric, avoiding overflow near the bigint limit.
	err := tx.QueryRow(ctx, `SELECT COALESCE(sum(CASE status
		WHEN 'reserved' THEN amount WHEN 'committed' THEN committed_amount ELSE 0 END),0)
		> ($2::numeric-$3::numeric) FROM credit_reservations
		WHERE api_key_id=$1 AND created_at >= $4 AND created_at < $5`,
		req.APIKeyID, cap, req.Amount, monthStart, monthStart.AddDate(0, 1, 0)).Scan(&exceeded)
	if err != nil {
		return err
	}
	if exceeded {
		return ErrSpendCapExceeded
	}
	return nil
}

type creditBucket struct {
	id      int64
	balance int64
	kind    string
	expires time.Time
}

func lockBuckets(ctx context.Context, tx pgx.Tx, workspaceID string, now time.Time) ([]creditBucket, error) {
	// Settlement must acquire these locks in the same id order.
	rows, err := tx.Query(ctx, `SELECT id, balance_remaining, bucket_type, expires_at
		FROM credit_buckets WHERE workspace_id=$1 AND expires_at>$2 AND balance_remaining>0
		ORDER BY id FOR UPDATE`, workspaceID, now)
	if err != nil {
		return nil, err
	}
	buckets, err := pgx.CollectRows(rows, func(row pgx.CollectableRow) (creditBucket, error) {
		var bucket creditBucket
		err := row.Scan(&bucket.id, &bucket.balance, &bucket.kind, &bucket.expires)
		return bucket, err
	})
	if err != nil {
		return nil, err
	}
	priority := map[string]int{"rollover": 0, "included": 1, "topup": 2}
	sort.Slice(buckets, func(i, j int) bool {
		a, b := buckets[i], buckets[j]
		if priority[a.kind] != priority[b.kind] {
			return priority[a.kind] < priority[b.kind]
		}
		if !a.expires.Equal(b.expires) {
			return a.expires.Before(b.expires)
		}
		return a.id < b.id
	})
	return buckets, nil
}

func deduct(ctx context.Context, tx pgx.Tx, req ReserveRequest, reservationID string, buckets []creditBucket, now time.Time) error {
	remaining := req.Amount
	for _, bucket := range buckets {
		if remaining == 0 {
			break
		}
		amount := min(remaining, bucket.balance)
		if _, err := tx.Exec(ctx, `UPDATE credit_buckets SET balance_remaining=balance_remaining-$2 WHERE id=$1`, bucket.id, amount); err != nil {
			return err
		}
		if _, err := tx.Exec(ctx, `INSERT INTO credit_ledger_entries
			(workspace_id, bucket_id, reservation_id, entry_type, amount, created_at)
			VALUES ($1,$2,$3,'reserve',$4,$5)`, req.WorkspaceID, bucket.id, reservationID, amount, now); err != nil {
			return err
		}
		remaining -= amount
	}
	return nil
}

// Available returns unreserved credits whose lots have not expired at now.
func Available(ctx context.Context, pool *pgxpool.Pool, workspaceID string, now time.Time) (int64, error) {
	var available int64
	err := pool.QueryRow(ctx, `SELECT COALESCE(sum(balance_remaining),0) FROM credit_buckets
		WHERE workspace_id=$1 AND expires_at>$2`, workspaceID, now).Scan(&available)
	if err != nil {
		return 0, fmt.Errorf("ledger: available: %w", err)
	}
	return available, nil
}
