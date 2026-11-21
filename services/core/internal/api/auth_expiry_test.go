package api

import (
	"context"
	"errors"
	"net/http"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgconn"

	"rowan.ai/rowan/services/core/internal/ledger"
)

func TestAuthRejectsExpiredSessionKey(t *testing.T) {
	e := newEnv(t)
	ctx := context.Background()
	ws := e.workspace(ledger.Solo)
	userID := scalar[string](t, e.pool, `INSERT INTO users(workspace_id,email,password_hash)
		VALUES ($1,$2,$3) RETURNING id::text`, ws, "founder@example.com", "test-password-hash")
	token := "rk_sess_test-expiry"
	expiresAt := testNow.Add(time.Minute)
	if _, err := e.pool.Exec(ctx, `INSERT INTO api_keys
		(workspace_id,key_hash,label,monthly_spend_cap,rate_per_minute,kind,user_id,expires_at)
		VALUES ($1,$2,'session',9223372036854775807,300,'session',$3,$4)`,
		ws, HashKey(token), userID, expiresAt); err != nil {
		t.Fatalf("insert session key: %v", err)
	}

	wantStatus(t, e.do(http.MethodGet, "/v1/credits", token, "", nil), http.StatusOK)

	e.advance(expiresAt.Sub(testNow))
	rec := e.do(http.MethodGet, "/v1/credits", token, "", nil)
	wantError(t, rec, http.StatusUnauthorized, "unauthorized")
	if got := rec.Header().Get("WWW-Authenticate"); got != "Bearer" {
		t.Fatalf("WWW-Authenticate = %q, want Bearer", got)
	}

	_, agentToken := e.key(ws, 1_000_000, 120)
	wantStatus(t, e.do(http.MethodGet, "/v1/credits", agentToken, "", nil), http.StatusOK)

	_, err := e.pool.Exec(ctx, `INSERT INTO api_keys
		(workspace_id,key_hash,label,monthly_spend_cap,kind,user_id,expires_at)
		VALUES ($1,$2,'session',0,'session',NULL,$3)`, ws, HashKey("rk_sess_invalid-shape"), expiresAt)
	var pgErr *pgconn.PgError
	if !errors.As(err, &pgErr) || pgErr.Code != "23514" || pgErr.ConstraintName != "api_keys_session_shape" {
		t.Fatalf("insert session without user: got %v, want check violation api_keys_session_shape", err)
	}
}
