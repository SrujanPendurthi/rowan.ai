package api

import (
	"context"
	"crypto/rand"
	"crypto/sha256"
	"encoding/base32"
	"errors"
	"fmt"
	"math"
	"net/http"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/jackc/pgx/v5"
	"golang.org/x/time/rate"
)

const tokenPrefix = "rk_live_"

var errUnauthorized = errors.New("unauthorized")

type apiKey struct {
	ID, WorkspaceID string
	SpendCap        int64
	RatePerMinute   int
}

// HashKey returns the stored form of an API token.
func HashKey(token string) []byte {
	sum := sha256.Sum256([]byte(token))
	return sum[:]
}

// NewToken returns a fresh plaintext key: rk_live_ plus 32 random bytes in base32.
func NewToken() (string, error) {
	var raw [32]byte
	if _, err := rand.Read(raw[:]); err != nil {
		return "", fmt.Errorf("generate API token: %w", err)
	}
	return tokenPrefix + base32.StdEncoding.WithPadding(base32.NoPadding).EncodeToString(raw[:]), nil
}

func (s *server) authenticate(ctx context.Context, r *http.Request) (apiKey, error) {
	scheme, token, ok := strings.Cut(r.Header.Get("Authorization"), " ")
	token = strings.TrimSpace(token)
	if !ok || !strings.EqualFold(scheme, "Bearer") || token == "" {
		return apiKey{}, errUnauthorized
	}
	var k apiKey
	err := s.Pool.QueryRow(ctx, `SELECT id::text, workspace_id::text, monthly_spend_cap, rate_per_minute
		FROM api_keys WHERE key_hash=$1 AND revoked_at IS NULL`, HashKey(token)).
		Scan(&k.ID, &k.WorkspaceID, &k.SpendCap, &k.RatePerMinute)
	if errors.Is(err, pgx.ErrNoRows) {
		return apiKey{}, errUnauthorized
	}
	if err != nil {
		return apiKey{}, fmt.Errorf("look up API key: %w", err)
	}
	return k, nil
}

// limiters holds one token bucket per key.
// ponytail: per-process limiter; move to Redis/PG if more than one instance serves traffic.
type limiters struct {
	mu sync.Mutex
	m  map[string]*rate.Limiter
}

// limit returns how long the caller must wait, or 0 if the request is allowed.
func (s *server) limit(k apiKey, now time.Time) time.Duration {
	perMinute := max(k.RatePerMinute, 1)
	s.limits.mu.Lock()
	defer s.limits.mu.Unlock()
	if s.limits.m == nil {
		s.limits.m = make(map[string]*rate.Limiter)
	}
	lim := s.limits.m[k.ID]
	if lim == nil || lim.Burst() != perMinute {
		lim = rate.NewLimiter(rate.Limit(float64(perMinute)/60.0), perMinute)
		s.limits.m[k.ID] = lim
	}
	res := lim.ReserveN(now, 1)
	if !res.OK() {
		return time.Minute
	}
	if d := res.DelayFrom(now); d > 0 {
		res.CancelAt(now)
		return d
	}
	return 0
}

func rateLimited(wait time.Duration) response {
	secs := max(int(math.Ceil(wait.Seconds())), 1)
	resp := errorResponse(429, "rate_limited", "rate limit exceeded", nil)
	resp.headers["Retry-After"] = strconv.Itoa(secs)
	return resp
}
