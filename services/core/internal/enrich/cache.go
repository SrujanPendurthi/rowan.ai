package enrich

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"sort"
	"strings"
	"time"

	"github.com/jackc/pgx/v5"

	"rowan.ai/rowan/services/core/internal/db"
	"rowan.ai/rowan/services/core/internal/resolver"
)

type cacheRow struct {
	zeroHit bool
	payload []byte
}

// identity is the provider-independent description of a candidate: normalized name, state, and IDs.
func identity(c Candidate) string {
	ids := make([]string, 0, len(c.IDs))
	for k, v := range c.IDs {
		ids = append(ids, strings.ToLower(strings.TrimSpace(k))+"="+strings.ToLower(strings.TrimSpace(v)))
	}
	sort.Strings(ids)
	return resolver.NormalizeName(c.LegalName) + "|" + strings.ToUpper(strings.TrimSpace(c.State)) + "|" + strings.Join(ids, ",")
}

// cacheKey scopes cached provider answers to one workspace and one candidate identity.
// The client-supplied Ref is deliberately not part of it: refs are not trusted to name a company.
func cacheKey(workspaceID string, c Candidate) string {
	sum := sha256.Sum256([]byte(identity(c)))
	return workspaceID + ":" + hex.EncodeToString(sum[:])
}

// requestHash fingerprints a run request: buy box plus every candidate's ref and identity, order-independent.
func requestHash(buyBoxID string, cs []Candidate) []byte {
	lines := make([]string, 0, len(cs))
	for _, c := range cs {
		lines = append(lines, c.Ref+"|"+identity(c))
	}
	sort.Strings(lines)
	sum := sha256.Sum256([]byte(buyBoxID + "\n" + strings.Join(lines, "\n")))
	return sum[:]
}

func cacheKeys(workspaceID string, cs []Candidate) []string {
	out := make([]string, len(cs))
	for i, c := range cs {
		out[i] = cacheKey(workspaceID, c)
	}
	return out
}

// freshCache returns cache rows by cache_key, fetched strictly within CacheTTL of now.
func (s *Service) freshCache(ctx context.Context, keys []string, now time.Time) (map[string]cacheRow, error) {
	rows, err := s.Pool.Query(ctx, `SELECT cache_key, zero_hit, payload FROM source_fetch_cache
		WHERE provider=$1 AND cache_key = ANY($2) AND fetched_at > $3`,
		s.Provider.Name(), keys, now.Add(-s.CacheTTL))
	if err != nil {
		return nil, fmt.Errorf("enrich: read cache: %w", err)
	}
	defer rows.Close()
	out := map[string]cacheRow{}
	for rows.Next() {
		var key string
		var row cacheRow
		if err := rows.Scan(&key, &row.zeroHit, &row.payload); err != nil {
			return nil, fmt.Errorf("enrich: scan cache: %w", err)
		}
		out[key] = row
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("enrich: read cache: %w", err)
	}
	return out, nil
}

func upsertCache(ctx context.Context, tx pgx.Tx, provider, key string, payload []byte, zeroHit bool, now time.Time) error {
	hash := sha256.Sum256(payload)
	_, err := tx.Exec(ctx, `INSERT INTO source_fetch_cache(provider,cache_key,content_hash,payload,zero_hit,fetched_at)
		VALUES ($1,$2,$3,$4,$5,$6)
		ON CONFLICT (provider,cache_key) DO UPDATE SET content_hash=EXCLUDED.content_hash,
			payload=EXCLUDED.payload, zero_hit=EXCLUDED.zero_hit, fetched_at=EXCLUDED.fetched_at`,
		provider, key, hash[:], payload, zeroHit, now)
	if err != nil {
		return fmt.Errorf("upsert fetch cache: %w", err)
	}
	return nil
}

// resolveAll resolves records into workspaceID; the caller already holds resolver.LockWorkspace.
// Cached payloads carry no workspace, so the target workspace always overrides the record's own.
func resolveAll(ctx context.Context, tx pgx.Tx, workspaceID string, recs []resolver.SourceRecordInput, now time.Time) ([]string, error) {
	ids := make([]string, 0, len(recs))
	for _, in := range recs {
		in.WorkspaceID = workspaceID
		if in.FetchedAt.IsZero() {
			in.FetchedAt = now
		}
		res, err := resolver.Resolve(ctx, tx, in, now)
		if err != nil {
			return nil, fmt.Errorf("resolve %s/%s: %w", in.SourceSystem, in.ExternalRef, err)
		}
		ids = append(ids, res.CanonicalID)
	}
	return ids, nil
}

// storeLookup persists a provider answer: hits resolve and cache atomically; zero-hits cache only.
func (s *Service) storeLookup(ctx context.Context, workspaceID, key string, recs []resolver.SourceRecordInput, now time.Time) ([]string, error) {
	var payload []byte
	if len(recs) > 0 {
		stored := make([]resolver.SourceRecordInput, len(recs))
		copy(stored, recs)
		for i := range stored {
			stored[i].WorkspaceID = ""
			if stored[i].FetchedAt.IsZero() {
				stored[i].FetchedAt = now
			}
		}
		var err error
		if payload, err = json.Marshal(stored); err != nil {
			return nil, fmt.Errorf("enrich: marshal cache payload: %w", err)
		}
	}
	var ids []string
	err := db.InTx(ctx, s.Pool, func(tx pgx.Tx) error {
		// Lock order: workspace row before any source_fetch_cache write.
		if err := resolver.LockWorkspace(ctx, tx, workspaceID); err != nil {
			return err
		}
		var err error
		if ids, err = resolveAll(ctx, tx, workspaceID, recs, now); err != nil {
			return err
		}
		return upsertCache(ctx, tx, s.Provider.Name(), key, payload, len(recs) == 0, now)
	})
	if err != nil {
		return nil, fmt.Errorf("enrich: store lookup %s: %w", key, err)
	}
	return ids, nil
}

// resolveCached resolves a cached non-zero-hit payload into workspaceID without touching the cache row.
func (s *Service) resolveCached(ctx context.Context, workspaceID string, payload []byte, now time.Time) ([]string, error) {
	var recs []resolver.SourceRecordInput
	if err := json.Unmarshal(payload, &recs); err != nil {
		return nil, fmt.Errorf("enrich: decode cache payload: %w", err)
	}
	var ids []string
	err := db.InTx(ctx, s.Pool, func(tx pgx.Tx) error {
		if err := resolver.LockWorkspace(ctx, tx, workspaceID); err != nil {
			return err
		}
		var err error
		ids, err = resolveAll(ctx, tx, workspaceID, recs, now)
		return err
	})
	if err != nil {
		return nil, fmt.Errorf("enrich: resolve cached: %w", err)
	}
	return ids, nil
}
