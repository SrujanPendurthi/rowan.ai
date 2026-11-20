package api

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/json"
	"errors"
	"log/slog"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"

	"rowan.ai/rowan/services/core/internal/enrich"
)

// staleAfter is how long an in_progress record blocks retries before a retry may take it over.
// ponytail: matches the 15m enrichment abandoned-run and ledger.ReleaseStale windows; a crash
// between finishing work and storing the response is recovered by the handler's own idempotency.
const staleAfter = 15 * time.Minute

// handlerTimeout bounds every idempotent handler so a live owner is never older than staleAfter.
// ponytail: couples with enrich runTimeout (10m) and staleAfter (15m); keep handlerTimeout < staleAfter,
// and the server WriteTimeout above handlerTimeout.
const handlerTimeout = 10 * time.Minute

type stored struct {
	hash    []byte
	state   string
	status  *int
	body    []byte
	headers []byte
	created time.Time
}

// idempotent wraps a mutating handler: same key and request replays, a different request conflicts.
func (s *server) idempotent(c *call, h handlerFunc) response {
	key := c.r.Header.Get("Idempotency-Key")
	switch {
	case key == "":
		return errorResponse(400, "idempotency_key_required", "Idempotency-Key header is required", nil)
	case len(key) > 255:
		return mapError(badRequest("Idempotency-Key must be 1-255 characters"))
	}
	sum := sha256.Sum256([]byte(c.r.Method + "\n" + c.r.URL.Path + "\n" + string(c.body)))
	hash := sum[:]
	replay, claimed, owned, err := s.claim(c, key, hash)
	if err != nil {
		return mapError(err)
	}
	if !owned {
		return replay
	}
	ctx, cancel := context.WithTimeout(c.r.Context(), handlerTimeout)
	defer cancel()
	c.r = c.r.WithContext(ctx)
	finished := false
	defer func() {
		if !finished {
			s.release(c, key, claimed)
		}
	}()
	resp := s.invoke(c, h)
	finished = true
	if resp.status >= 500 || resp.transient {
		s.release(c, key, claimed)
		return resp
	}
	s.store(c, key, claimed, resp)
	return resp
}

// store records the response, only if the caller still owns the record.
func (s *server) store(c *call, key string, claimed time.Time, resp response) {
	headers, err := json.Marshal(resp.headers)
	if err == nil {
		var tag pgconn.CommandTag
		tag, err = s.Pool.Exec(context.WithoutCancel(c.ctx()), `UPDATE idempotency_records
			SET state='done', status_code=$3, response_body=$4, response_headers=$5
			WHERE workspace_id=$1 AND key=$2 AND created_at=$6`, c.key.WorkspaceID, key, resp.status, resp.body, headers, claimed)
		if err == nil && tag.RowsAffected() == 0 {
			slog.Error("idempotency record superseded; response not stored", "key", key)
		}
	}
	if err != nil {
		// The record stays in_progress and is taken over after staleAfter.
		slog.Error("store idempotent response", "err", err)
	}
}

// release deletes the record only if the caller still owns it (created_at fences a superseded owner).
func (s *server) release(c *call, key string, claimed time.Time) {
	tag, err := s.Pool.Exec(context.WithoutCancel(c.ctx()), `DELETE FROM idempotency_records
		WHERE workspace_id=$1 AND key=$2 AND created_at=$3`, c.key.WorkspaceID, key, claimed)
	if err != nil {
		slog.Error("release idempotency record", "err", err)
	} else if tag.RowsAffected() == 0 {
		slog.Error("idempotency record superseded; not released", "key", key)
	}
}

// claim inserts the in_progress record. owned reports that the caller must run the handler, and
// claimed is the record's created_at fencing token; otherwise replay is the response to send.
func (s *server) claim(c *call, key string, hash []byte) (replay response, claimed time.Time, owned bool, err error) {
	ctx := c.ctx()
	for range 3 {
		// RETURNING gives the stored (microsecond-rounded) created_at, which later fenced writes must match.
		err := s.Pool.QueryRow(ctx, `INSERT INTO idempotency_records(workspace_id,key,request_hash,state,created_at)
			VALUES ($1,$2,$3,'in_progress',$4) ON CONFLICT (workspace_id,key) DO NOTHING RETURNING created_at`,
			c.key.WorkspaceID, key, hash, c.now).Scan(&claimed)
		if err == nil {
			return response{}, claimed, true, nil
		}
		if !errors.Is(err, pgx.ErrNoRows) {
			return response{}, time.Time{}, false, err
		}
		var rec stored
		err = s.Pool.QueryRow(ctx, `SELECT request_hash,state,status_code,response_body,response_headers,created_at
			FROM idempotency_records WHERE workspace_id=$1 AND key=$2`, c.key.WorkspaceID, key).
			Scan(&rec.hash, &rec.state, &rec.status, &rec.body, &rec.headers, &rec.created)
		if errors.Is(err, pgx.ErrNoRows) {
			continue // released between insert and read
		}
		if err != nil {
			return response{}, time.Time{}, false, err
		}
		switch {
		case !bytes.Equal(rec.hash, hash):
			return errorResponse(409, "idempotency_key_reuse", "Idempotency-Key was already used with a different request", nil), time.Time{}, false, nil
		case rec.state == "done":
			resp, err := replayOf(rec)
			return resp, time.Time{}, false, err
		case rec.created.Before(c.now.Add(-staleAfter)):
			err := s.Pool.QueryRow(ctx, `UPDATE idempotency_records SET created_at=$3
				WHERE workspace_id=$1 AND key=$2 AND state='in_progress' AND created_at=$4 RETURNING created_at`,
				c.key.WorkspaceID, key, c.now, rec.created).Scan(&claimed)
			if err == nil {
				return response{}, claimed, true, nil
			}
			if !errors.Is(err, pgx.ErrNoRows) {
				return response{}, time.Time{}, false, err
			}
			continue
		}
		return mapError(enrich.ErrRunInProgress), time.Time{}, false, nil
	}
	return mapError(enrich.ErrRunInProgress), time.Time{}, false, nil
}

func replayOf(rec stored) (response, error) {
	resp := response{headers: map[string]string{}, body: rec.body}
	if rec.status != nil {
		resp.status = *rec.status
	}
	if rec.headers != nil {
		if err := json.Unmarshal(rec.headers, &resp.headers); err != nil {
			return response{}, err
		}
	}
	resp.headers["Idempotent-Replay"] = "true"
	return resp, nil
}
