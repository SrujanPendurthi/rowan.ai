// Package api serves the agent-facing HTTP API: key auth, rate limits, idempotency, and domain routes.
package api

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"runtime/debug"
	"time"

	"github.com/jackc/pgx/v5/pgtype"
	"github.com/jackc/pgx/v5/pgxpool"

	"rowan.ai/rowan/services/core/internal/enrich"
	"rowan.ai/rowan/services/core/internal/evaluate"
	"rowan.ai/rowan/services/core/internal/export"
	"rowan.ai/rowan/services/core/internal/ledger"
	"rowan.ai/rowan/services/core/internal/resolver"
)

const maxBodyBytes = 1 << 20

type Deps struct {
	Pool        *pgxpool.Pool
	Enrich      *enrich.Service
	ArtifactDir string
	Now         func() time.Time
}

type server struct {
	Deps
	limits limiters
}

// call is one authenticated request; body is the raw request body (already size-limited).
type call struct {
	r    *http.Request
	key  apiKey
	body []byte
	now  time.Time
}

func (c *call) ctx() context.Context { return c.r.Context() }

// actor is the audit identity recorded for review, split, and export.
func (c *call) actor() string { return "api_key:" + c.key.ID }

// decode parses the body into v, rejecting unknown fields and trailing data.
func (c *call) decode(v any) error {
	dec := json.NewDecoder(bytes.NewReader(c.body))
	dec.DisallowUnknownFields()
	if err := dec.Decode(v); err != nil {
		return badRequest("invalid JSON body: " + err.Error())
	}
	if _, err := dec.Token(); err != io.EOF {
		return badRequest("invalid JSON body: trailing data")
	}
	return nil
}

// pathID returns a path UUID; a malformed id cannot exist, so it is not found.
func (c *call) pathID(name string) (string, error) {
	id := c.r.PathValue(name)
	if !validUUID(id) {
		return "", errNotFound
	}
	return id, nil
}

func validUUID(s string) bool {
	var id pgtype.UUID
	return id.Scan(s) == nil && id.Valid
}

type handlerFunc func(*server, *call) (response, error)

// New builds the API. Order per request: auth, rate limit, body read, idempotency (mutating routes), handler.
func New(deps Deps) http.Handler {
	if deps.Now == nil {
		deps.Now = time.Now
	}
	s := &server{Deps: deps}
	mux := http.NewServeMux()
	route := func(pattern string, idempotent bool, h handlerFunc) {
		mux.HandleFunc(pattern, s.wrap(idempotent, h))
	}
	route("POST /v1/buy-boxes", true, (*server).createBuyBox)
	route("GET /v1/buy-boxes/{id}", false, (*server).getBuyBox)
	route("GET /v1/buy-boxes/{id}/evaluations", false, (*server).listEvaluations)
	route("POST /v1/discovery/search", false, (*server).discoverySearch)
	route("POST /v1/enrichment/quote", false, (*server).enrichQuote)
	route("POST /v1/enrichment/run", true, (*server).enrichRun)
	route("GET /v1/companies/{id}", false, (*server).getCompany)
	route("POST /v1/companies/{id}/split", true, (*server).splitCompany)
	route("GET /v1/review-queue", false, (*server).reviewQueue)
	route("POST /v1/review-queue/{id}/resolve", true, (*server).resolveReview)
	route("POST /v1/exports", true, (*server).exportCSV)
	route("GET /v1/credits", false, (*server).credits)
	return mux
}

func (s *server) wrap(idempotent bool, h handlerFunc) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		now := s.Now().UTC()
		key, err := s.authenticate(r.Context(), r)
		if err != nil {
			s.write(w, mapError(err))
			return
		}
		if retry := s.limit(key, now); retry > 0 {
			s.write(w, rateLimited(retry))
			return
		}
		body, err := io.ReadAll(http.MaxBytesReader(w, r.Body, maxBodyBytes))
		if err != nil {
			s.write(w, mapError(badRequest("request body unreadable or larger than 1 MiB")))
			return
		}
		c := &call{r: r, key: key, body: body, now: now}
		var resp response
		if idempotent {
			resp = s.idempotent(c, h)
		} else {
			resp = s.invoke(c, h)
		}
		s.write(w, resp)
	}
}

// invoke runs a handler, converting errors and panics into responses. 500s never carry error text.
func (s *server) invoke(c *call, h handlerFunc) (resp response) {
	defer func() {
		if rec := recover(); rec != nil {
			slog.Error("handler panic", "path", c.r.URL.Path, "panic", rec, "stack", string(debug.Stack()))
			resp = mapError(errors.New("panic"))
		}
	}()
	resp, err := h(s, c)
	if err != nil {
		return mapError(err)
	}
	return resp
}

func (s *server) write(w http.ResponseWriter, resp response) {
	for k, v := range resp.headers {
		w.Header().Set(k, v)
	}
	w.WriteHeader(resp.status)
	_, _ = w.Write(resp.body)
}

// response is a fully rendered reply; idempotency stores it verbatim.
type response struct {
	status  int
	headers map[string]string
	body    []byte
	// transient responses describe momentary state and are never stored for replay.
	transient bool
}

func jsonResponse(status int, v any) response {
	raw, err := json.Marshal(v)
	if err != nil {
		slog.Error("marshal response", "err", err)
		return errorResponse(500, "internal", "internal error", nil)
	}
	return response{status: status, headers: map[string]string{"Content-Type": "application/json"}, body: raw}
}

func errorResponse(status int, code, message string, extra map[string]any) response {
	body := map[string]any{"error": map[string]string{"code": code, "message": message}}
	for k, v := range extra {
		body[k] = v
	}
	return jsonResponse(status, body)
}

// apiError is an explicit error response; extra fields sit beside "error" in the body.
type apiError struct {
	status        int
	code, message string
	extra         map[string]any
}

func (e *apiError) Error() string { return e.code + ": " + e.message }

func badRequest(message string) error {
	return &apiError{status: 400, code: "invalid_request", message: message}
}

var errNotFound = errors.New("not found")

// mapError renders err. Unmapped errors are 500 "internal" with no detail; they are logged.
func mapError(err error) response {
	var ae *apiError
	switch {
	case errors.As(err, &ae):
		return errorResponse(ae.status, ae.code, ae.message, ae.extra)
	case errors.Is(err, errNotFound), errors.Is(err, evaluate.ErrNotFound), errors.Is(err, export.ErrNotFound):
		return errorResponse(404, "not_found", "resource not found", nil)
	case errors.Is(err, errUnauthorized):
		resp := errorResponse(401, "unauthorized", "missing, unknown, or revoked API key", nil)
		resp.headers["WWW-Authenticate"] = "Bearer"
		return resp
	case errors.Is(err, ledger.ErrInsufficientCredits):
		resp := errorResponse(402, "insufficient_credits", "not enough credits for this request", nil)
		resp.transient = true // a top-up makes the same request succeed, so never store it
		return resp
	case errors.Is(err, ledger.ErrSpendCapExceeded):
		resp := errorResponse(403, "spend_cap_exceeded", "monthly spend cap for this API key would be exceeded", nil)
		resp.transient = true // a raised cap or a new month changes the outcome
		return resp
	case errors.Is(err, resolver.ErrInvalidSplit):
		return errorResponse(422, "invalid_split", "split request cannot be applied", nil)
	case errors.Is(err, resolver.ErrAlreadyResolved):
		return errorResponse(409, "already_resolved", "review candidate was already resolved differently", nil)
	case errors.Is(err, resolver.ErrDoNotMerge):
		return errorResponse(409, "do_not_merge", "records are marked do-not-merge", nil)
	case errors.Is(err, enrich.ErrRunInProgress):
		resp := errorResponse(409, "request_in_progress", "a request with this Idempotency-Key is still running", nil)
		resp.transient = true
		return resp
	case errors.Is(err, ledger.ErrIdempotencyConflict):
		return errorResponse(409, "idempotency_key_reuse", "Idempotency-Key was already used with a different request", nil)
	case errors.Is(err, export.ErrUnknownColumn):
		return errorResponse(400, "invalid_request", "unknown export column", nil)
	case errors.Is(err, enrich.ErrTooManyCandidates):
		return errorResponse(400, "invalid_request", fmt.Sprintf("at most %d distinct candidates per request", enrich.MaxCandidates), nil)
	case errors.Is(err, enrich.ErrInvalidRequest), errors.Is(err, resolver.ErrInvalidRequest),
		errors.Is(err, ledger.ErrInvalidRequest), errors.Is(err, export.ErrInvalidRequest):
		return errorResponse(400, "invalid_request", "invalid request", nil)
	}
	slog.Error("request failed", "err", err)
	return errorResponse(500, "internal", "internal error", nil)
}
