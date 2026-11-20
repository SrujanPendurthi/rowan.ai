package api

import (
	"errors"

	"rowan.ai/rowan/services/core/internal/enrich"
	"rowan.ai/rowan/services/core/internal/ledger"
)

type candidateJSON struct {
	Ref       string            `json:"ref"`
	LegalName string            `json:"legal_name"`
	State     string            `json:"state"`
	IDs       map[string]string `json:"ids"`
}

func toCandidates(in []candidateJSON) []enrich.Candidate {
	out := make([]enrich.Candidate, len(in))
	for i, c := range in {
		out[i] = enrich.Candidate{Ref: c.Ref, LegalName: c.LegalName, State: c.State, IDs: c.IDs}
	}
	return out
}

type quoteJSON struct {
	CandidateCount       int   `json:"candidate_count"`
	AlreadyEnriched      int   `json:"already_enriched"`
	Billable             int   `json:"billable"`
	CreditsRequired      int64 `json:"credits_required"`
	CreditsAvailable     int64 `json:"credits_available"`
	Shortfall            int64 `json:"shortfall"`
	OverageEstimateCents int64 `json:"overage_estimate_cents"`
}

func toQuoteJSON(q enrich.Quote) quoteJSON {
	return quoteJSON{q.CandidateCount, q.AlreadyEnriched, q.Billable, q.CreditsRequired, q.CreditsAvailable,
		q.Shortfall, q.OverageEstimateCents}
}

func (s *server) enrichQuote(c *call) (response, error) {
	var in struct {
		Candidates []candidateJSON `json:"candidates"`
	}
	if err := c.decode(&in); err != nil {
		return response{}, err
	}
	q, err := s.Enrich.Quote(c.ctx(), c.key.WorkspaceID, toCandidates(in.Candidates))
	if err != nil {
		return response{}, err
	}
	return jsonResponse(200, toQuoteJSON(q)), nil
}

func (s *server) enrichRun(c *call) (response, error) {
	var in struct {
		BuyBoxID   string          `json:"buy_box_id"`
		Candidates []candidateJSON `json:"candidates"`
	}
	if err := c.decode(&in); err != nil {
		return response{}, err
	}
	if in.BuyBoxID != "" && !validUUID(in.BuyBoxID) {
		return response{}, badRequest("buy_box_id must be a UUID")
	}
	cs := toCandidates(in.Candidates)
	res, err := s.Enrich.Run(c.ctx(), c.key.WorkspaceID, c.key.ID, c.r.Header.Get("Idempotency-Key"), in.BuyBoxID, cs)
	if errors.Is(err, ledger.ErrInsufficientCredits) {
		resp := mapError(err)
		// The quote tells the caller exactly how short the workspace is.
		if q, qerr := s.Enrich.Quote(c.ctx(), c.key.WorkspaceID, cs); qerr == nil {
			return jsonResponse(402, map[string]any{
				"error": map[string]string{"code": "insufficient_credits", "message": "not enough credits for this request"},
				"quote": toQuoteJSON(q),
			}), nil
		}
		return resp, nil
	}
	if err != nil {
		return response{}, err
	}
	return jsonResponse(200, res), nil
}
