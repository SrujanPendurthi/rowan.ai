// Package quality defines provenance and confidence metadata for cited facts.
package quality

import (
	"encoding/json"
	"fmt"
	"time"
)

type Status string

const (
	VerifiedFiling     Status = "verified_filing"
	CommercialEstimate Status = "commercial_estimate"
	ScrapedWeb         Status = "scraped_web"
	Missing            Status = "missing"
)

type Origin string

const (
	RawSource         Origin = "raw_source"
	DerivedEnrichment Origin = "derived_enrichment"
)

type ExportPolicy string

const (
	Allow        ExportPolicy = "allow"
	DealTeamPlus ExportPolicy = "deal_team_plus"
	FundOnly     ExportPolicy = "fund_only"
	DisplayOnly  ExportPolicy = "display_only"
)

// Citation preserves a field value and its provenance.
type Citation struct {
	FieldName      string          `json:"field_name"`
	Value          json.RawMessage `json:"field_value"`
	Origin         Origin          `json:"origin_type"`
	Status         Status          `json:"data_quality_status"`
	ExportPolicy   ExportPolicy    `json:"export_policy"`
	SourceSystem   string          `json:"source_system"`
	SourceRef      string          `json:"source_ref"`
	TableName      string          `json:"table_name,omitempty"`
	RowKey         string          `json:"row_key,omitempty"`
	PageOrSpan     string          `json:"page_or_span,omitempty"`
	RetrievedAt    time.Time       `json:"retrieved_at"`
	SourceRecordID string          `json:"source_record_id,omitempty"`
}

// Validate checks the supported enums and required provenance.
func (c Citation) Validate() error {
	switch c.Status {
	case VerifiedFiling, CommercialEstimate, ScrapedWeb, Missing:
	default:
		return fmt.Errorf("citation: unknown data_quality_status %q", c.Status)
	}
	switch c.Origin {
	case RawSource, DerivedEnrichment:
	default:
		return fmt.Errorf("citation: unknown origin_type %q", c.Origin)
	}
	switch c.ExportPolicy {
	case Allow, DealTeamPlus, FundOnly, DisplayOnly:
	default:
		return fmt.Errorf("citation: unknown export_policy %q", c.ExportPolicy)
	}
	if c.SourceSystem == "" {
		return fmt.Errorf("citation: source_system is required")
	}
	if c.SourceRef == "" {
		return fmt.Errorf("citation: source_ref is required")
	}
	if c.Status != Missing && c.RetrievedAt.IsZero() {
		return fmt.Errorf("citation: retrieved_at is required")
	}
	return nil
}

// Deterministic reports whether a status supports deterministic evaluation.
func (s Status) Deterministic() bool { return s == VerifiedFiling }

func (s Status) rank() int {
	switch s {
	case VerifiedFiling:
		return 4
	case CommercialEstimate:
		return 3
	case ScrapedWeb:
		return 2
	case Missing:
		return 1
	default:
		return 0
	}
}

// BestCitation selects by quality, then retrieval time. Exact ties keep the first.
// Empty input returns false. Unknown statuses rank below all known statuses.
func BestCitation(cs []Citation) (Citation, bool) {
	if len(cs) == 0 {
		return Citation{}, false
	}
	best := cs[0]
	for _, c := range cs[1:] {
		if c.Status.rank() > best.Status.rank() || (c.Status.rank() == best.Status.rank() && c.RetrievedAt.After(best.RetrievedAt)) {
			best = c
		}
	}
	return best, true
}
