// Package evaluate evaluates buy-box criteria using cited facts.
package evaluate

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
)

type Criteria struct {
	NAICSIn               []string `json:"naics_in"`
	ExcludeNAICS          []string `json:"exclude_naics"`
	StatesIn              []string `json:"states_in"`
	ExcludeStates         []string `json:"exclude_states"`
	RevenueMinCents       *int64   `json:"revenue_min_cents"`
	RevenueMaxCents       *int64   `json:"revenue_max_cents"`
	HeadcountMin          *int64   `json:"headcount_min"`
	HeadcountMax          *int64   `json:"headcount_max"`
	ContractValueMinCents *int64   `json:"contract_value_min_cents"`
}

// ParseCriteria validates a single criteria object with at least one criterion.
func ParseCriteria(raw json.RawMessage) (Criteria, error) {
	var c Criteria
	decoder := json.NewDecoder(bytes.NewReader(raw))
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(&c); err != nil {
		return Criteria{}, fmt.Errorf("parse criteria: %w", err)
	}
	var trailing any
	if err := decoder.Decode(&trailing); err != io.EOF {
		if err == nil {
			return Criteria{}, fmt.Errorf("parse criteria: trailing JSON value")
		}
		return Criteria{}, fmt.Errorf("parse criteria: %w", err)
	}
	if c.RevenueMinCents != nil && c.RevenueMaxCents != nil && *c.RevenueMinCents > *c.RevenueMaxCents {
		return Criteria{}, fmt.Errorf("parse criteria: revenue minimum exceeds maximum")
	}
	if c.HeadcountMin != nil && c.HeadcountMax != nil && *c.HeadcountMin > *c.HeadcountMax {
		return Criteria{}, fmt.Errorf("parse criteria: headcount minimum exceeds maximum")
	}
	if len(c.NAICSIn)+len(c.ExcludeNAICS)+len(c.StatesIn)+len(c.ExcludeStates) == 0 && c.RevenueMinCents == nil && c.RevenueMaxCents == nil && c.HeadcountMin == nil && c.HeadcountMax == nil && c.ContractValueMinCents == nil {
		return Criteria{}, fmt.Errorf("parse criteria: empty criteria")
	}
	return c, nil
}
