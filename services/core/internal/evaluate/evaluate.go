package evaluate

import (
	"bytes"
	"encoding/json"
	"math"
	"strings"

	"rowan.ai/rowan/services/core/internal/quality"
)

const (
	StatusMet            = "MET"
	StatusNotMet         = "NOT_MET"
	StatusUnverified     = "UNVERIFIED_LOW_CONFIDENCE"
	FitMatch             = "match"
	FitPartialUnverified = "partial_unverified"
	FitFail              = "fail"
)

type CriterionResult struct {
	Criterion         string            `json:"criterion"`
	Status            string            `json:"status"`
	Observed          json.RawMessage   `json:"observed,omitempty"`
	DataQualityStatus quality.Status    `json:"data_quality_status"`
	Citation          *quality.Citation `json:"citation,omitempty"`
}

type Evaluation struct {
	OverallScore float64           `json:"overall_score"`
	Fit          string            `json:"fit"`
	Criteria     []CriterionResult `json:"criteria"`
}

// Evaluate preserves provenance and gates deterministic decisions on verified filings.
func Evaluate(c Criteria, citations []quality.Citation) Evaluation {
	fields := make(map[string][]quality.Citation)
	for _, citation := range citations {
		fields[citation.FieldName] = append(fields[citation.FieldName], citation)
	}
	result := Evaluation{Fit: FitPartialUnverified, Criteria: make([]CriterionResult, 0)}
	met, failed := 0, false
	add := func(name, field string, compare func(json.RawMessage) (bool, bool)) {
		r := CriterionResult{Criterion: name, Status: StatusUnverified, DataQualityStatus: quality.Missing}
		if best, ok := quality.BestCitation(fields[field]); ok {
			r.Citation = &best
			r.Observed = best.Value
			r.DataQualityStatus = best.Status
			if best.Status.Deterministic() {
				if matches, valid := compare(best.Value); valid {
					r.Status = StatusNotMet
					if matches {
						r.Status = StatusMet
					}
				}
			}
		}
		if r.Status == StatusMet {
			met++
		}
		if r.Status == StatusNotMet {
			failed = true
		}
		result.Criteria = append(result.Criteria, r)
	}
	text := func(values []string, prefix, exclude bool) func(json.RawMessage) (bool, bool) {
		return func(raw json.RawMessage) (bool, bool) {
			var observed string
			if bytes.Equal(bytes.TrimSpace(raw), []byte("null")) || json.Unmarshal(raw, &observed) != nil {
				return false, false
			}
			if !prefix {
				observed = strings.ToUpper(strings.TrimSpace(observed))
			}
			found := false
			for _, value := range values {
				if !prefix {
					value = strings.ToUpper(strings.TrimSpace(value))
				}
				if (prefix && strings.HasPrefix(observed, value)) || (!prefix && observed == value) {
					found = true
					break
				}
			}
			if exclude {
				found = !found
			}
			return found, true
		}
	}
	numeric := func(bound int64, minimum bool) func(json.RawMessage) (bool, bool) {
		return func(raw json.RawMessage) (bool, bool) {
			var observed int64
			if bytes.Equal(bytes.TrimSpace(raw), []byte("null")) || json.Unmarshal(raw, &observed) != nil {
				return false, false
			}
			if minimum {
				return observed >= bound, true
			}
			return observed <= bound, true
		}
	}
	if len(c.NAICSIn) > 0 {
		add("naics_in", "naics", text(c.NAICSIn, true, false))
	}
	if len(c.ExcludeNAICS) > 0 {
		add("exclude_naics", "naics", text(c.ExcludeNAICS, true, true))
	}
	if len(c.StatesIn) > 0 {
		add("states_in", "state", text(c.StatesIn, false, false))
	}
	if len(c.ExcludeStates) > 0 {
		add("exclude_states", "state", text(c.ExcludeStates, false, true))
	}
	for _, criterion := range []struct {
		name, field string
		bound       *int64
		minimum     bool
	}{
		{"revenue_min_cents", "revenue_cents", c.RevenueMinCents, true},
		{"revenue_max_cents", "revenue_cents", c.RevenueMaxCents, false},
		{"headcount_min", "headcount", c.HeadcountMin, true},
		{"headcount_max", "headcount", c.HeadcountMax, false},
		{"contract_value_min_cents", "contract_value_cents", c.ContractValueMinCents, true},
	} {
		if criterion.bound != nil {
			add(criterion.name, criterion.field, numeric(*criterion.bound, criterion.minimum))
		}
	}
	if len(result.Criteria) > 0 {
		result.OverallScore = math.Round(float64(met)/float64(len(result.Criteria))*1000) / 1000
		if met == len(result.Criteria) {
			result.Fit = FitMatch
		}
	}
	if failed {
		result.Fit = FitFail
	}
	return result
}
