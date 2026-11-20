package evaluate

import (
	"encoding/json"
	"testing"

	"rowan.ai/rowan/services/core/internal/quality"
)

func number(n int64) *int64 { return &n }
func citation(field, value string, status quality.Status) quality.Citation {
	return quality.Citation{FieldName: field, Value: json.RawMessage(value), Status: status}
}
func check(t *testing.T, c Criteria, cs []quality.Citation, status, fit string) Evaluation {
	t.Helper()
	got := Evaluate(c, cs)
	if len(got.Criteria) != 1 || got.Criteria[0].Status != status || got.Fit != fit {
		t.Fatalf("unexpected evaluation: %+v", got)
	}
	return got
}
func TestRevenueVerifiedMet(t *testing.T) {
	check(t, Criteria{RevenueMinCents: number(200000000)}, []quality.Citation{citation("revenue_cents", "250000000", quality.VerifiedFiling)}, StatusMet, FitMatch)
}
func TestRevenueVerifiedNotMet(t *testing.T) {
	check(t, Criteria{RevenueMinCents: number(200000000)}, []quality.Citation{citation("revenue_cents", "150000000", quality.VerifiedFiling)}, StatusNotMet, FitFail)
}
func TestRevenueCommercialEstimateUnverified(t *testing.T) {
	got := check(t, Criteria{RevenueMinCents: number(200000000)}, []quality.Citation{citation("revenue_cents", "900000000", quality.CommercialEstimate)}, StatusUnverified, FitPartialUnverified)
	r := got.Criteria[0]
	if r.DataQualityStatus != quality.CommercialEstimate || string(r.Observed) != "900000000" || r.Citation == nil {
		t.Fatalf("lost provenance: %+v", r)
	}
}
func TestScrapedExclusionUnverified(t *testing.T) {
	check(t, Criteria{ExcludeStates: []string{"TX"}}, []quality.Citation{citation("state", `"TX"`, quality.ScrapedWeb)}, StatusUnverified, FitPartialUnverified)
}
func TestMissingFieldUnverified(t *testing.T) {
	got := check(t, Criteria{HeadcountMin: number(10)}, nil, StatusUnverified, FitPartialUnverified)
	if got.Criteria[0].DataQualityStatus != quality.Missing || got.Criteria[0].Citation != nil {
		t.Fatal(got)
	}
}
func TestVerifiedBeatsEstimate(t *testing.T) {
	check(t, Criteria{RevenueMinCents: number(200000000)}, []quality.Citation{citation("revenue_cents", "150000000", quality.VerifiedFiling), citation("revenue_cents", "300000000", quality.CommercialEstimate)}, StatusNotMet, FitFail)
}
func TestOverallScoreAndFit(t *testing.T) {
	got := Evaluate(Criteria{NAICSIn: []string{"2382"}, StatesIn: []string{" tx "}, HeadcountMin: number(10)}, []quality.Citation{citation("naics", `"238220"`, quality.VerifiedFiling), citation("state", `"TX"`, quality.VerifiedFiling)})
	if got.OverallScore != .667 || got.Fit != FitPartialUnverified {
		t.Fatal(got)
	}
}
func TestParseCriteriaRejectsUnknownAndInverted(t *testing.T) {
	for _, raw := range []string{`{}`, `null`, `{"unknown":1}`, `{"revenue_min_cents":2,"revenue_max_cents":1}`, `{"headcount_min":2,"headcount_max":1}`, `{"states_in":["TX"]} {}`} {
		if _, err := ParseCriteria(json.RawMessage(raw)); err == nil {
			t.Errorf("accepted %s", raw)
		}
	}
	if _, err := ParseCriteria(json.RawMessage(`{"states_in":["TX"]}`)); err != nil {
		t.Fatal(err)
	}
}
func TestAllCriteriaOrderAndBoundaries(t *testing.T) {
	c := Criteria{NAICSIn: []string{"2382"}, ExcludeNAICS: []string{"11"}, StatesIn: []string{"tx"}, ExcludeStates: []string{"NY"}, RevenueMinCents: number(20), RevenueMaxCents: number(20), HeadcountMin: number(5), HeadcountMax: number(5), ContractValueMinCents: number(30)}
	got := Evaluate(c, []quality.Citation{citation("naics", `"238220"`, quality.VerifiedFiling), citation("state", `" tx "`, quality.VerifiedFiling), citation("revenue_cents", "20", quality.VerifiedFiling), citation("headcount", "5", quality.VerifiedFiling), citation("contract_value_cents", "30", quality.VerifiedFiling)})
	names := []string{"naics_in", "exclude_naics", "states_in", "exclude_states", "revenue_min_cents", "revenue_max_cents", "headcount_min", "headcount_max", "contract_value_min_cents"}
	if got.Fit != FitMatch || got.OverallScore != 1 || len(got.Criteria) != len(names) {
		t.Fatal(got)
	}
	for i, name := range names {
		if got.Criteria[i].Criterion != name {
			t.Fatal(got)
		}
	}
	check(t, Criteria{ExcludeNAICS: []string{"2382"}}, []quality.Citation{citation("naics", `"238220"`, quality.VerifiedFiling)}, StatusNotMet, FitFail)
	check(t, Criteria{ExcludeStates: []string{"tx"}}, []quality.Citation{citation("state", `" TX "`, quality.VerifiedFiling)}, StatusNotMet, FitFail)
}
func TestMalformedAndMissingCitation(t *testing.T) {
	for _, value := range []string{`"20"`, `20.5`, `null`, `9223372036854775808`, `true`} {
		got := check(t, Criteria{HeadcountMin: number(1)}, []quality.Citation{citation("headcount", value, quality.VerifiedFiling)}, StatusUnverified, FitPartialUnverified)
		if got.Criteria[0].DataQualityStatus != quality.VerifiedFiling || got.Criteria[0].Citation == nil {
			t.Fatal(got)
		}
	}
	check(t, Criteria{StatesIn: []string{"TX"}}, []quality.Citation{citation("state", `12`, quality.VerifiedFiling)}, StatusUnverified, FitPartialUnverified)
	got := check(t, Criteria{HeadcountMin: number(1)}, []quality.Citation{citation("headcount", `null`, quality.Missing)}, StatusUnverified, FitPartialUnverified)
	if got.Criteria[0].Citation == nil || got.Criteria[0].DataQualityStatus != quality.Missing {
		t.Fatal(got)
	}
}

func TestParseCriteriaRejectsBlankListEntries(t *testing.T) {
	for _, raw := range []string{
		`{"naics_in":[""]}`, `{"naics_in":["2382"," "]}`, `{"exclude_naics":["\t"]}`,
		`{"states_in":[""]}`, `{"exclude_states":["  "]}`,
	} {
		if _, err := ParseCriteria(json.RawMessage(raw)); err == nil {
			t.Fatalf("accepted blank list entry: %s", raw)
		}
	}
}

func TestParseCriteriaRejectsNegativeBounds(t *testing.T) {
	for _, raw := range []string{
		`{"revenue_min_cents":-1}`, `{"revenue_max_cents":-1}`, `{"headcount_min":-1}`,
		`{"headcount_max":-5}`, `{"contract_value_min_cents":-1}`,
	} {
		if _, err := ParseCriteria(json.RawMessage(raw)); err == nil {
			t.Fatalf("accepted negative bound: %s", raw)
		}
	}
}

func TestParseCriteriaAcceptsZeroBoundAndValues(t *testing.T) {
	if _, err := ParseCriteria(json.RawMessage(`{"naics_in":["2382"],"headcount_min":0}`)); err != nil {
		t.Fatal(err)
	}
}
