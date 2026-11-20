package api

import (
	"testing"

	"rowan.ai/rowan/services/core/internal/ledger"
	"rowan.ai/rowan/services/core/internal/quality"
)

// JSON API values follow the same export_policy gating as CSV exports, per the caller workspace's tier.
func TestAPIJSONRedactsByTier(t *testing.T) {
	for _, tc := range []struct {
		tier        ledger.Tier
		wantRevenue any
		wantName    string
	}{
		{ledger.Solo, "[REDACTED]", "[REDACTED]"},
		{ledger.Fund, float64(250000000), "Zephyr Plumbing LLC"},
	} {
		t.Run(string(tc.tier), func(t *testing.T) {
			e := newEnv(t)
			_, _, token := e.tenant(tc.tier)
			cs := candidates(1)
			e.provide(cs)
			rec := e.provider.Results["c1"][0]
			rec.Citations = append(rec.Citations, citation("legal_name", `"Zephyr Plumbing LLC"`, quality.VerifiedFiling, quality.FundOnly))
			e.provider.Results["c1"][0] = rec
			run := e.run(token, "k1", cs, "")
			wantStatus(t, run, 200)
			id := decode(t, run)["companies"].([]any)[0].(string)
			bb := e.buyBox(token, "kb")

			co := e.do("GET", "/v1/companies/"+id, token, "", nil)
			wantStatus(t, co, 200)
			body := decode(t, co)
			if body["display_name"] != tc.wantName {
				t.Fatalf("display_name = %v", body["display_name"])
			}
			seen := map[string]bool{}
			for _, c := range body["citations"].([]any) {
				m := c.(map[string]any)
				seen[m["field_name"].(string)] = true
				if m["data_quality_status"] == "" || m["export_policy"] == "" || m["source_ref"] == "" || m["origin_type"] == "" {
					t.Fatalf("provenance dropped: %v", m)
				}
				switch m["field_name"] {
				case "revenue_cents":
					if m["field_value"] != tc.wantRevenue || m["data_quality_status"] != "commercial_estimate" {
						t.Fatalf("revenue citation = %v", m)
					}
				case "state":
					if m["field_value"] != "TX" {
						t.Fatalf("allowed citation redacted: %v", m)
					}
				}
			}
			if !seen["revenue_cents"] || !seen["state"] {
				t.Fatalf("citations missing: %v", seen)
			}

			for _, path := range []string{"discovery", "evaluations"} {
				var rec = e.do("POST", "/v1/discovery/search", token, "", cand{"buy_box_id": bb})
				key := "results"
				if path == "evaluations" {
					rec, key = e.do("GET", "/v1/buy-boxes/"+bb+"/evaluations", token, "", nil), "evaluations"
				}
				wantStatus(t, rec, 200)
				r := decode(t, rec)[key].([]any)[0].(map[string]any)
				if r["display_name"] != tc.wantName {
					t.Fatalf("%s display_name = %v", path, r["display_name"])
				}
				found := false
				for _, b := range r["criteria_breakdown"].([]any) {
					m := b.(map[string]any)
					if m["criterion"] != "revenue_min_cents" {
						continue
					}
					found = true
					cit := m["citation"].(map[string]any)
					if m["observed"] != tc.wantRevenue || cit["field_value"] != tc.wantRevenue ||
						m["data_quality_status"] != "commercial_estimate" || cit["data_quality_status"] != "commercial_estimate" ||
						m["status"] != "UNVERIFIED_LOW_CONFIDENCE" || cit["export_policy"] != "deal_team_plus" {
						t.Fatalf("%s criterion = %v", path, m)
					}
				}
				if !found {
					t.Fatalf("%s: revenue criterion missing", path)
				}
			}
		})
	}
}
