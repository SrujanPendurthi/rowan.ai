package quality

import (
	"encoding/json"
	"testing"
	"time"
)

func validCitation() Citation {
	return Citation{FieldName: "revenue", Value: json.RawMessage(`100`), Origin: RawSource, Status: VerifiedFiling, ExportPolicy: Allow, SourceSystem: "filing", SourceRef: "report", RetrievedAt: time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)}
}

func TestValidateRejectsUnknownStatus(t *testing.T) {
	c := validCitation()
	c.Status = "unknown"
	if c.Validate() == nil {
		t.Fatal("accepted unknown status")
	}
}

func TestValidateRequiresSource(t *testing.T) {
	for _, status := range []Status{VerifiedFiling, Missing} {
		for _, field := range []string{"system", "ref"} {
			t.Run(string(status)+"/"+field, func(t *testing.T) {
				c := validCitation()
				c.Status = status
				if field == "system" {
					c.SourceSystem = ""
				} else {
					c.SourceRef = ""
				}
				if c.Validate() == nil {
					t.Fatal("accepted missing source")
				}
			})
		}
	}
}

func TestBestCitationPrefersVerified(t *testing.T) {
	verified := validCitation()
	scraped := verified
	scraped.Status = ScrapedWeb
	scraped.RetrievedAt = verified.RetrievedAt.Add(time.Hour)
	got, ok := BestCitation([]Citation{scraped, verified})
	if !ok || got.Status != VerifiedFiling {
		t.Fatalf("got %+v, %v", got, ok)
	}
}

func TestBestCitationTieBreaksNewest(t *testing.T) {
	old := validCitation()
	newest := old
	newest.RetrievedAt = old.RetrievedAt.Add(time.Hour)
	for _, cs := range [][]Citation{{old, newest}, {newest, old}} {
		got, ok := BestCitation(cs)
		if !ok || !got.RetrievedAt.Equal(newest.RetrievedAt) {
			t.Fatalf("got %+v, %v", got, ok)
		}
	}
}

func TestDeterministicOnlyVerified(t *testing.T) {
	for _, status := range []Status{VerifiedFiling, CommercialEstimate, ScrapedWeb, Missing, "unknown", ""} {
		if got := status.Deterministic(); got != (status == VerifiedFiling) {
			t.Errorf("%q: got %v", status, got)
		}
	}
}

func TestValidateEnumsAndTimestamp(t *testing.T) {
	for _, origin := range []Origin{RawSource, DerivedEnrichment} {
		for _, status := range []Status{VerifiedFiling, CommercialEstimate, ScrapedWeb, Missing} {
			for _, policy := range []ExportPolicy{Allow, DealTeamPlus, FundOnly, DisplayOnly} {
				c := validCitation()
				c.Origin = origin
				c.Status = status
				c.ExportPolicy = policy
				if err := c.Validate(); err != nil {
					t.Errorf("valid citation: %v", err)
				}
			}
		}
	}
	for _, field := range []string{"origin", "policy", "timestamp"} {
		c := validCitation()
		switch field {
		case "origin":
			c.Origin = "unknown"
		case "policy":
			c.ExportPolicy = "unknown"
		case "timestamp":
			c.RetrievedAt = time.Time{}
		}
		if c.Validate() == nil {
			t.Errorf("accepted invalid %s", field)
		}
	}
	c := validCitation()
	c.Status = Missing
	c.Value = nil
	c.RetrievedAt = time.Time{}
	if err := c.Validate(); err != nil {
		t.Errorf("missing citation: %v", err)
	}
}

func TestBestCitationRankingAndEmpty(t *testing.T) {
	if _, ok := BestCitation(nil); ok {
		t.Fatal("empty input returned citation")
	}
	statuses := []Status{Missing, ScrapedWeb, CommercialEstimate, VerifiedFiling}
	for i, status := range statuses {
		cs := make([]Citation, i+1)
		for j := range cs {
			cs[j] = validCitation()
			cs[j].Status = statuses[j]
		}
		got, ok := BestCitation(cs)
		if !ok || got.Status != status {
			t.Errorf("want %s, got %s, %v", status, got.Status, ok)
		}
	}
}

func TestCitationJSON(t *testing.T) {
	c := validCitation()
	c.TableName = "filings"
	c.RowKey = "42"
	c.PageOrSpan = "p1"
	c.SourceRecordID = "record-id"
	data, err := json.Marshal(c)
	if err != nil {
		t.Fatal(err)
	}
	var fields map[string]json.RawMessage
	if err := json.Unmarshal(data, &fields); err != nil {
		t.Fatal(err)
	}
	for _, key := range []string{"field_name", "field_value", "origin_type", "data_quality_status", "export_policy", "source_system", "source_ref", "table_name", "row_key", "page_or_span", "retrieved_at", "source_record_id"} {
		if _, ok := fields[key]; !ok {
			t.Errorf("missing JSON key %s", key)
		}
	}
	var roundTrip Citation
	if err := json.Unmarshal(data, &roundTrip); err != nil {
		t.Fatal(err)
	}
	if roundTrip.SourceRecordID != c.SourceRecordID || roundTrip.Status != c.Status || string(roundTrip.Value) != string(c.Value) {
		t.Fatalf("round trip lost citation: %+v", roundTrip)
	}
	data, err = json.Marshal(Citation{})
	if err != nil {
		t.Fatal(err)
	}
	var empty map[string]json.RawMessage
	if err := json.Unmarshal(data, &empty); err != nil {
		t.Fatal(err)
	}
	for _, key := range []string{"table_name", "row_key", "page_or_span", "source_record_id"} {
		if _, ok := empty[key]; ok {
			t.Errorf("optional string %s not omitted", key)
		}
	}
	for _, key := range []string{"origin_type", "data_quality_status"} {
		if _, ok := empty[key]; !ok {
			t.Errorf("required key %s omitted", key)
		}
	}
}
