package proxy

import (
	"encoding/json"
	"reflect"
	"testing"
	"time"

	"rowan.ai/rowan/services/core/internal/quality"
)

var testNow = time.Date(2026, 11, 20, 12, 0, 0, 0, time.UTC)

func TestWindowBoundsInclusive(t *testing.T) {
	start, end := testNow.AddDate(0, -9, 0), testNow.AddDate(0, -6, 0)
	for _, tt := range []struct {
		name string
		at   time.Time
		want bool
	}{
		{"nine months", start, true}, {"six months", end, true},
		{"five months 29 days", testNow.AddDate(0, -5, -29), false},
		{"nine months one day", start.AddDate(0, 0, -1), false},
		{"before start", start.Add(-time.Nanosecond), false}, {"after end", end.Add(time.Nanosecond), false},
	} {
		t.Run(tt.name, func(t *testing.T) {
			got := Analyze([]Signal{{Type: UCCFiling, ObservedAt: tt.at}}, testNow)
			if got.InWindow != tt.want || got.SignalCount != len(got.Signals) || (got.SignalCount == 1) != tt.want {
				t.Fatalf("got %+v", got)
			}
			if !got.WindowStart.Equal(start) || !got.WindowEnd.Equal(end) {
				t.Fatalf("wrong window: %+v", got)
			}
		})
	}
}

func TestProxyLabelDefault(t *testing.T) {
	got := Analyze([]Signal{{Type: UCCFiling, ObservedAt: testNow.AddDate(0, -7, 0)}, {Type: OfficerChange, ObservedAt: testNow.AddDate(0, -8, 0)}}, testNow)
	if got.Label != LabelPostSaleProxy || got.SignalCount != 2 {
		t.Fatalf("got %+v", got)
	}
}

func TestDisclosedTransactionUnverifiedStaysProxy(t *testing.T) {
	got := Analyze([]Signal{{Type: DisclosedTransaction, ObservedAt: testNow.AddDate(0, -7, 0), Citation: quality.Citation{Status: quality.ScrapedWeb}}}, testNow)
	if got.Label != LabelPostSaleProxy {
		t.Fatalf("got %+v", got)
	}
}

func TestConfirmedTransferRequiresVerified(t *testing.T) {
	for _, status := range []quality.Status{quality.VerifiedFiling, quality.CommercialEstimate, quality.ScrapedWeb, quality.Missing, "unknown", ""} {
		got := Analyze([]Signal{{Type: DisclosedTransaction, ObservedAt: testNow.AddDate(0, -7, 0), Citation: quality.Citation{Status: status}}}, testNow)
		want := LabelPostSaleProxy
		if status == quality.VerifiedFiling {
			want = LabelConfirmedTransfer
		}
		if got.Label != want {
			t.Errorf("%s: got %s", status, got.Label)
		}
	}
	for _, s := range []Signal{{Type: DisclosedTransaction, ObservedAt: testNow}, {Type: UCCFiling, ObservedAt: testNow.AddDate(0, -7, 0)}} {
		s.Citation.Status = quality.VerifiedFiling
		if got := Analyze([]Signal{s}, testNow); got.Label == LabelConfirmedTransfer {
			t.Fatalf("incorrect confirmation: %+v", got)
		}
	}
}

func TestSignalsKeepCitations(t *testing.T) {
	c := quality.Citation{FieldName: "transfer", Value: json.RawMessage(`"sale"`), Origin: quality.RawSource, Status: quality.VerifiedFiling, ExportPolicy: quality.DisplayOnly, SourceSystem: "filing", SourceRef: "ref", TableName: "table", RowKey: "row", PageOrSpan: "p1", RetrievedAt: testNow, SourceRecordID: "id"}
	signal := Signal{Type: DisclosedTransaction, ObservedAt: testNow.AddDate(0, -7, 0), Citation: c}
	got := Analyze([]Signal{signal}, testNow)
	if !reflect.DeepEqual(got.Signals, []Signal{signal}) {
		t.Fatalf("citation changed: %+v", got)
	}
}

func TestKnownTypesStableOrderAndNoInputMutation(t *testing.T) {
	types := []SignalType{UCCFiling, UCCAmendment, OfficerChange, RegisteredAgentChange, EntityChange, NewHoldco, DomainShift, LeadershipChange, BrokerListingRemoved, DisclosedTransaction}
	values := []string{"ucc_filing", "ucc_amendment", "officer_change", "registered_agent_change", "entity_change", "new_holdco", "domain_shift", "leadership_change", "broker_listing_removed", "disclosed_transaction"}
	signals := []Signal{{Type: "unknown", ObservedAt: testNow.AddDate(0, -7, 0)}}
	for i, typ := range types {
		if string(typ) != values[i] {
			t.Errorf("type %d: %s", i, typ)
		}
		signals = append(signals, Signal{Type: typ, ObservedAt: testNow.AddDate(0, -7, 0)})
	}
	signals = append(signals, Signal{Type: OfficerChange, ObservedAt: testNow.AddDate(0, -8, 0)})
	original := append([]Signal(nil), signals...)
	got := Analyze(signals, testNow)
	want := append([]Signal{signals[len(signals)-1]}, signals[1:len(signals)-1]...)
	if !reflect.DeepEqual(got.Signals, want) || !reflect.DeepEqual(signals, original) {
		t.Fatalf("unstable order or mutated input: %+v", got)
	}
}

func TestEmptyUnknownAndUTCWindow(t *testing.T) {
	now := time.Date(2026, 11, 1, 1, 0, 0, 0, time.FixedZone("east", 9*3600))
	for _, signals := range [][]Signal{nil, {}, {{Type: "unknown", ObservedAt: now.AddDate(0, -7, 0)}}} {
		got := Analyze(signals, now)
		if got.Label != LabelNone || got.InWindow || got.SignalCount != 0 || len(got.Signals) != 0 {
			t.Fatalf("got %+v", got)
		}
		if got.WindowStart != now.UTC().AddDate(0, -9, 0) || got.WindowEnd != now.UTC().AddDate(0, -6, 0) {
			t.Fatalf("non-UTC window: %+v", got)
		}
	}
}

func TestJSONTags(t *testing.T) {
	got := Analyze([]Signal{{Type: UCCFiling, ObservedAt: testNow.AddDate(0, -7, 0)}}, testNow)
	data, err := json.Marshal(got)
	if err != nil {
		t.Fatal(err)
	}
	var fields map[string]json.RawMessage
	if err := json.Unmarshal(data, &fields); err != nil {
		t.Fatal(err)
	}
	for _, key := range []string{"label", "in_window", "window_start", "window_end", "signal_count", "signals"} {
		if _, ok := fields[key]; !ok {
			t.Errorf("missing %s", key)
		}
	}
	var signals []map[string]json.RawMessage
	if err := json.Unmarshal(fields["signals"], &signals); err != nil {
		t.Fatal(err)
	}
	for _, key := range []string{"type", "observed_at", "citation"} {
		if _, ok := signals[0][key]; !ok {
			t.Errorf("missing %s", key)
		}
	}
}
