package resolver

import (
	"reflect"
	"testing"
)

func TestNormalizeName(t *testing.T) {
	for _, tt := range []struct{ input, want string }{
		{"Acme Plumbing, Inc.", "acme plumbing"},
		{"ACME PLUMBING LLC", "acme plumbing"},
		{"Co-Op Supply Co", "coop supply"},
		{" Acme   Plumbing l.l.c. ", "acme plumbing"},
		{"Acme Plumbing L L C", "acme plumbing"},
		{"Foo Co Inc", "foo"},
		{"Foo Incorporated Corporation Company Limited LP LLP PLLC", "foo"},
		{"", ""},
		{"...", ""},
	} {
		t.Run(tt.input, func(t *testing.T) {
			if got := NormalizeName(tt.input); got != tt.want {
				t.Fatalf("NormalizeName(%q) = %q, want %q", tt.input, got, tt.want)
			}
		})
	}
}

func TestClassifyBoundaries(t *testing.T) {
	for _, tt := range []struct {
		score float64
		want  Decision
	}{
		{0.85, AutoMerge}, {0.849, Review}, {0.8499, AutoMerge},
		{0.55, Review}, {0.549, Distinct}, {0.5499, Review},
		{1, AutoMerge}, {0, Distinct},
	} {
		if got := Classify(tt.score); got != tt.want {
			t.Errorf("Classify(%v) = %q, want %q", tt.score, got, tt.want)
		}
	}
}

func assertScore(t *testing.T, a, b RecordFacts, want float64, reasons []string) {
	t.Helper()
	for _, pair := range [][2]RecordFacts{{a, b}, {b, a}} {
		got, gotReasons := Score(pair[0], pair[1])
		if got != want || !reflect.DeepEqual(gotReasons, reasons) {
			t.Fatalf("Score = (%v, %v), want (%v, %v)", got, gotReasons, want, reasons)
		}
	}
}

func TestScoreHardID(t *testing.T) {
	assertScore(t, RecordFacts{LegalName: "Alpha", IDs: map[string]string{"uei": " AbC "}},
		RecordFacts{LegalName: "Beta", IDs: map[string]string{"UEI": "abc"}},
		0.97, []string{"hard_id:uei"})
}

func TestScoreHardIDConflictCaps(t *testing.T) {
	a := RecordFacts{LegalName: "Acme LLC", Zip: "12345", Domain: "acme.com", IDs: map[string]string{"uei": "one"}}
	b := RecordFacts{LegalName: "Acme Inc", Zip: "12345", Domain: "acme.com", IDs: map[string]string{"uei": "two"}}
	assertScore(t, a, b, 0.30, []string{"name_zip_contact", "hard_id_conflict"})
}

func TestScoreNameZipDomain(t *testing.T) {
	assertScore(t, RecordFacts{LegalName: "Acme LLC", Zip: " 12345 ", Domain: "HTTPS://WWW.ACME.COM/about"},
		RecordFacts{LegalName: "Acme Inc", Zip: "12345", Domain: "acme.com"},
		0.92, []string{"name_zip_contact"})
}

func TestScoreNameZipPhone(t *testing.T) {
	assertScore(t, RecordFacts{LegalName: "Acme", Zip: "12345", Phone: "+1 (212) 555-1234"},
		RecordFacts{LegalName: "Acme", Zip: "12345", Phone: "2125551234"},
		0.92, []string{"name_zip_contact"})
}

func TestScoreNamePrincipalState(t *testing.T) {
	assertScore(t, RecordFacts{LegalName: "Acme LLC", Principal: " Jane Doe ", State: " oh "},
		RecordFacts{LegalName: "Acme", Principal: "JANE DOE", State: "OH"},
		0.88, []string{"name_principal_state", "name_state"})
}

func TestScoreDBA(t *testing.T) {
	a, b := RecordFacts{DBA: "Acme Inc", State: "OH"}, RecordFacts{LegalName: "ACME LLC", State: "OH"}
	assertScore(t, a, b, 0.65, []string{"dba_legal_state"})
	if Classify(0.65) != Review {
		t.Fatal("DBA match must require review")
	}
}

func TestScoreSuiteMismatch(t *testing.T) {
	for _, address := range []string{"100 Main St Suite 2", "100 Main St STE. 2", "100 Main St unit 2", "100 Main St #2"} {
		assertScore(t, RecordFacts{Address1: address, Zip: "12345"},
			RecordFacts{Address1: "100 MAIN ST", Zip: "12345"},
			0.60, []string{"address_suite_mismatch"})
	}
	if Classify(0.60) != Review {
		t.Fatal("address match must require review")
	}
}

func TestScoreSharedAgent(t *testing.T) {
	assertScore(t, RecordFacts{RegisteredAgent: " Agent LLC ", State: "OH"},
		RecordFacts{RegisteredAgent: "AGENT INC", State: "oh"},
		0.58, []string{"shared_agent_or_pobox"})
	if Classify(0.58) != Review {
		t.Fatal("shared agent must require review")
	}
}

func TestScorePOBox(t *testing.T) {
	a := RecordFacts{POBox: true, Address1: "PO Box 12", State: "OH"}
	b := RecordFacts{POBox: true, Address1: "po box 12", State: "OH"}
	assertScore(t, a, b, 0.58, []string{"shared_agent_or_pobox"})
	b.POBox = false
	assertScore(t, a, b, 0, []string{})
}

func TestScoreNameState(t *testing.T) {
	assertScore(t, RecordFacts{LegalName: "Acme", State: "OH"},
		RecordFacts{LegalName: "Acme", State: "OH"}, 0.70, []string{"name_state"})
}

func TestScoreNameOnlyDifferentState(t *testing.T) {
	assertScore(t, RecordFacts{LegalName: "Acme", State: "OH"},
		RecordFacts{LegalName: "Acme", State: "PA"}, 0, []string{})
	if Classify(0) != Distinct {
		t.Fatal("no match must be distinct")
	}
}

func TestScoreEmptyValuesNeverMatch(t *testing.T) {
	for _, pair := range [][2]RecordFacts{
		{{}, {}},
		{{LegalName: "Acme", Domain: "acme.com"}, {LegalName: "Acme", Domain: "acme.com"}},
		{{LegalName: "Inc", State: "OH"}, {LegalName: "LLC", State: "OH"}},
		{{State: "OH", POBox: true}, {State: "OH", POBox: true}},
		{{Zip: "12345"}, {Zip: "12345"}},
		{{IDs: map[string]string{"uei": " "}}, {IDs: map[string]string{"uei": ""}}},
		{{IDs: map[string]string{"uei": "one"}}, {IDs: map[string]string{"uei": ""}}},
	} {
		assertScore(t, pair[0], pair[1], 0, []string{})
	}
}

func TestScoreAllReasonsAndConflict(t *testing.T) {
	a := RecordFacts{LegalName: "Acme", DBA: "Acme", Address1: "1 Main", Zip: "12345",
		State: "OH", Domain: "acme.com", Principal: "Jane", RegisteredAgent: "Agent",
		IDs: map[string]string{"cage": "same", "license:OH": "same", "filing:OH": "one"}}
	b := a
	b.IDs = map[string]string{"cage": "SAME", "license:OH": "same", "filing:OH": "two"}
	assertScore(t, a, b, 0.30, []string{"address_suite_mismatch", "dba_legal_state",
		"hard_id:cage", "hard_id:license:oh", "name_principal_state", "name_state",
		"name_zip_contact", "shared_agent_or_pobox", "hard_id_conflict"})
}

func TestScoreIDsStateScoped(t *testing.T) {
	assertScore(t, RecordFacts{IDs: map[string]string{"license:OH": "123"}},
		RecordFacts{IDs: map[string]string{"license:PA": "123"}}, 0, []string{})
}

func TestNormalizeContactAndAddress(t *testing.T) {
	for _, tt := range []struct {
		name, input, want string
		fn                func(string) string
	}{
		{"domain", " HTTPS://WWW.Example.COM/path?q=x ", "example.com", normalizeDomain},
		{"domain bare", "www.example.com/path", "example.com", normalizeDomain},
		{"domain query", "example.com?query=x", "example.com", normalizeDomain},
		{"phone", "+1 (212) 555-1234", "2125551234", normalizePhone},
		{"empty phone", " -- ", "", normalizePhone},
		{"address", " 100   MAIN ST Suite 2 ", "100 main st", normalizeAddress},
		{"street boundary", "100 Steuben St", "100 steuben st", normalizeAddress},
	} {
		t.Run(tt.name, func(t *testing.T) {
			if got := tt.fn(tt.input); got != tt.want {
				t.Fatalf("normalize(%q) = %q, want %q", tt.input, got, tt.want)
			}
		})
	}
}
