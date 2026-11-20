// Package resolver scores record similarity without persistence or side effects.
package resolver

import (
	"math"
	"regexp"
	"sort"
	"strings"
	"unicode"
)

// RecordFacts contains the identity facts used by the resolution rules.
// License and filing ID keys include their state, for example "license:OH".
type RecordFacts struct {
	LegalName, DBA, Address1, Zip, State, Domain, Phone, Principal, RegisteredAgent string
	POBox                                                                           bool
	IDs                                                                             map[string]string
}

// Decision identifies the action associated with a rounded similarity score.
type Decision string

const (
	AutoMerge Decision = "auto_merge"
	Review    Decision = "review"
	Distinct  Decision = "distinct"
)

// Classify rounds to the database's three-decimal precision before comparing.
func Classify(score float64) Decision {
	score = roundScore(score)
	if score >= 0.85 {
		return AutoMerge
	}
	if score >= 0.55 {
		return Review
	}
	return Distinct
}

func roundScore(score float64) float64 { return math.Round(score*1000) / 1000 }

func normalizeText(s string) string {
	return strings.Join(strings.Fields(strings.ToLower(s)), " ")
}

// NormalizeName removes punctuation and repeated trailing legal suffixes.
func NormalizeName(s string) string {
	s = strings.Map(func(r rune) rune {
		if unicode.IsPunct(r) {
			return -1
		}
		return r
	}, strings.ToLower(s))
	tokens := strings.Fields(s)
	for len(tokens) > 0 {
		n := len(tokens)
		if n >= 3 && tokens[n-3] == "l" && tokens[n-2] == "l" && tokens[n-1] == "c" {
			tokens = tokens[:n-3]
			continue
		}
		switch tokens[n-1] {
		case "inc", "incorporated", "llc", "corp", "corporation", "co", "company", "ltd", "limited", "lp", "llp", "pllc":
			tokens = tokens[:n-1]
		default:
			return strings.Join(tokens, " ")
		}
	}
	return ""
}

func normalizeDomain(s string) string {
	s = strings.ToLower(strings.TrimSpace(s))
	if i := strings.Index(s, "://"); i >= 0 {
		s = s[i+3:]
	}
	s = strings.TrimPrefix(s, "www.")
	if i := strings.IndexAny(s, "/?#"); i >= 0 {
		s = s[:i]
	}
	return strings.TrimSpace(s)
}

func normalizePhone(s string) string {
	s = strings.Map(func(r rune) rune {
		if r >= '0' && r <= '9' {
			return r
		}
		return -1
	}, s)
	if len(s) > 10 {
		s = s[len(s)-10:]
	}
	return s
}

var suiteMarker = regexp.MustCompile(`(?i)\b(suite|ste|unit)\b|#`)

func normalizeAddress(s string) string {
	if loc := suiteMarker.FindStringIndex(s); loc != nil {
		s = s[:loc[0]]
	}
	return normalizeText(s)
}

func equalNonEmpty(a, b string) bool { return a != "" && a == b }

// Score returns the highest matched rule score and every matched rule reason.
// Hard ID conflicts cap the score, even when another hard ID matches.
func Score(a, b RecordFacts) (score float64, reasons []string) {
	reasons = make([]string, 0)
	matched := make(map[string]bool)
	match := func(ok bool, value float64, reason string) {
		if ok {
			score = max(score, value)
			matched[reason] = true
		}
	}
	conflict := false
	for aKey, aValue := range a.IDs {
		aKey, aValue = normalizeText(aKey), normalizeText(aValue)
		if aKey == "" || aValue == "" {
			continue
		}
		for bKey, bValue := range b.IDs {
			bKey, bValue = normalizeText(bKey), normalizeText(bValue)
			if aKey != bKey || bValue == "" {
				continue
			}
			match(aValue == bValue, 0.97, "hard_id:"+aKey)
			conflict = conflict || aValue != bValue
		}
	}
	nameEqual := equalNonEmpty(NormalizeName(a.LegalName), NormalizeName(b.LegalName))
	stateEqual := equalNonEmpty(normalizeText(a.State), normalizeText(b.State))
	zipEqual := equalNonEmpty(normalizeText(a.Zip), normalizeText(b.Zip))
	addressEqual := equalNonEmpty(normalizeAddress(a.Address1), normalizeAddress(b.Address1))
	contactEqual := equalNonEmpty(normalizeDomain(a.Domain), normalizeDomain(b.Domain)) ||
		equalNonEmpty(normalizePhone(a.Phone), normalizePhone(b.Phone))
	principalEqual := equalNonEmpty(NormalizeName(a.Principal), NormalizeName(b.Principal))
	agentEqual := equalNonEmpty(NormalizeName(a.RegisteredAgent), NormalizeName(b.RegisteredAgent))
	dbaLegalEqual := equalNonEmpty(NormalizeName(a.DBA), NormalizeName(b.LegalName)) ||
		equalNonEmpty(NormalizeName(b.DBA), NormalizeName(a.LegalName))
	match(nameEqual && zipEqual && contactEqual, 0.92, "name_zip_contact")
	match(nameEqual && principalEqual && stateEqual, 0.88, "name_principal_state")
	match(nameEqual && stateEqual, 0.70, "name_state")
	match(dbaLegalEqual && stateEqual, 0.65, "dba_legal_state")
	match(addressEqual && zipEqual, 0.60, "address_suite_mismatch")
	match((agentEqual || (a.POBox && b.POBox && addressEqual)) && stateEqual, 0.58, "shared_agent_or_pobox")
	for reason := range matched {
		reasons = append(reasons, reason)
	}
	sort.Strings(reasons)
	if conflict {
		score = min(score, 0.30)
		reasons = append(reasons, "hard_id_conflict")
	}
	return roundScore(score), reasons
}
