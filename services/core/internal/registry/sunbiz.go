package registry

import (
	"bufio"
	"context"
	"errors"
	"fmt"
	"io"
	"maps"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"

	"rowan.ai/rowan/services/core/internal/proxy"
	"rowan.ai/rowan/services/core/internal/resolver"
)

const sunbizCorpRecordBytes = 1440

type sunbizOfficer struct {
	Title string `json:"title"`
	Type  string `json:"type"`
	Name  string `json:"name"`
	City  string `json:"city"`
	State string `json:"state"`
}

// LoadSunbizCorp loads a local fixed-width corporate extract and cites changes
// against the previous snapshot. Offsets count ASCII bytes, starting at one.
func LoadSunbizCorp(ctx context.Context, pool *pgxpool.Pool, path, snapshotRef string, now time.Time) (stats LoadStats, err error) {
	file, err := os.Open(path)
	if err != nil {
		return stats, fmt.Errorf("open Sunbiz corporate extract: %w", err)
	}
	defer func() {
		if closeErr := file.Close(); closeErr != nil {
			err = errors.Join(err, fmt.Errorf("close Sunbiz corporate extract: %w", closeErr))
		}
	}()
	scanner := newLineScanner(file)
	line := 0
	next := func() (Entity, bool, error) {
		if !scanner.Scan() {
			return Entity{}, false, scanErr(scanner, "Sunbiz", line)
		}
		line++
		row := scanner.Text()
		if len(row) < sunbizCorpRecordBytes {
			return Entity{}, false, fmt.Errorf("Sunbiz line %d: got %d bytes, need at least %d", line, len(row), sunbizCorpRecordBytes)
		}
		return sunbizCorpEntity(row[:sunbizCorpRecordBytes], line), true, nil
	}
	meta := LoadMeta{SourceSystem: "fl_sunbiz", TableName: "cordata", FileName: filepath.Base(path),
		SnapshotRef: snapshotRef, LoadedAt: now}
	return Load(ctx, pool, meta, next, sunbizDiff)
}

const maxLineBytes = 64 << 10

func newLineScanner(r io.Reader) *bufio.Scanner {
	s := bufio.NewScanner(r)
	s.Buffer(make([]byte, 0, 4096), maxLineBytes)
	return s
}

// scanErr reports the failure after a false Scan; nil means clean EOF.
func scanErr(s *bufio.Scanner, kind string, line int) error {
	err := s.Err()
	if errors.Is(err, bufio.ErrTooLong) {
		return fmt.Errorf("%s line %d: line too long", kind, line+1)
	}
	if err != nil {
		return fmt.Errorf("%s line %d: %w", kind, line+1, err)
	}
	return nil
}

func parseSunbizDate(s string) *time.Time {
	s = strings.TrimSpace(s)
	if len(s) != 8 || s == "00000000" {
		return nil
	}
	for i := range s {
		if s[i] < '0' || s[i] > '9' {
			return nil
		}
	}
	layout := "01022006"
	if year, _ := strconv.Atoi(s[:4]); year >= 1900 && year <= 2100 {
		layout = "20060102"
	}
	date, err := time.Parse(layout, s)
	if err != nil {
		return nil
	}
	return &date
}

func sunbizCorpEntity(row string, line int) Entity {
	field := func(start, length int) string {
		return strings.TrimSpace(strings.ToValidUTF8(row[start-1:start-1+length], ""))
	}
	raw := map[string]string{
		"doc_number": field(1, 12), "legal_name": field(13, 192),
		"status": field(205, 1), "filing_type": field(206, 15),
		"address1": field(221, 42), "address2": field(263, 42), "city": field(305, 28),
		"state": field(333, 2), "zip": field(335, 10), "country": field(345, 2),
		"file_date": field(473, 8), "fei": field(481, 14), "more_than_six_officers": field(495, 1),
		"last_transaction_date": field(496, 8), "registered_agent": field(545, 42),
		"registered_agent_type": field(587, 1), "registered_agent_address": field(588, 42),
		"registered_agent_city": field(630, 28), "registered_agent_state": field(658, 2), "registered_agent_zip": field(660, 9),
	}
	officers := make([]sunbizOfficer, 0, 6)
	for n := 0; n < 6; n++ {
		start := 669 + 128*n
		officer := sunbizOfficer{
			Title: field(start, 4), Type: field(start+4, 1), Name: field(start+5, 42),
			City: field(start+89, 28), State: field(start+117, 2),
		}
		prefix := fmt.Sprintf("officer_%d_", n+1)
		for key, value := range map[string]string{
			"title": officer.Title, "type": officer.Type, "name": officer.Name,
			"address": field(start+47, 42), "city": officer.City, "state": officer.State, "zip": field(start+119, 9),
		} {
			raw[prefix+key] = value
		}
		if officer.Name != "" {
			officers = append(officers, officer)
		}
	}
	principal := ""
	if len(officers) > 0 {
		principal = officers[0].Name
		for _, officer := range officers {
			if strings.Contains(strings.ToUpper(officer.Title), "P") {
				principal = officer.Name
				break
			}
		}
	}
	key, ref := NormID("filing:fl", raw["doc_number"])
	ids := map[string]string{key: ref}
	fei := strings.Map(func(r rune) rune {
		if r >= '0' && r <= '9' {
			return r
		}
		return -1
	}, raw["fei"])
	if len(fei) == 9 && fei != "000000000" {
		ids["fein"] = fei
	}
	zip := raw["zip"]
	if len(zip) > 5 {
		zip = zip[:5]
	}
	return Entity{
		SourceSystem: "fl_sunbiz", ExternalRef: ref, Line: line,
		Facts: resolver.RecordFacts{
			LegalName: raw["legal_name"], Address1: raw["address1"], Zip: zip, State: raw["state"],
			IDs: ids, Principal: principal, RegisteredAgent: raw["registered_agent"],
		},
		Extra: map[string]any{
			"status": raw["status"], "filing_type": raw["filing_type"], "file_date": raw["file_date"],
			"last_transaction_date": raw["last_transaction_date"], "officers": officers,
		},
		Raw: raw, SourceUpdatedAt: parseSunbizDate(raw["last_transaction_date"]),
	}
}

func sunbizDiff(old, new Entity, meta LoadMeta) []Signal {
	observedAt := meta.LoadedAt
	if new.SourceUpdatedAt != nil && (old.SourceUpdatedAt == nil || new.SourceUpdatedAt.After(*old.SourceUpdatedAt)) {
		observedAt = *new.SourceUpdatedAt
	}
	var signals []Signal
	add := func(kind proxy.SignalType, field string, value any) {
		signals = append(signals, Signal{
			SourceSystem: meta.SourceSystem, ExternalRef: new.ExternalRef, Type: kind,
			ObservedAt: observedAt, Citation: Cite(meta, new, field, value),
		})
	}
	oldNames, newNames := sunbizOfficerNames(old), sunbizOfficerNames(new)
	if len(oldNames) > 0 && len(newNames) > 0 && !maps.Equal(oldNames, newNames) {
		add(proxy.OfficerChange, "officers", new.Extra["officers"])
	}
	oldAgent, newAgent := resolver.NormalizeName(old.Facts.RegisteredAgent), resolver.NormalizeName(new.Facts.RegisteredAgent)
	if oldAgent != "" && newAgent != "" && oldAgent != newAgent {
		add(proxy.RegisteredAgentChange, "registered_agent", new.Facts.RegisteredAgent)
	}
	if resolver.NormalizeName(old.Facts.LegalName) != resolver.NormalizeName(new.Facts.LegalName) {
		add(proxy.EntityChange, "legal_name", new.Facts.LegalName)
	}
	return signals
}

func sunbizOfficerNames(e Entity) map[string]bool {
	names := make(map[string]bool)
	add := func(name string) {
		if normalized := resolver.NormalizeName(name); normalized != "" {
			names[normalized] = true
		}
	}
	switch officers := e.Extra["officers"].(type) {
	case []sunbizOfficer:
		for _, officer := range officers {
			add(officer.Name)
		}
	case []any:
		// jsonb decoding turns the stored typed slice into generic JSON values.
		for _, value := range officers {
			if officer, ok := value.(map[string]any); ok {
				if name, ok := officer["name"].(string); ok {
					add(name)
				}
			}
		}
	}
	return names
}
