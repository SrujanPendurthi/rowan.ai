// Package registry stores public registry entities and cited change signals.
package registry

import (
	"encoding/json"
	"fmt"
	"strings"
	"time"

	"rowan.ai/rowan/services/core/internal/proxy"
	"rowan.ai/rowan/services/core/internal/quality"
	"rowan.ai/rowan/services/core/internal/resolver"
)

type Entity struct {
	SourceSystem    string               `json:"source_system"`
	ExternalRef     string               `json:"external_ref"`
	Facts           resolver.RecordFacts `json:"facts"`
	Extra           map[string]any       `json:"extra"`
	Raw             map[string]string    `json:"raw"`
	SourceUpdatedAt *time.Time           `json:"source_updated_at"`
	Line            int                  `json:"line,omitempty"`
	// Tombstone asks Load to delete the stored row instead of upserting it.
	Tombstone bool `json:"-"`
}

type Signal struct {
	SourceSystem string
	ExternalRef  string
	Type         proxy.SignalType
	ObservedAt   time.Time
	Citation     quality.Citation
}

type LoadMeta struct {
	SourceSystem, FileName, TableName, SnapshotRef string
	LoadedAt                                       time.Time
}

type LoadStats struct {
	Read, Inserted, Updated, Unchanged, Skipped, Signals int
	// SignalRefs contains sorted, unique refs with at least one newly inserted signal.
	SignalRefs []string
}

// Diff runs only for an existing row whose content hash changed.
type Diff func(old, new Entity, meta LoadMeta) []Signal

// NormID normalizes a source's identity key and value for exact lookup.
func NormID(key, value string) (string, string) {
	return strings.ToLower(strings.TrimSpace(key)), strings.ToUpper(strings.TrimSpace(value))
}

// Cite preserves the raw filing's field value and source location. Values must
// support JSON encoding; metadata must include a source, file, and load time.
func Cite(meta LoadMeta, e Entity, field string, value any) quality.Citation {
	encoded, err := json.Marshal(value)
	if err != nil {
		// The public helper cannot return an error. Never silently replace a value.
		panic(fmt.Errorf("registry cite %s: %w", field, err))
	}
	return quality.Citation{
		FieldName: field, Value: encoded,
		Origin: quality.RawSource, Status: quality.VerifiedFiling, ExportPolicy: quality.Allow,
		SourceSystem: meta.SourceSystem, SourceRef: meta.FileName, TableName: meta.TableName,
		RowKey: e.ExternalRef, PageOrSpan: fmt.Sprintf("line %d", e.Line), RetrievedAt: meta.LoadedAt,
	}
}
