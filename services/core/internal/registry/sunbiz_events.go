package registry

import (
	"bufio"
	"context"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"

	"rowan.ai/rowan/services/core/internal/db"
	"rowan.ai/rowan/services/core/internal/proxy"
)

const sunbizEventRecordBytes = 662

type EventStats struct {
	Read, Signals, Unmapped int
	// TopUnmapped contains up to 20 "DESC (n)" entries, ordered by count then description.
	TopUnmapped []string
	// SignalRefs contains sorted, unique refs with at least one newly inserted signal.
	SignalRefs []string
}

// LoadSunbizEvents loads historical filing signals without changing registry entities.
// Batches commit every 5000 input rows. On failure, Signals and SignalRefs describe
// committed batches; Read and Unmapped also include the discarded in-flight batch.
func LoadSunbizEvents(ctx context.Context, pool *pgxpool.Pool, path, snapshotRef string, now time.Time) (stats EventStats, err error) {
	file, err := os.Open(path)
	if err != nil {
		return stats, fmt.Errorf("open Sunbiz event extract: %w", err)
	}
	defer func() {
		if closeErr := file.Close(); closeErr != nil {
			err = errors.Join(err, fmt.Errorf("close Sunbiz event extract: %w", closeErr))
		}
	}()
	refs := make(map[string]bool)
	unmapped := make(map[string]int)
	defer func() {
		for ref := range refs {
			stats.SignalRefs = append(stats.SignalRefs, ref)
		}
		sort.Strings(stats.SignalRefs)
		descriptions := make([]string, 0, len(unmapped))
		for desc := range unmapped {
			descriptions = append(descriptions, desc)
		}
		sort.Slice(descriptions, func(i, j int) bool {
			if unmapped[descriptions[i]] != unmapped[descriptions[j]] {
				return unmapped[descriptions[i]] > unmapped[descriptions[j]]
			}
			return descriptions[i] < descriptions[j]
		})
		for _, desc := range descriptions[:min(20, len(descriptions))] {
			stats.TopUnmapped = append(stats.TopUnmapped, fmt.Sprintf("%s (%d)", desc, unmapped[desc]))
		}
	}()
	meta := LoadMeta{SourceSystem: "fl_sunbiz", TableName: "corevent", FileName: filepath.Base(path),
		SnapshotRef: snapshotRef, LoadedAt: now}
	reader := bufio.NewReader(file)
	line := 0
	for {
		signals := make([]Signal, 0, batchSize)
		read := 0
		for read < batchSize {
			if err := ctx.Err(); err != nil {
				return stats, fmt.Errorf("Sunbiz event load: %w", err)
			}
			row, err := reader.ReadString('\n')
			if err != nil && !errors.Is(err, io.EOF) {
				return stats, fmt.Errorf("Sunbiz event line %d: %w", line+1, err)
			}
			if len(row) == 0 && errors.Is(err, io.EOF) {
				break
			}
			line++
			row = strings.TrimSuffix(strings.TrimSuffix(row, "\n"), "\r")
			if len(row) < sunbizEventRecordBytes {
				return stats, fmt.Errorf("Sunbiz event line %d: got %d bytes, need at least %d", line, len(row), sunbizEventRecordBytes)
			}
			stats.Read++
			read++
			sig, desc, ok := sunbizEventSignal(row[:sunbizEventRecordBytes], line, meta)
			if !ok {
				stats.Unmapped++
				unmapped[desc]++
				continue
			}
			signals = append(signals, sig)
		}
		count, insertedRefs, err := insertSunbizEventBatch(ctx, pool, signals)
		if err != nil {
			return stats, err
		}
		stats.Signals += count
		for _, ref := range insertedRefs {
			refs[ref] = true
		}
		if read < batchSize {
			return stats, nil
		}
	}
}

func sunbizEventSignal(row string, line int, meta LoadMeta) (Signal, string, bool) {
	field := func(start, length int) string {
		return strings.TrimSpace(strings.ToValidUTF8(row[start-1:start-1+length], ""))
	}
	desc := field(38, 40)
	kind, ok := classifyEvent(desc, field(413, 1) == "Y")
	observedAt := parseSunbizDate(field(86, 8))
	if observedAt == nil {
		observedAt = parseSunbizDate(field(78, 8))
	}
	if !ok || observedAt == nil {
		return Signal{}, strings.ToUpper(desc), false
	}
	_, ref := NormID("filing:fl", field(1, 12))
	entity := Entity{ExternalRef: ref, Line: line}
	// Sequence participates in InsertSignals' citation-value hash, preserving
	// distinct same-document events of the same type filed on the same day.
	value := map[string]string{"seq": field(13, 5), "code": field(18, 20), "description": desc}
	return Signal{
		SourceSystem: meta.SourceSystem, ExternalRef: ref, Type: kind, ObservedAt: *observedAt,
		Citation: Cite(meta, entity, "event", value),
	}, desc, true
}

func insertSunbizEventBatch(ctx context.Context, pool *pgxpool.Pool, signals []Signal) (int, []string, error) {
	if len(signals) == 0 {
		return 0, nil, nil
	}
	// InsertSignals returns counts, so group by document to identify exactly
	// which refs gained signals, including batches mixing old and new events.
	grouped := make(map[string][]Signal)
	var order []string
	for _, sig := range signals {
		if _, ok := grouped[sig.ExternalRef]; !ok {
			order = append(order, sig.ExternalRef)
		}
		grouped[sig.ExternalRef] = append(grouped[sig.ExternalRef], sig)
	}
	count := 0
	var refs []string
	err := db.InTx(ctx, pool, func(tx pgx.Tx) error {
		for _, ref := range order {
			inserted, err := InsertSignals(ctx, tx, grouped[ref])
			if err != nil {
				return err
			}
			count += inserted
			if inserted > 0 {
				refs = append(refs, ref)
			}
		}
		return nil
	})
	if err != nil {
		return 0, nil, fmt.Errorf("Sunbiz event batch: %w", err)
	}
	return count, refs, nil
}

// ponytail: keyword map from unpublished event codes; refine from TopUnmapped after the first real load.
func classifyEvent(desc string, nameChange bool) (proxy.SignalType, bool) {
	desc = strings.ToUpper(desc)
	switch {
	case nameChange:
		return proxy.EntityChange, true
	case strings.Contains(desc, "REGISTERED AGENT") || strings.Contains(desc, "REG. AGENT"):
		return proxy.RegisteredAgentChange, true
	case strings.Contains(desc, "MERGER") || strings.Contains(desc, "CONVERSION") || strings.Contains(desc, "NAME CHANGE"):
		return proxy.EntityChange, true
	case strings.Contains(desc, "OFFICER") || strings.Contains(desc, "DIRECTOR") || strings.Contains(desc, "MANAGER") || strings.Contains(desc, "MEMBER"):
		return proxy.OfficerChange, true
	default:
		return "", false
	}
}
