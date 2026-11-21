package registry

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"slices"
	"sort"
	"strings"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"

	"rowan.ai/rowan/services/core/internal/db"
	"rowan.ai/rowan/services/core/internal/resolver"
)

const batchSize = 5000

// Load commits entities and their diff signals together in batches of 5000.
// On failure, mutation counts describe only committed batches; Read includes
// every successfully read row, including the discarded in-flight batch.
func Load(ctx context.Context, pool *pgxpool.Pool, meta LoadMeta, next func() (Entity, bool, error), diff Diff) (stats LoadStats, err error) {
	refs := make(map[string]bool)
	defer func() {
		for ref := range refs {
			stats.SignalRefs = append(stats.SignalRefs, ref)
		}
		sort.Strings(stats.SignalRefs)
	}()
	for {
		batch := make([]Entity, 0, batchSize)
		for len(batch) < batchSize {
			if err := ctx.Err(); err != nil {
				return stats, fmt.Errorf("registry load: %w", err)
			}
			e, ok, err := next()
			if err != nil {
				return stats, fmt.Errorf("registry read: %w", err)
			}
			if !ok {
				break
			}
			stats.Read++
			batch = append(batch, e)
		}
		if len(batch) == 0 {
			return stats, nil
		}
		committed, err := loadBatch(ctx, pool, meta, batch, diff)
		if err != nil {
			return stats, err
		}
		stats.Inserted += committed.Inserted
		stats.Updated += committed.Updated
		stats.Unchanged += committed.Unchanged
		stats.Skipped += committed.Skipped
		stats.Signals += committed.Signals
		for _, ref := range committed.SignalRefs {
			refs[ref] = true
		}
		if len(batch) < batchSize {
			return stats, nil
		}
	}
}

type entityKey struct{ source, ref string }

type storedEntity struct {
	hash   []byte
	entity Entity
}

type change struct {
	old, new Entity
	existed  bool
	applied  bool
}

func loadBatch(ctx context.Context, pool *pgxpool.Pool, meta LoadMeta, entities []Entity, diff Diff) (stats LoadStats, err error) {
	err = db.InTx(ctx, pool, func(tx pgx.Tx) error {
		stored, err := readStored(ctx, tx, entities)
		if err != nil {
			return err
		}
		batch := &pgx.Batch{}
		changes := make([]change, 0, len(entities))
		for _, e := range entities {
			names := normalizedNames(e)
			// Missing identities cannot support registry lookup or resolution.
			if strings.TrimSpace(e.ExternalRef) == "" || strings.TrimSpace(e.Facts.LegalName) == "" || len(names) == 0 {
				stats.Skipped++
				continue
			}
			encoded, err := json.Marshal(e)
			if err != nil {
				return fmt.Errorf("encode registry entity %s: %w", e.ExternalRef, err)
			}
			content := e
			content.Line = 0 // Line is provenance, not content; omitempty excludes it.
			canonical, err := json.Marshal(content)
			if err != nil {
				return fmt.Errorf("hash registry entity %s: %w", e.ExternalRef, err)
			}
			hash := sha256.Sum256(canonical)
			key := entityKey{e.SourceSystem, e.ExternalRef}
			old, existed := stored[key]
			if existed && bytes.Equal(old.hash, hash[:]) {
				stats.Unchanged++
				continue
			}
			ids := e.Facts.IDs
			if ids == nil {
				ids = map[string]string{}
			}
			idsJSON, err := json.Marshal(ids)
			if err != nil {
				return fmt.Errorf("encode registry IDs %s: %w", e.ExternalRef, err)
			}
			batch.Queue(`INSERT INTO registry_entities
				(source_system, external_ref, legal_name, names, state, zip5, ids, entity,
				 content_hash, file_name, snapshot_ref, source_updated_at, loaded_at)
				VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13)
				ON CONFLICT (source_system, external_ref) DO UPDATE SET
				legal_name = EXCLUDED.legal_name, names = EXCLUDED.names, state = EXCLUDED.state,
				zip5 = EXCLUDED.zip5, ids = EXCLUDED.ids, entity = EXCLUDED.entity,
				content_hash = EXCLUDED.content_hash, file_name = EXCLUDED.file_name,
				snapshot_ref = EXCLUDED.snapshot_ref, source_updated_at = EXCLUDED.source_updated_at,
				loaded_at = EXCLUDED.loaded_at
				WHERE registry_entities.content_hash <> EXCLUDED.content_hash`,
				e.SourceSystem, e.ExternalRef, e.Facts.LegalName, names, e.Facts.State, e.Facts.Zip,
				idsJSON, encoded, hash[:], meta.FileName, meta.SnapshotRef, e.SourceUpdatedAt, meta.LoadedAt)
			changes = append(changes, change{old: old.entity, new: e, existed: existed})
			// Duplicate keys in one batch observe earlier rows from that batch.
			stored[key] = storedEntity{hash: hash[:], entity: e}
		}
		if len(changes) == 0 {
			return nil
		}
		results := tx.SendBatch(ctx, batch)
		for i := range changes {
			tag, err := results.Exec()
			if err != nil {
				_ = results.Close()
				return fmt.Errorf("upsert registry entity %s: %w", changes[i].new.ExternalRef, err)
			}
			changes[i].applied = tag.RowsAffected() > 0
		}
		if err := results.Close(); err != nil {
			return fmt.Errorf("close registry batch: %w", err)
		}
		for _, c := range changes {
			if !c.applied {
				stats.Unchanged++
				continue
			}
			if !c.existed {
				stats.Inserted++
				continue
			}
			stats.Updated++
			if diff != nil {
				count, refs, err := insertSignals(ctx, tx, diff(c.old, c.new, meta))
				if err != nil {
					return err
				}
				stats.Signals += count
				stats.SignalRefs = append(stats.SignalRefs, refs...)
			}
		}
		return nil
	})
	if err != nil {
		return LoadStats{}, fmt.Errorf("registry batch: %w", err)
	}
	return stats, nil
}

func normalizedNames(e Entity) []string {
	candidates := []string{e.Facts.LegalName, e.Facts.DBA}
	if e.SourceSystem == "fl_dbpr" {
		// Licensees may trade under a DBA; retain both searchable names.
		candidates = append(candidates, e.Facts.Principal)
	}
	names := make([]string, 0, len(candidates))
	for _, name := range candidates {
		name = resolver.NormalizeName(name)
		if name != "" && !slices.Contains(names, name) {
			names = append(names, name)
		}
	}
	return names
}

func readStored(ctx context.Context, tx pgx.Tx, entities []Entity) (map[entityKey]storedEntity, error) {
	sources := make([]string, len(entities))
	refs := make([]string, len(entities))
	for i, e := range entities {
		sources[i], refs[i] = e.SourceSystem, e.ExternalRef
	}
	rows, err := tx.Query(ctx, `SELECT source_system, external_ref, content_hash, entity
		FROM registry_entities WHERE (source_system, external_ref) IN
		(SELECT * FROM unnest($1::text[], $2::text[]))`, sources, refs)
	if err != nil {
		return nil, fmt.Errorf("read stored registry entities: %w", err)
	}
	defer rows.Close()
	stored := make(map[entityKey]storedEntity, len(entities))
	for rows.Next() {
		var key entityKey
		var old storedEntity
		if err := rows.Scan(&key.source, &key.ref, &old.hash, &old.entity); err != nil {
			return nil, fmt.Errorf("decode stored registry entity: %w", err)
		}
		stored[key] = old
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("iterate stored registry entities: %w", err)
	}
	return stored, nil
}

// InsertSignals inserts new signals without duplicating an existing observation.
func InsertSignals(ctx context.Context, tx pgx.Tx, sigs []Signal) (int, error) {
	count, _, err := insertSignals(ctx, tx, sigs)
	return count, err
}

func insertSignals(ctx context.Context, tx pgx.Tx, sigs []Signal) (int, []string, error) {
	count := 0
	var refs []string
	for _, sig := range sigs {
		if err := sig.Citation.Validate(); err != nil {
			return count, refs, fmt.Errorf("validate registry signal %s: %w", sig.ExternalRef, err)
		}
		citation, err := json.Marshal(sig.Citation)
		if err != nil {
			return count, refs, fmt.Errorf("encode registry signal %s: %w", sig.ExternalRef, err)
		}
		key := strings.Join([]string{sig.SourceSystem, sig.ExternalRef, string(sig.Type),
			sig.ObservedAt.UTC().Format("2006-01-02"), string(sig.Citation.Value)}, "|")
		hash := sha256.Sum256([]byte(key))
		tag, err := tx.Exec(ctx, `INSERT INTO registry_signals
			(source_system, external_ref, signal_type, observed_at, citation, dedupe_key)
			VALUES ($1, $2, $3, $4, $5, $6) ON CONFLICT (dedupe_key) DO NOTHING`,
			sig.SourceSystem, sig.ExternalRef, sig.Type, sig.ObservedAt, citation, hex.EncodeToString(hash[:]))
		if err != nil {
			return count, refs, fmt.Errorf("insert registry signal %s: %w", sig.ExternalRef, err)
		}
		if tag.RowsAffected() > 0 {
			count++
			refs = append(refs, sig.ExternalRef)
		}
	}
	return count, refs, nil
}
