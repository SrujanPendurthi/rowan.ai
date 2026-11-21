package registry

import (
	"context"
	"encoding/json"
	"fmt"
	"sort"
	"strings"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"

	"rowan.ai/rowan/services/core/internal/enrich"
	"rowan.ai/rowan/services/core/internal/quality"
	"rowan.ai/rowan/services/core/internal/resolver"
)

// Provider answers enrichment lookups from loaded registry_entities.
type Provider struct {
	Pool         *pgxpool.Pool
	MaxPerSource int
}

// registryTables maps a source system to its citation table name.
var registryTables = map[string]string{"sam": "sam_public_v2", "fl_sunbiz": "cordata", "fl_dbpr": "dbpr_license"}

func (p *Provider) Name() string { return "registry" }

const selectRows = `SELECT source_system, external_ref, entity, file_name, loaded_at FROM (
	SELECT *, row_number() OVER (PARTITION BY source_system ORDER BY external_ref) AS rn
	FROM registry_entities WHERE %s) t WHERE rn <= $1 ORDER BY source_system, external_ref`

// Lookup matches by exact ID first; only without an ID hit does it fall back to name (and state).
func (p *Provider) Lookup(ctx context.Context, c enrich.Candidate) ([]resolver.SourceRecordInput, error) {
	limit := p.MaxPerSource
	if limit <= 0 {
		limit = 3
	}
	keys := make([]string, 0, len(c.IDs))
	ids := map[string]string{}
	for k, v := range c.IDs {
		k, v = NormID(k, v)
		if k != "" && v != "" {
			if _, dup := ids[k]; !dup {
				keys = append(keys, k)
			}
			ids[k] = v
		}
	}
	sort.Strings(keys)
	if len(keys) > 0 {
		args := []any{limit}
		var conds []string
		for _, k := range keys {
			args = append(args, k, ids[k])
			conds = append(conds, fmt.Sprintf("ids @> jsonb_build_object($%d::text, $%d::text)", len(args)-1, len(args)))
		}
		out, err := p.query(ctx, fmt.Sprintf(selectRows, strings.Join(conds, " OR ")), args)
		if err != nil || len(out) > 0 {
			return out, err
		}
	}
	name := resolver.NormalizeName(c.LegalName)
	if name == "" {
		return []resolver.SourceRecordInput{}, nil
	}
	cond, args := "names @> ARRAY[$2::text]", []any{limit, name}
	if c.State != "" {
		cond += " AND state = upper($3::text)"
		args = append(args, c.State)
	}
	return p.query(ctx, fmt.Sprintf(selectRows, cond), args)
}

func (p *Provider) query(ctx context.Context, sql string, args []any) ([]resolver.SourceRecordInput, error) {
	rows, err := p.Pool.Query(ctx, sql, args...)
	if err != nil {
		return nil, fmt.Errorf("registry lookup: %w", err)
	}
	defer rows.Close()
	out := []resolver.SourceRecordInput{}
	for rows.Next() {
		var (
			system, ref, file string
			raw               []byte
			loaded            time.Time
		)
		if err := rows.Scan(&system, &ref, &raw, &file, &loaded); err != nil {
			return nil, fmt.Errorf("registry lookup scan: %w", err)
		}
		var e Entity
		if err := json.Unmarshal(raw, &e); err != nil {
			return nil, fmt.Errorf("registry lookup decode %s/%s: %w", system, ref, err)
		}
		out = append(out, resolver.SourceRecordInput{SourceSystem: system, ExternalRef: ref, Facts: e.Facts,
			Payload: raw, FetchedAt: loaded, Citations: citations(system, file, loaded, e)})
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("registry lookup rows: %w", err)
	}
	return out, nil
}

func citations(system, file string, loaded time.Time, e Entity) []quality.Citation {
	meta := LoadMeta{SourceSystem: system, FileName: file, TableName: registryTables[system], LoadedAt: loaded}
	f := e.Facts
	fields := []struct{ name, value string }{
		{"legal_name", f.LegalName}, {"dba", f.DBA}, {"address", f.Address1}, {"zip", f.Zip}, {"state", f.State},
		{"principal", f.Principal}, {"registered_agent", f.RegisteredAgent}, {"domain", f.Domain},
	}
	for _, k := range []string{"primary_naics", "status"} {
		if v, ok := e.Extra[k].(string); ok {
			fields = append(fields, struct{ name, value string }{k, v})
		}
	}
	var out []quality.Citation
	for _, fv := range fields {
		if fv.value != "" {
			out = append(out, Cite(meta, e, fv.name, fv.value))
		}
	}
	return out
}
