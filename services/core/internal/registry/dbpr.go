package registry

import (
	"context"
	"encoding/csv"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"

	"rowan.ai/rowan/services/core/internal/resolver"
)

const dbprMinFields = 21

// LoadDBPR loads a local, headerless construction or electrical license CSV.
// Read counts nonblank records; malformed records are excluded. Mutation counts
// cover committed batches. DBPR has no qualifier field and emits no v1 signals.
func LoadDBPR(ctx context.Context, pool *pgxpool.Pool, path, snapshotRef string, now time.Time) (stats LoadStats, err error) {
	file, err := os.Open(path)
	if err != nil {
		return stats, fmt.Errorf("open DBPR license extract: %w", err)
	}
	defer func() {
		if closeErr := file.Close(); closeErr != nil {
			err = errors.Join(err, fmt.Errorf("close DBPR license extract: %w", closeErr))
		}
	}()
	reader := csv.NewReader(file)
	reader.FieldsPerRecord = -1
	reader.LazyQuotes = true
	next := func() (Entity, bool, error) {
		for {
			if err := ctx.Err(); err != nil {
				return Entity{}, false, fmt.Errorf("read DBPR license extract: %w", err)
			}
			fields, err := reader.Read()
			if errors.Is(err, io.EOF) {
				return Entity{}, false, nil
			}
			line := 1
			if len(fields) > 0 {
				line, _ = reader.FieldPos(0)
			}
			if err != nil {
				var parseErr *csv.ParseError
				if len(fields) == 0 && errors.As(err, &parseErr) {
					line = parseErr.StartLine
				}
				return Entity{}, false, fmt.Errorf("DBPR line %d: %w", line, err)
			}
			blank := true
			for i, value := range fields {
				fields[i] = strings.TrimSpace(strings.ToValidUTF8(value, "�"))
				if fields[i] != "" {
					blank = false
				}
			}
			if blank {
				continue
			}
			if len(fields) < dbprMinFields {
				return Entity{}, false, fmt.Errorf("DBPR line %d: got %d fields, need at least %d", line, len(fields), dbprMinFields)
			}
			return dbprEntity(fields, line), true, nil
		}
	}
	meta := LoadMeta{SourceSystem: "fl_dbpr", TableName: "dbpr_license", FileName: filepath.Base(path),
		SnapshotRef: snapshotRef, LoadedAt: now}
	return Load(ctx, pool, meta, next, nil)
}

func parseDBPRDate(s string) *time.Time {
	s = strings.TrimSpace(s)
	for _, layout := range []string{"01/02/2006", "20060102"} {
		if date, err := time.Parse(layout, s); err == nil {
			return &date
		}
	}
	return nil
}

func dbprEntity(fields []string, line int) Entity {
	raw := map[string]string{
		"board": fields[0], "occupation_code": fields[1], "licensee_name": fields[2], "dba_name": fields[3],
		"class_code": fields[4], "address1": fields[5], "address2": fields[6], "address3": fields[7],
		"city": fields[8], "state": fields[9], "zip": fields[10], "county_code": fields[11],
		"license_number": fields[12], "primary_status": fields[13], "secondary_status": fields[14],
		"original_date": fields[15], "effective_date": fields[16], "expiration_date": fields[17],
		"renewal_period": fields[19], "alternate_license_number": fields[20],
	}
	key, ref := NormID("license:fl", raw["license_number"])
	name := raw["dba_name"]
	if name == "" {
		name = raw["licensee_name"]
	}
	zip := raw["zip"]
	if len(zip) > 5 {
		zip = zip[:5]
	}
	return Entity{
		SourceSystem: "fl_dbpr", ExternalRef: ref, Line: line,
		Facts: resolver.RecordFacts{
			LegalName: name, DBA: raw["dba_name"], Principal: raw["licensee_name"],
			Address1: raw["address1"], State: raw["state"], Zip: zip, IDs: map[string]string{key: ref},
		},
		Extra: map[string]any{
			"board": raw["board"], "occupation_code": raw["occupation_code"], "class_code": raw["class_code"],
			"primary_status": raw["primary_status"], "secondary_status": raw["secondary_status"],
			"original_date": raw["original_date"], "expiration_date": raw["expiration_date"], "county_code": raw["county_code"],
		},
		Raw: raw, SourceUpdatedAt: parseDBPRDate(raw["effective_date"]),
	}
}
