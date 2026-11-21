package registry

import (
	"archive/zip"
	"bufio"
	"context"
	"errors"
	"fmt"
	"io"
	"net/url"
	"os"
	"path/filepath"
	"strings"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"

	"rowan.ai/rowan/services/core/internal/fetch"
	"rowan.ai/rowan/services/core/internal/resolver"
)

var ErrUnknownFormat = errors.New("registry: unknown file format")

// Column numbers follow the 1-based SAM Master Extract Public File V2 layout.
const (
	samUEIColumn                      = 1
	samCAGEColumn                     = 4
	samExtractCodeColumn              = 6
	samInitialRegistrationDateColumn  = 8
	samLastUpdateDateColumn           = 10
	samLegalBusinessNameColumn        = 12
	samDBANameColumn                  = 13
	samPhysicalAddressLine1Column     = 16
	samCityColumn                     = 18
	samStateColumn                    = 19
	samZIPColumn                      = 20
	samCountryCodeColumn              = 22
	samEntityStartDateColumn          = 25
	samEntityURLColumn                = 27
	samPrimaryNAICSColumn             = 33
	samNAICSStringColumn              = 35
	samGovtBusinessPOCFirstNameColumn = 47
	samGovtBusinessPOCLastNameColumn  = 49
	samNoPublicDisplayColumn          = 119
	samMaxLineBytes                   = 1 << 20
)

// LoadSAM loads a public monthly V2 extract, selecting the first .dat in a ZIP.
// Read includes successfully parsed data records, including NPDY skips; framing
// lines and malformed records are excluded. Mutation counts cover committed batches.
func LoadSAM(ctx context.Context, pool *pgxpool.Pool, path, snapshotRef string, now time.Time) (stats LoadStats, err error) {
	reader, closeFile, err := openSAM(path)
	if err != nil {
		return stats, err
	}
	defer func() {
		if closeErr := closeFile(); closeErr != nil {
			err = errors.Join(err, fmt.Errorf("close SAM extract: %w", closeErr))
		}
	}()
	scanner := bufio.NewScanner(reader)
	// Reserve space for a full 1 MiB line plus CRLF so Scanner accepts the limit.
	scanner.Buffer(make([]byte, 64*1024), samMaxLineBytes+2)
	line := 0
	scan := func() (bool, error) {
		if err := ctx.Err(); err != nil {
			return false, fmt.Errorf("SAM line %d: %w", line+1, err)
		}
		if !scanner.Scan() {
			if err := scanner.Err(); err != nil {
				return false, fmt.Errorf("SAM line %d: %w", line+1, err)
			}
			return false, nil
		}
		line++
		if len(scanner.Bytes()) > samMaxLineBytes {
			return false, fmt.Errorf("SAM line %d: exceeds 1 MiB", line)
		}
		return true, nil
	}
	ok, err := scan()
	if err != nil {
		return stats, err
	}
	if !ok || !strings.HasPrefix(scanner.Text(), "BOF PUBLIC") {
		return stats, fmt.Errorf("SAM line 1: expected BOF PUBLIC: %w", ErrUnknownFormat)
	}
	skipped := 0
	next := func() (Entity, bool, error) {
		for {
			ok, err := scan()
			if err != nil {
				return Entity{}, false, err
			}
			if !ok {
				return Entity{}, false, fmt.Errorf("SAM line %d: missing EOF: %w", line+1, ErrUnknownFormat)
			}
			if strings.HasPrefix(scanner.Text(), "EOF") {
				// Consume the stream to validate the trailer and ZIP checksum.
				ok, err := scan()
				if err != nil {
					return Entity{}, false, err
				}
				if ok {
					return Entity{}, false, fmt.Errorf("SAM line %d: content after EOF: %w", line, ErrUnknownFormat)
				}
				return Entity{}, false, nil
			}
			fields := strings.Split(scanner.Text(), "|")
			for i := range fields {
				fields[i] = strings.TrimSpace(fields[i])
			}
			if len(fields) == 1 {
				return Entity{}, false, fmt.Errorf("SAM line %d: expected pipe-delimited record: %w", line, ErrUnknownFormat)
			}
			if len(fields) < samNoPublicDisplayColumn {
				return Entity{}, false, fmt.Errorf("SAM line %d: got %d fields, need at least %d", line, len(fields), samNoPublicDisplayColumn)
			}
			if fields[len(fields)-1] != "!end" {
				return Entity{}, false, fmt.Errorf("SAM line %d: missing !end field: %w", line, ErrUnknownFormat)
			}
			if fields[samNoPublicDisplayColumn-1] == "NPDY" {
				skipped++
				continue
			}
			e, err := samEntity(fields, line)
			return e, err == nil, err
		}
	}
	meta := LoadMeta{SourceSystem: "sam", TableName: "sam_public_v2", FileName: filepath.Base(path),
		SnapshotRef: snapshotRef, LoadedAt: now}
	stats, err = Load(ctx, pool, meta, next, nil)
	stats.Read += skipped
	stats.Skipped += skipped
	return stats, err
}

func openSAM(path string) (io.Reader, func() error, error) {
	switch strings.ToLower(filepath.Ext(path)) {
	case ".dat":
		file, err := os.Open(path)
		if err != nil {
			return nil, nil, fmt.Errorf("open SAM extract: %w", err)
		}
		return file, file.Close, nil
	case ".zip":
		archive, err := zip.OpenReader(path)
		if err != nil {
			return nil, nil, fmt.Errorf("open SAM ZIP: %w", err)
		}
		for _, file := range archive.File {
			if file.FileInfo().IsDir() || !strings.EqualFold(filepath.Ext(file.Name), ".dat") {
				continue
			}
			reader, err := file.Open()
			if err != nil {
				_ = archive.Close()
				return nil, nil, fmt.Errorf("open SAM ZIP entry: %w", err)
			}
			return reader, func() error { return errors.Join(reader.Close(), archive.Close()) }, nil
		}
		_ = archive.Close()
		return nil, nil, fmt.Errorf("SAM ZIP has no .dat entry: %w", ErrUnknownFormat)
	default:
		return nil, nil, fmt.Errorf("SAM extract extension: %w", ErrUnknownFormat)
	}
}

func samEntity(fields []string, line int) (Entity, error) {
	field := func(column int) string { return fields[column-1] }
	entityURL := fetch.RedactURL(field(samEntityURLColumn))
	ids := make(map[string]string)
	for key, value := range map[string]string{"uei": field(samUEIColumn), "cage": field(samCAGEColumn)} {
		key, value = NormID(key, value)
		if value != "" {
			ids[key] = value
		}
	}
	zip5 := field(samZIPColumn)
	if len(zip5) > 5 {
		zip5 = zip5[:5]
	}
	naics := []string{}
	if value := field(samNAICSStringColumn); value != "" {
		naics = strings.Split(value, "~")
		for i := range naics {
			naics[i] = strings.TrimSpace(naics[i])
		}
	}
	e := Entity{
		SourceSystem: "sam", ExternalRef: ids["uei"], Line: line,
		Facts: resolver.RecordFacts{
			LegalName: field(samLegalBusinessNameColumn), DBA: field(samDBANameColumn),
			Address1: field(samPhysicalAddressLine1Column), State: field(samStateColumn), Zip: zip5,
			Domain: samDomain(entityURL), IDs: ids,
			Principal: strings.TrimSpace(field(samGovtBusinessPOCFirstNameColumn) + " " + field(samGovtBusinessPOCLastNameColumn)),
		},
		Extra: map[string]any{
			"primary_naics": field(samPrimaryNAICSColumn), "naics": naics,
			"extract_code": field(samExtractCodeColumn), "registration_date": field(samInitialRegistrationDateColumn),
			"entity_start_date": field(samEntityStartDateColumn), "country": field(samCountryCodeColumn),
		},
		Raw: map[string]string{
			"uei": field(samUEIColumn), "cage": field(samCAGEColumn), "extract_code": field(samExtractCodeColumn),
			"initial_registration_date": field(samInitialRegistrationDateColumn), "last_update_date": field(samLastUpdateDateColumn),
			"legal_business_name": field(samLegalBusinessNameColumn), "dba_name": field(samDBANameColumn),
			"physical_address_line_1": field(samPhysicalAddressLine1Column), "city": field(samCityColumn),
			"state": field(samStateColumn), "zip": field(samZIPColumn), "country_code": field(samCountryCodeColumn),
			"entity_start_date": field(samEntityStartDateColumn), "entity_url": entityURL,
			"primary_naics": field(samPrimaryNAICSColumn), "naics_string": field(samNAICSStringColumn),
			"govt_business_poc_first_name": field(samGovtBusinessPOCFirstNameColumn),
			"govt_business_poc_last_name":  field(samGovtBusinessPOCLastNameColumn),
			"no_public_display":            field(samNoPublicDisplayColumn),
		},
	}
	if value := field(samLastUpdateDateColumn); value != "" {
		updated, err := time.Parse("20060102", value)
		if err != nil {
			return Entity{}, fmt.Errorf("SAM line %d: parse last update date: %w", line, err)
		}
		e.SourceUpdatedAt = &updated
	}
	return e, nil
}

func samDomain(value string) string {
	if value == "" {
		return ""
	}
	if !strings.Contains(value, "://") && !strings.HasPrefix(value, "//") {
		value = "//" + value
	}
	parsed, err := url.Parse(value)
	if err != nil {
		return ""
	}
	return strings.TrimPrefix(strings.ToLower(parsed.Hostname()), "www.")
}
