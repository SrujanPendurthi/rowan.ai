package migrations

import "embed"

// FS contains the SQL migrations in filename order.
//
//go:embed *.sql
var FS embed.FS
