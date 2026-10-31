package persistence

import "embed"

//go:embed schema.sql
var schemaFS embed.FS

func schemaSQL() ([]byte, error) { return schemaFS.ReadFile("schema.sql") }
