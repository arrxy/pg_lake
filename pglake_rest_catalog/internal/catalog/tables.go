package catalog

import (
	"context"
	"encoding/json"
	"errors"

	"github.com/jackc/pgx/v5"
	"github.com/snowflake-labs/pg_lake/pglake_rest_catalog/internal/db"
)

var ErrTableNotFound = errors.New("table not found")

// TableIdentifier represents an Iceberg table identifier.
type TableIdentifier struct {
	Namespace []string `json:"namespace"`
	Name      string   `json:"name"`
}

// LoadTableResult contains the response data for the Load Table endpoint.
type LoadTableResult struct {
	MetadataLocation string          `json:"metadata-location"`
	Metadata         json.RawMessage `json:"metadata"`
	Config           map[string]string `json:"config"`
}

// ListTables returns all table identifiers in a namespace.
func (s *Service) ListTables(ctx context.Context, catalogName, namespace string) ([]TableIdentifier, error) {
	rows, err := s.pool.Query(ctx, db.QueryListTables, catalogName, namespace)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var tables []TableIdentifier
	for rows.Next() {
		var name string
		if err := rows.Scan(&name); err != nil {
			return nil, err
		}
		tables = append(tables, TableIdentifier{
			Namespace: []string{namespace},
			Name:      name,
		})
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}

	if tables == nil {
		tables = []TableIdentifier{}
	}
	return tables, nil
}

// LoadTable retrieves the full Iceberg table metadata for a specific table.
// It first looks up the metadata_location, then calls lake_iceberg.metadata()
// to get the full metadata JSON from S3.
func (s *Service) LoadTable(ctx context.Context, catalogName, namespace, tableName string) (*LoadTableResult, error) {
	var metadataLocation string
	err := s.pool.QueryRow(ctx, db.QueryGetTableMetadataLocation, catalogName, namespace, tableName).Scan(&metadataLocation)
	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return nil, ErrTableNotFound
		}
		return nil, err
	}

	var metadataJSON string
	err = s.pool.QueryRow(ctx, db.QueryGetTableMetadata, metadataLocation).Scan(&metadataJSON)
	if err != nil {
		return nil, err
	}

	return &LoadTableResult{
		MetadataLocation: metadataLocation,
		Metadata:         json.RawMessage(metadataJSON),
		Config:           map[string]string{},
	}, nil
}
