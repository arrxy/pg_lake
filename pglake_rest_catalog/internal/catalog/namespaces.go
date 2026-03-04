package catalog

import (
	"context"
	"errors"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/snowflake-labs/pg_lake/pglake_rest_catalog/internal/db"
)

var ErrNamespaceNotFound = errors.New("namespace not found")

type Service struct {
	pool *pgxpool.Pool
}

func NewService(pool *pgxpool.Pool) *Service {
	return &Service{pool: pool}
}

// ListNamespaces returns all namespaces for the given catalog.
// Each namespace is represented as a single-element string slice
// per the Iceberg REST spec.
func (s *Service) ListNamespaces(ctx context.Context, catalogName string) ([][]string, error) {
	rows, err := s.pool.Query(ctx, db.QueryListNamespaces, catalogName)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var namespaces [][]string
	for rows.Next() {
		var ns string
		if err := rows.Scan(&ns); err != nil {
			return nil, err
		}
		namespaces = append(namespaces, []string{ns})
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}

	if namespaces == nil {
		namespaces = [][]string{}
	}
	return namespaces, nil
}

// GetNamespace returns the properties for a namespace.
// Returns ErrNamespaceNotFound if the namespace does not exist.
func (s *Service) GetNamespace(ctx context.Context, catalogName, namespace string) (map[string]string, error) {
	var exists bool
	err := s.pool.QueryRow(ctx, db.QueryNamespaceExists, catalogName, namespace).Scan(&exists)
	if err != nil {
		return nil, err
	}
	if !exists {
		return nil, ErrNamespaceNotFound
	}

	rows, err := s.pool.Query(ctx, db.QueryGetNamespaceProperties, catalogName, namespace)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	props := make(map[string]string)
	for rows.Next() {
		var key, value string
		if err := rows.Scan(&key, &value); err != nil {
			return nil, err
		}
		props[key] = value
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}

	return props, nil
}
