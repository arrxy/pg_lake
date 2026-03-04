package db

const (
	// ListNamespaces returns all distinct namespaces for a catalog,
	// combining both tables and namespace_properties.
	QueryListNamespaces = `
		SELECT DISTINCT ns FROM (
			SELECT table_namespace AS ns FROM lake_iceberg.tables WHERE catalog_name = $1
			UNION
			SELECT namespace AS ns FROM lake_iceberg.namespace_properties WHERE catalog_name = $1
		) sub
		ORDER BY ns`

	// NamespaceExists checks whether a namespace exists in either
	// the tables view or namespace_properties.
	QueryNamespaceExists = `
		SELECT EXISTS(
			SELECT 1 FROM lake_iceberg.namespace_properties
			WHERE catalog_name = $1 AND namespace = $2
			UNION ALL
			SELECT 1 FROM lake_iceberg.tables
			WHERE catalog_name = $1 AND table_namespace = $2
		)`

	// GetNamespaceProperties returns all key-value properties for a namespace.
	QueryGetNamespaceProperties = `
		SELECT property_key, property_value
		FROM lake_iceberg.namespace_properties
		WHERE catalog_name = $1 AND namespace = $2`

	// ListTables returns all table names in a namespace.
	QueryListTables = `
		SELECT table_name
		FROM lake_iceberg.tables
		WHERE catalog_name = $1 AND table_namespace = $2
		ORDER BY table_name`

	// GetTableMetadataLocation returns the metadata_location for a specific table.
	QueryGetTableMetadataLocation = `
		SELECT metadata_location
		FROM lake_iceberg.tables
		WHERE catalog_name = $1 AND table_namespace = $2 AND table_name = $3`

	// GetTableMetadata calls the existing C function that reads Iceberg
	// metadata from S3 and returns the full JSON as text.
	QueryGetTableMetadata = `SELECT lake_iceberg.metadata($1)::text`
)
