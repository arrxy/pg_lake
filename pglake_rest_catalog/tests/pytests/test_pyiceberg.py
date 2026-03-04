"""End-to-end integration tests using pyiceberg RestCatalog."""

from utils_pytest import *
from helpers.pglake_rest_catalog import (
    PGLAKE_REST_CATALOG_PORT,
    PGLAKE_REST_CATALOG_CLIENT_ID,
    PGLAKE_REST_CATALOG_CLIENT_SECRET,
)


def _create_pyiceberg_catalog():
    """Create a pyiceberg RestCatalog pointed at our REST server."""
    from pyiceberg.catalog.rest import RestCatalog

    return RestCatalog(
        "pglake_test",
        **{
            "uri": f"http://localhost:{PGLAKE_REST_CATALOG_PORT}/v1",
            "warehouse": server_params.PG_DATABASE,
            "credential": f"{PGLAKE_REST_CATALOG_CLIENT_ID}:{PGLAKE_REST_CATALOG_CLIENT_SECRET}",
        },
    )


def test_pyiceberg_list_namespaces(
    pg_conn, pglake_rest_catalog, extension, with_default_location, s3
):
    """pyiceberg should be able to list namespaces via our REST catalog."""
    schema_name = "test_pyiceberg_ns"
    run_command(f'CREATE SCHEMA "{schema_name}"', pg_conn)
    run_command(
        f'CREATE TABLE "{schema_name}".tbl(a int) USING iceberg',
        pg_conn,
    )
    pg_conn.commit()

    try:
        catalog = _create_pyiceberg_catalog()
        namespaces = catalog.list_namespaces()
        ns_names = [ns[0] for ns in namespaces]
        assert schema_name in ns_names
    finally:
        run_command(f'DROP SCHEMA "{schema_name}" CASCADE', pg_conn)
        pg_conn.commit()


def test_pyiceberg_list_tables(
    pg_conn, pglake_rest_catalog, extension, with_default_location, s3
):
    """pyiceberg should be able to list tables in a namespace."""
    schema_name = "test_pyiceberg_tbls"
    run_command(f'CREATE SCHEMA "{schema_name}"', pg_conn)
    run_command(
        f'CREATE TABLE "{schema_name}".t1(a int) USING iceberg',
        pg_conn,
    )
    run_command(
        f'CREATE TABLE "{schema_name}".t2(b text) USING iceberg',
        pg_conn,
    )
    pg_conn.commit()

    try:
        catalog = _create_pyiceberg_catalog()
        tables = catalog.list_tables(schema_name)
        table_names = [t[1] for t in tables]
        assert "t1" in table_names
        assert "t2" in table_names
    finally:
        run_command(f'DROP SCHEMA "{schema_name}" CASCADE', pg_conn)
        pg_conn.commit()


def test_pyiceberg_load_table(
    pg_conn, pglake_rest_catalog, extension, with_default_location, s3
):
    """pyiceberg should be able to load table metadata from our REST catalog."""
    schema_name = "test_pyiceberg_load"
    run_command(f'CREATE SCHEMA "{schema_name}"', pg_conn)
    run_command(
        f'CREATE TABLE "{schema_name}".tbl(a int, b text) USING iceberg',
        pg_conn,
    )
    run_command(
        f"""INSERT INTO "{schema_name}".tbl VALUES (1, 'hello'), (2, 'world')""",
        pg_conn,
    )
    pg_conn.commit()

    try:
        catalog = _create_pyiceberg_catalog()
        table = catalog.load_table(f"{schema_name}.tbl")

        # Verify table metadata
        assert table.metadata.format_version == 2
        schema_fields = {f.name for f in table.schema().fields}
        assert "a" in schema_fields
        assert "b" in schema_fields

        # Verify we can see the current snapshot
        assert table.metadata.current_snapshot_id is not None
    finally:
        run_command(f'DROP SCHEMA "{schema_name}" CASCADE', pg_conn)
        pg_conn.commit()
