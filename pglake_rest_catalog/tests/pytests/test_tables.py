"""Tests for table endpoints."""

from utils_pytest import *
from helpers.pglake_rest_catalog import (
    PGLAKE_REST_CATALOG_PORT,
)


BASE_URL = f"http://localhost:{PGLAKE_REST_CATALOG_PORT}"


def test_list_tables_empty(rest_catalog_session, extension, s3):
    """List tables in an empty namespace should return an empty list."""
    import requests

    resp = rest_catalog_session.get(
        f"{BASE_URL}/v1/{server_params.PG_DATABASE}/namespaces/nonexistent_ns/tables",
        timeout=2,
    )
    assert resp.status_code == 200
    data = resp.json()
    assert "identifiers" in data
    assert data["identifiers"] == []


def test_list_tables_with_iceberg_tables(
    pg_conn, rest_catalog_session, extension, with_default_location, s3
):
    """List tables should include Iceberg tables in the namespace."""
    import requests

    schema_name = "test_rest_tbl_list"
    run_command(f'CREATE SCHEMA "{schema_name}"', pg_conn)
    run_command(
        f'CREATE TABLE "{schema_name}".tbl_a(a int) USING iceberg',
        pg_conn,
    )
    run_command(
        f'CREATE TABLE "{schema_name}".tbl_b(b text) USING iceberg',
        pg_conn,
    )
    pg_conn.commit()

    try:
        resp = rest_catalog_session.get(
            f"{BASE_URL}/v1/{server_params.PG_DATABASE}/namespaces/{schema_name}/tables",
            timeout=5,
        )
        assert resp.status_code == 200
        data = resp.json()
        table_names = [t["name"] for t in data["identifiers"]]
        assert "tbl_a" in table_names
        assert "tbl_b" in table_names
        for t in data["identifiers"]:
            assert t["namespace"] == [schema_name]
    finally:
        run_command(f'DROP SCHEMA "{schema_name}" CASCADE', pg_conn)
        pg_conn.commit()


def test_load_table_not_found(rest_catalog_session, extension, s3):
    """Loading a non-existent table should return 404."""
    import requests

    resp = rest_catalog_session.get(
        f"{BASE_URL}/v1/{server_params.PG_DATABASE}/namespaces/public/tables/nonexistent_table_12345",
        timeout=2,
    )
    assert resp.status_code == 404
    data = resp.json()
    assert data["error"]["type"] == "NoSuchTableException"


def test_load_table_returns_metadata(
    pg_conn, rest_catalog_session, extension, with_default_location, s3
):
    """Loading an existing Iceberg table should return metadata-location and full metadata JSON."""
    import requests

    schema_name = "test_rest_tbl_load"
    run_command(f'CREATE SCHEMA "{schema_name}"', pg_conn)
    run_command(
        f'CREATE TABLE "{schema_name}".tbl(a int, b text) USING iceberg',
        pg_conn,
    )
    # Insert data to create a snapshot
    run_command(
        f"""INSERT INTO "{schema_name}".tbl VALUES (1, 'hello')""",
        pg_conn,
    )
    pg_conn.commit()

    try:
        resp = rest_catalog_session.get(
            f"{BASE_URL}/v1/{server_params.PG_DATABASE}/namespaces/{schema_name}/tables/tbl",
            timeout=10,
        )
        assert resp.status_code == 200
        data = resp.json()

        # Verify response structure per Iceberg REST spec
        assert "metadata-location" in data
        assert "metadata" in data
        assert "config" in data

        # Metadata location should be an S3 path
        assert data["metadata-location"].startswith("s3://")

        # Metadata should be valid Iceberg v2 format
        metadata = data["metadata"]
        assert metadata["format-version"] == 2
        assert "table-uuid" in metadata
        assert "schemas" in metadata
        assert "snapshots" in metadata
    finally:
        run_command(f'DROP SCHEMA "{schema_name}" CASCADE', pg_conn)
        pg_conn.commit()
