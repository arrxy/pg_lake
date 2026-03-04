"""Tests for namespace endpoints."""

from utils_pytest import *
from helpers.pglake_rest_catalog import (
    PGLAKE_REST_CATALOG_PORT,
)


BASE_URL = f"http://localhost:{PGLAKE_REST_CATALOG_PORT}"


def test_list_namespaces_empty(
    rest_catalog_session, extension, with_default_location, s3
):
    """List namespaces should return an empty list when no Iceberg tables exist."""
    import requests

    # Use a catalog name that doesn't exist to get empty results
    resp = rest_catalog_session.get(
        f"{BASE_URL}/v1/nonexistent_catalog/namespaces",
        timeout=2,
    )
    assert resp.status_code == 200
    data = resp.json()
    assert "namespaces" in data
    assert data["namespaces"] == []


def test_list_namespaces_with_tables(
    pg_conn, rest_catalog_session, extension, with_default_location, s3
):
    """List namespaces should include schemas that contain Iceberg tables."""
    import requests

    schema_name = "test_rest_ns_list"
    run_command(f'CREATE SCHEMA "{schema_name}"', pg_conn)
    run_command(
        f'CREATE TABLE "{schema_name}".tbl(a int) USING iceberg',
        pg_conn,
    )
    pg_conn.commit()

    try:
        resp = rest_catalog_session.get(
            f"{BASE_URL}/v1/{server_params.PG_DATABASE}/namespaces",
            timeout=5,
        )
        assert resp.status_code == 200
        data = resp.json()
        ns_list = [ns[0] for ns in data["namespaces"]]
        assert schema_name in ns_list
    finally:
        run_command(f'DROP SCHEMA "{schema_name}" CASCADE', pg_conn)
        pg_conn.commit()


def test_get_namespace_not_found(rest_catalog_session, extension, s3):
    """Getting a non-existent namespace should return 404."""
    import requests

    resp = rest_catalog_session.get(
        f"{BASE_URL}/v1/{server_params.PG_DATABASE}/namespaces/does_not_exist_12345",
        timeout=2,
    )
    assert resp.status_code == 404
    data = resp.json()
    assert data["error"]["type"] == "NoSuchNamespaceException"


def test_get_namespace_exists(
    pg_conn, rest_catalog_session, extension, with_default_location, s3
):
    """Getting an existing namespace should return 200 with properties."""
    import requests

    schema_name = "test_rest_ns_get"
    run_command(f'CREATE SCHEMA "{schema_name}"', pg_conn)
    run_command(
        f'CREATE TABLE "{schema_name}".tbl(a int) USING iceberg',
        pg_conn,
    )
    pg_conn.commit()

    try:
        resp = rest_catalog_session.get(
            f"{BASE_URL}/v1/{server_params.PG_DATABASE}/namespaces/{schema_name}",
            timeout=5,
        )
        assert resp.status_code == 200
        data = resp.json()
        assert data["namespace"] == [schema_name]
        assert "properties" in data
    finally:
        run_command(f'DROP SCHEMA "{schema_name}" CASCADE', pg_conn)
        pg_conn.commit()
