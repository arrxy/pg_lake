"""Tests for GET /v1/config endpoint."""

from helpers.pglake_rest_catalog import (
    PGLAKE_REST_CATALOG_PORT,
)
from helpers import server_params


def test_config_returns_ok(pglake_rest_catalog):
    """Config endpoint should return 200 with JSON."""
    import requests

    url = f"http://localhost:{PGLAKE_REST_CATALOG_PORT}/v1/config"
    resp = requests.get(url, timeout=2)
    assert resp.status_code == 200
    data = resp.json()
    assert "defaults" in data
    assert "overrides" in data


def test_config_returns_warehouse_as_prefix(pglake_rest_catalog):
    """Config should return the warehouse as the prefix override."""
    import requests

    url = f"http://localhost:{PGLAKE_REST_CATALOG_PORT}/v1/config"
    resp = requests.get(url, timeout=2)
    data = resp.json()
    assert data["overrides"]["prefix"] == server_params.PG_DATABASE


def test_config_warehouse_query_param(pglake_rest_catalog):
    """Config should respect the warehouse query parameter."""
    import requests

    url = f"http://localhost:{PGLAKE_REST_CATALOG_PORT}/v1/config?warehouse=custom_wh"
    resp = requests.get(url, timeout=2)
    data = resp.json()
    assert data["overrides"]["prefix"] == "custom_wh"


def test_config_no_auth_required(pglake_rest_catalog):
    """Config endpoint should not require authentication."""
    import requests

    url = f"http://localhost:{PGLAKE_REST_CATALOG_PORT}/v1/config"
    # No Authorization header
    resp = requests.get(url, timeout=2)
    assert resp.status_code == 200
