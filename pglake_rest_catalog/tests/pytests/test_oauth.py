"""Tests for POST /v1/oauth/tokens endpoint."""

import requests

from helpers.pglake_rest_catalog import (
    PGLAKE_REST_CATALOG_PORT,
    PGLAKE_REST_CATALOG_CLIENT_ID,
    PGLAKE_REST_CATALOG_CLIENT_SECRET,
    get_rest_catalog_access_token,
)


BASE_URL = f"http://localhost:{PGLAKE_REST_CATALOG_PORT}"


def test_oauth_valid_credentials_basic_auth(pglake_rest_catalog):
    """Valid client_credentials grant with Basic auth should return a token."""
    resp = requests.post(
        f"{BASE_URL}/v1/oauth/tokens",
        data={"grant_type": "client_credentials", "scope": "PRINCIPAL_ROLE:ALL"},
        auth=(PGLAKE_REST_CATALOG_CLIENT_ID, PGLAKE_REST_CATALOG_CLIENT_SECRET),
        timeout=2,
    )
    assert resp.status_code == 200
    data = resp.json()
    assert "access_token" in data
    assert data["token_type"] == "bearer"
    assert data["expires_in"] > 0


def test_oauth_valid_credentials_form_body(pglake_rest_catalog):
    """Valid client_credentials grant with credentials in form body."""
    resp = requests.post(
        f"{BASE_URL}/v1/oauth/tokens",
        data={
            "grant_type": "client_credentials",
            "client_id": PGLAKE_REST_CATALOG_CLIENT_ID,
            "client_secret": PGLAKE_REST_CATALOG_CLIENT_SECRET,
            "scope": "PRINCIPAL_ROLE:ALL",
        },
        timeout=2,
    )
    assert resp.status_code == 200
    data = resp.json()
    assert "access_token" in data
    assert data["token_type"] == "bearer"


def test_oauth_invalid_credentials(pglake_rest_catalog):
    """Invalid credentials should return 401."""
    resp = requests.post(
        f"{BASE_URL}/v1/oauth/tokens",
        data={"grant_type": "client_credentials"},
        auth=("wrong_id", "wrong_secret"),
        timeout=2,
    )
    assert resp.status_code == 401


def test_oauth_missing_grant_type(pglake_rest_catalog):
    """Missing grant_type should return 400."""
    resp = requests.post(
        f"{BASE_URL}/v1/oauth/tokens",
        data={},
        auth=(PGLAKE_REST_CATALOG_CLIENT_ID, PGLAKE_REST_CATALOG_CLIENT_SECRET),
        timeout=2,
    )
    assert resp.status_code == 400


def test_oauth_unsupported_grant_type(pglake_rest_catalog):
    """Unsupported grant_type should return 400."""
    resp = requests.post(
        f"{BASE_URL}/v1/oauth/tokens",
        data={"grant_type": "authorization_code"},
        auth=(PGLAKE_REST_CATALOG_CLIENT_ID, PGLAKE_REST_CATALOG_CLIENT_SECRET),
        timeout=2,
    )
    assert resp.status_code == 400


def test_oauth_token_is_valid_bearer(pglake_rest_catalog):
    """Issued token should work as a Bearer token on protected endpoints."""
    token = get_rest_catalog_access_token()

    resp = requests.get(
        f"{BASE_URL}/v1/{PGLAKE_REST_CATALOG_PORT}/namespaces",
        headers={"Authorization": f"Bearer {token}"},
        timeout=2,
    )
    # Should not be 401 (may be 404 or 500 if no DB, but auth should pass)
    assert resp.status_code != 401


def test_oauth_no_auth_required_for_tokens_endpoint(pglake_rest_catalog):
    """The /v1/oauth/tokens endpoint itself should not require a Bearer token."""
    resp = requests.post(
        f"{BASE_URL}/v1/oauth/tokens",
        data={"grant_type": "client_credentials"},
        auth=(PGLAKE_REST_CATALOG_CLIENT_ID, PGLAKE_REST_CATALOG_CLIENT_SECRET),
        timeout=2,
    )
    assert resp.status_code == 200


def test_protected_endpoint_without_token(pglake_rest_catalog):
    """Protected endpoints should return 401 without a token."""
    resp = requests.get(
        f"{BASE_URL}/v1/postgres/namespaces",
        timeout=2,
    )
    assert resp.status_code == 401


def test_protected_endpoint_with_invalid_token(pglake_rest_catalog):
    """Protected endpoints should return 401 with an invalid token."""
    resp = requests.get(
        f"{BASE_URL}/v1/postgres/namespaces",
        headers={"Authorization": "Bearer invalid_token_here"},
        timeout=2,
    )
    assert resp.status_code == 401
