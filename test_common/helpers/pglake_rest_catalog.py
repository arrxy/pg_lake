"""pg_lake REST Catalog server helper functions and fixtures."""

import atexit
import os
import subprocess
import time

import pytest
import requests
from pathlib import Path

from . import server_params
from .server import stop_process_via_pidfile

PGLAKE_REST_CATALOG_PORT = 8282
PGLAKE_REST_CATALOG_PID_FILE = "/tmp/regress-pglake-rest-catalog.pid"
PGLAKE_REST_CATALOG_CLIENT_ID = "test_client_id"
PGLAKE_REST_CATALOG_CLIENT_SECRET = "test_client_secret"


# ---------------------------------------------------------------------------
# Server management
# ---------------------------------------------------------------------------


def get_pglake_rest_catalog_path():
    """Find the pglake_rest_catalog binary via pg_config --bindir."""
    bindir = (
        subprocess.run(
            ["pg_config", "--bindir"], capture_output=True, text=True, check=True
        )
        .stdout.strip()
    )
    path = Path(bindir) / "pglake_rest_catalog"
    if path.exists():
        return path

    # Fallback: check PATH
    result = subprocess.run(
        ["which", "pglake_rest_catalog"], capture_output=True, text=True
    )
    if result.returncode == 0:
        return Path(result.stdout.strip())

    raise FileNotFoundError(
        "pglake_rest_catalog not found in pg_config --bindir or PATH"
    )


def start_pglake_rest_catalog_in_background(
    port=PGLAKE_REST_CATALOG_PORT,
    client_id=PGLAKE_REST_CATALOG_CLIENT_ID,
    client_secret=PGLAKE_REST_CATALOG_CLIENT_SECRET,
):
    """Start the pglake_rest_catalog server as a background process."""
    stop_pglake_rest_catalog()

    binary = get_pglake_rest_catalog_path()

    conn_string = (
        f"host={server_params.PG_HOST} port={server_params.PG_PORT} "
        f"dbname={server_params.PG_DATABASE} user={server_params.PG_USER} "
        f"password={server_params.PG_PASSWORD}"
    )

    cmd = [
        str(binary),
        "-port",
        str(port),
        "-postgres_conn_string",
        conn_string,
        "-client_id",
        client_id,
        "-client_secret",
        client_secret,
        "-warehouse",
        server_params.PG_DATABASE,
        "-pidfile",
        PGLAKE_REST_CATALOG_PID_FILE,
        "-debug",
    ]

    log_path = Path("/tmp/pglake_rest_catalog.log")
    log_file = log_path.open("w+", buffering=1)

    atexit.register(stop_pglake_rest_catalog)

    proc = subprocess.Popen(
        cmd,
        stdout=log_file,
        stderr=subprocess.STDOUT,
        start_new_session=True,
    )

    wait_for_pglake_rest_catalog(port=port)
    return proc


def wait_for_pglake_rest_catalog(port=PGLAKE_REST_CATALOG_PORT, timeout=10):
    """Wait for the REST catalog server to be healthy."""
    url = f"http://localhost:{port}/v1/config"
    for _ in range(timeout * 10):
        try:
            resp = requests.get(url, timeout=1)
            if resp.ok:
                return
        except requests.ConnectionError:
            pass
        time.sleep(0.1)
    raise RuntimeError("pglake_rest_catalog did not become healthy in time")


def stop_pglake_rest_catalog(timeout=10):
    """Stop the pglake_rest_catalog server using its PID file."""
    stop_process_via_pidfile(PGLAKE_REST_CATALOG_PID_FILE, timeout)


# ---------------------------------------------------------------------------
# Token helpers
# ---------------------------------------------------------------------------


def get_rest_catalog_access_token(
    port=PGLAKE_REST_CATALOG_PORT,
    client_id=PGLAKE_REST_CATALOG_CLIENT_ID,
    client_secret=PGLAKE_REST_CATALOG_CLIENT_SECRET,
):
    """Get an OAuth2 access token from the REST catalog server."""
    url = f"http://localhost:{port}/v1/oauth/tokens"
    resp = requests.post(
        url,
        data={"grant_type": "client_credentials", "scope": "PRINCIPAL_ROLE:ALL"},
        auth=(client_id, client_secret),
        timeout=2,
    )
    resp.raise_for_status()
    return resp.json()["access_token"]


# ---------------------------------------------------------------------------
# Pytest fixtures
# ---------------------------------------------------------------------------

_rest_catalog_started = False


@pytest.fixture(scope="session")
def pglake_rest_catalog(postgres):
    """Session fixture that starts the pg_lake REST catalog server."""
    global _rest_catalog_started
    if _rest_catalog_started:
        yield
        return
    start_pglake_rest_catalog_in_background()
    _rest_catalog_started = True
    yield
    stop_pglake_rest_catalog()


@pytest.fixture(scope="module")
def rest_catalog_session(pglake_rest_catalog):
    """Ready-to-use requests.Session with Bearer token."""
    token = get_rest_catalog_access_token()
    sess = requests.Session()
    sess.headers.update({"Authorization": f"Bearer {token}"})
    return sess
