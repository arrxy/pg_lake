# Iceberg REST Catalog Server for pg_lake

## Context

pg_lake currently integrates with external Iceberg REST catalogs (like Apache Polaris) as a **client**, but cannot serve as a REST catalog itself. External tools (Spark, Trino, pyiceberg, DuckDB) that want to read Iceberg tables managed by pg_lake must either access S3 directly or go through a heavyweight Java-based Polaris server.

This plan adds `pglake_rest_catalog` -- a lightweight Go binary that implements the Iceberg REST Catalog API, backed by PostgreSQL as the source of truth. It queries existing `lake_iceberg.*` tables and functions, so all Iceberg metadata management stays in the C extension. The REST server is a thin HTTP/JSON protocol translator.

## Architecture

```
External clients (Spark, Trino, pyiceberg)
        │  HTTPS + OAuth2
        ▼
┌─────────────────────┐
│ pglake_rest_catalog  │  Go binary, port 8181
│ (Iceberg REST API)   │
└────────┬────────────┘
         │  PostgreSQL protocol (libpq)
         ▼
┌─────────────────────┐
│ PostgreSQL           │
│ + pg_lake extensions │  lake_iceberg.tables, .metadata(), .namespace_properties
└─────────────────────┘
```

Key design decision: the REST server calls `lake_iceberg.metadata(metadata_uri)` through PostgreSQL to fetch table metadata from S3. This reuses pg_lake's existing S3 credential configuration and avoids duplicating object storage access in Go.

## Language: Go

Single static binary, no runtime dependencies, installs to `pg_config --bindir` like pgduck_server. Minimal external deps: `pgx/v5` (PostgreSQL driver), `golang-jwt/jwt/v5` (OAuth2 tokens). Everything else uses Go stdlib (`net/http`, `crypto/tls`, `encoding/json`).

## Directory Structure

```
pglake_rest_catalog/
  cmd/pglake-rest-catalog/
    main.go                    # Entry point, signal handling, graceful shutdown
  internal/
    config/config.go           # CLI flags, Config struct
    auth/
      credentials.go           # Client credential storage/validation
      oauth2.go                # JWT token issuance
      middleware.go             # Bearer token validation middleware
    db/
      pool.go                  # pgxpool setup
      queries.go               # SQL query constants
    catalog/
      namespaces.go            # Namespace business logic
      tables.go                # Table business logic
    handlers/
      router.go                # Route registration
      config.go                # GET /v1/config
      oauth.go                 # POST /v1/oauth/tokens
      namespaces.go            # Namespace handlers
      tables.go                # Table handlers
      errors.go                # Iceberg error response format
      middleware.go             # Logging, recovery
    tls/tls.go                 # TLS config loading
  go.mod
  go.sum
  Makefile
  tests/pytests/               # Pytest tests (follows project conventions)
    conftest.py
    test_config.py
    test_oauth.py
    test_namespaces.py
    test_tables.py
    test_pyiceberg.py          # End-to-end with pyiceberg RestCatalog
```

## CLI Flags

```
--port                     HTTP port (default: 8181, matches Polaris)
--postgres_conn_string     PostgreSQL connection (default: "host=/tmp port=5432 dbname=postgres user=postgres")
--client_id                OAuth2 client ID (required)
--client_secret            OAuth2 client secret (required)
--jwt_secret               JWT signing secret (auto-generated if omitted)
--token_expiry_seconds     Token lifetime (default: 3600)
--tls_cert_file            TLS certificate path
--tls_key_file             TLS private key path
--warehouse                Default catalog name (default: "postgres")
--pidfile                  PID file path
--debug                    Verbose logging
```

## REST Endpoints (Read-Only Initial Phase)

### Public (no auth)
| Method | Path | Description |
|--------|------|-------------|
| `GET` | `/v1/config` | Server configuration (warehouse defaults) |
| `POST` | `/v1/oauth/tokens` | OAuth2 token issuance (client_credentials grant) |

### Protected (Bearer token required)
| Method | Path | Description |
|--------|------|-------------|
| `GET` | `/v1/{prefix}/namespaces` | List namespaces |
| `GET` | `/v1/{prefix}/namespaces/{namespace}` | Get namespace + properties |
| `GET` | `/v1/{prefix}/namespaces/{namespace}/tables` | List tables |
| `GET` | `/v1/{prefix}/namespaces/{namespace}/tables/{table}` | Load table (metadata-location + full metadata JSON) |

The `{prefix}` path parameter = catalog name (= PostgreSQL database name for internal tables). This is the standard Iceberg REST spec `warehouse` parameter.

### Future Write Endpoints (design for, don't implement)
- `POST .../namespaces` (create), `DELETE .../namespaces/{ns}` (drop)
- `POST .../tables` (create), `DELETE .../tables/{tbl}` (drop)
- `POST .../tables/{tbl}` (commit update), `POST .../tables/rename`
- `POST .../transactions/commit` (multi-table transaction)

## PostgreSQL Queries

All queries target existing views/functions from `pg_lake_iceberg--3.0.sql`:

**List namespaces** (`lake_iceberg.tables` view + `lake_iceberg.namespace_properties`):
```sql
SELECT DISTINCT table_namespace FROM lake_iceberg.tables WHERE catalog_name = $1
UNION
SELECT DISTINCT namespace FROM lake_iceberg.namespace_properties WHERE catalog_name = $1
ORDER BY 1;
```

**Get namespace properties** (`lake_iceberg.namespace_properties`):
```sql
SELECT property_key, property_value
FROM lake_iceberg.namespace_properties
WHERE catalog_name = $1 AND namespace = $2;
```

**List tables** (`lake_iceberg.tables` view):
```sql
SELECT table_name FROM lake_iceberg.tables
WHERE catalog_name = $1 AND table_namespace = $2
ORDER BY table_name;
```

**Load table** (two-step):
```sql
-- 1. Get metadata location
SELECT metadata_location FROM lake_iceberg.tables
WHERE catalog_name = $1 AND table_namespace = $2 AND table_name = $3;

-- 2. Get full metadata JSON (C function reads from S3, returns JSONB)
SELECT lake_iceberg.metadata($1)::text;
```

The load table response follows the Iceberg REST spec:
```json
{
  "metadata-location": "s3://...",
  "metadata": { /* full Iceberg table metadata */ },
  "config": {}
}
```

## OAuth2 Implementation

Follows the same flow used by the existing pg_lake REST catalog client (`pg_lake_iceberg/src/rest_catalog/rest_catalog.c`) and Polaris:

1. Client sends `POST /v1/oauth/tokens` with `grant_type=client_credentials`
2. Credentials via `Authorization: Basic base64(id:secret)` header (default mode) or form body (horizon mode)
3. Server validates against configured `--client_id`/`--client_secret`
4. Returns signed JWT with `{"access_token": "...", "token_type": "bearer", "expires_in": 3600}`
5. Protected endpoints validate `Authorization: Bearer <jwt>` via signature + expiry check

Single client pair initially; credential store struct supports future expansion to multiple clients.

## SSL/TLS

When `--tls_cert_file` and `--tls_key_file` are provided, server uses `ListenAndServeTLS()` with TLS 1.2 minimum. Otherwise plain HTTP.

## Build Integration

### `pglake_rest_catalog/Makefile`
```makefile
PROGRAM = pglake_rest_catalog
PG_BINDIR := $(shell pg_config --bindir)

all:
	cd cmd/pglake-rest-catalog && go build -o ../../$(PROGRAM) .

install: all
	install $(PROGRAM) $(DESTDIR)$(PG_BINDIR)/$(PROGRAM)

clean:
	rm -f $(PROGRAM)

check:
	PYTHONPATH=../test_common pipenv run pytest -v tests/pytests
```

### Top-level Makefile additions
Add `install-pglake_rest_catalog` and `check-pglake_rest_catalog` targets following the pgduck_server pattern.

## Test Strategy

### New test helper: `test_common/helpers/pglake_rest_catalog.py`
Follows the pattern of `test_common/helpers/polaris.py`:
- `start_pglake_rest_catalog_in_background()` -- starts binary, waits for health check
- `stop_pglake_rest_catalog()` -- stops via PID file
- `get_access_token()` -- OAuth2 token helper
- Pytest fixtures: `pglake_rest_catalog` (session), `rest_catalog_session` (module, with Bearer token)

### Test files
- `test_config.py` -- `/v1/config` returns valid response
- `test_oauth.py` -- valid/invalid credentials, grant types, token expiry
- `test_namespaces.py` -- list/get namespaces, 404 for missing, special characters
- `test_tables.py` -- list/load tables, 404 for missing, metadata JSON validity
- `test_pyiceberg.py` -- end-to-end: create Iceberg table via pg_lake, read it via pyiceberg `RestCatalog` pointed at our server

### Running tests
```bash
make check-pglake_rest_catalog
# or individually:
cd pglake_rest_catalog && PYTHONPATH=../test_common pipenv run pytest -v tests/pytests/test_oauth.py
```

## Implementation Order

### Step 1: Skeleton + build system
- Create directory structure, `go.mod`, `config.go` (CLI parsing), `main.go` (startup, signals, PID file)
- Create `Makefile`, add to top-level Makefile
- Verify: `make install-pglake_rest_catalog && pglake_rest_catalog --help`

### Step 2: HTTP server + config endpoint
- `handlers/router.go`, `handlers/errors.go`, `handlers/middleware.go`
- `handlers/config.go` -- `GET /v1/config`
- Verify: `curl http://localhost:8181/v1/config`

### Step 3: OAuth2
- `auth/credentials.go`, `auth/oauth2.go`, `auth/middleware.go`
- `handlers/oauth.go` -- `POST /v1/oauth/tokens`
- Wire auth middleware into protected routes
- Verify: get token with curl, use it in authenticated requests

### Step 4: PostgreSQL + namespace endpoints
- `db/pool.go`, `db/queries.go`
- `catalog/namespaces.go`, `handlers/namespaces.go`
- Verify: create Iceberg table in PG, curl namespace endpoints

### Step 5: Table endpoints
- `catalog/tables.go`, `handlers/tables.go`
- Verify: curl load-table, confirm full metadata JSON returned

### Step 6: TLS support
- `tls/tls.go`, wire into `main.go`
- Verify with self-signed cert

### Step 7: Pytest test suite
- `test_common/helpers/pglake_rest_catalog.py`
- All test files
- `make check-pglake_rest_catalog`

### Step 8: Docker integration
- Add Go build stage to Dockerfile
- Add `pglake-rest-catalog` service to docker-compose.yml
- Add entrypoint script

## Key Files Reference

| File | Relevance |
|------|-----------|
| `pg_lake_iceberg/pg_lake_iceberg--3.0.sql` | Defines `lake_iceberg.tables`, `.namespace_properties`, `.metadata()` -- the SQL contract |
| `pg_lake_iceberg/include/pg_lake/rest_catalog/rest_catalog.h` | Existing REST client URL patterns, error format, operation types |
| `pg_lake_iceberg/src/rest_catalog/rest_catalog.c` | OAuth2 flow, HTTP headers, error parsing the server must be compatible with |
| `test_common/helpers/polaris.py` | Server lifecycle pattern to follow for test helper |
| `test_common/helpers/server_params.py` | Port/hostname constants pattern |
| `Makefile` | Top-level build orchestration to extend |
| `pgduck_server/Makefile` | Reference for standalone binary build/install pattern |

## Verification

After full implementation:
1. `make install-pglake_rest_catalog` -- binary installs to `pg_config --bindir`
2. Start server: `pglake_rest_catalog --port 8181 --client_id test --client_secret secret --warehouse postgres`
3. Health check: `curl http://localhost:8181/v1/config`
4. Get token: `curl -u test:secret -d grant_type=client_credentials http://localhost:8181/v1/oauth/tokens`
5. List namespaces: `curl -H "Authorization: Bearer <token>" http://localhost:8181/v1/postgres/namespaces`
6. Load table: `curl -H "Authorization: Bearer <token>" http://localhost:8181/v1/postgres/namespaces/public/tables/my_table`
7. pyiceberg integration: `RestCatalog("test", uri="http://localhost:8181/v1", warehouse="postgres", credential="test:secret")`
8. `make check-pglake_rest_catalog` -- all pytest tests pass
