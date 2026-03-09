import os
import pytest
from utils_pytest import *


# ── FDW and pre-created servers ────────────────────────────────────────────


def test_iceberg_catalog_fdw_exists(pg_conn, extension):
    """The iceberg_catalog FDW should be created by the extension."""
    result = run_query(
        "SELECT fdwname FROM pg_foreign_data_wrapper WHERE fdwname = 'iceberg_catalog'",
        pg_conn,
    )
    assert len(result) == 1
    assert result[0]["fdwname"] == "iceberg_catalog"


def test_iceberg_catalog_fdw_has_no_handler(pg_conn, extension):
    """iceberg_catalog is configuration-only, so it should have no handler."""
    result = run_query(
        "SELECT fdwhandler FROM pg_foreign_data_wrapper WHERE fdwname = 'iceberg_catalog'",
        pg_conn,
    )
    assert result[0]["fdwhandler"] == 0


def test_precreated_postgres_server(pg_conn, extension):
    """A 'postgres' server of TYPE 'postgres' should be pre-created."""
    result = run_query(
        "SELECT srvname, srvtype FROM pg_foreign_server WHERE srvname = 'postgres'",
        pg_conn,
    )
    assert len(result) == 1
    assert result[0]["srvname"] == "postgres"
    assert result[0]["srvtype"] == "postgres"


def test_precreated_object_store_server(pg_conn, extension):
    """An 'object_store' server of TYPE 'object_store' should be pre-created."""
    result = run_query(
        "SELECT srvname, srvtype FROM pg_foreign_server WHERE srvname = 'object_store'",
        pg_conn,
    )
    assert len(result) == 1
    assert result[0]["srvname"] == "object_store"
    assert result[0]["srvtype"] == "object_store"


# ── CREATE SERVER with valid options ───────────────────────────────────────


def test_create_rest_server_with_all_options(superuser_conn, extension):
    """All documented non-secret options should be accepted for a REST-type server."""
    run_command(
        """
        CREATE SERVER test_rest_all_opts TYPE 'rest'
            FOREIGN DATA WRAPPER iceberg_catalog
            OPTIONS (
                rest_endpoint 'http://localhost:8181',
                rest_auth_type 'default',
                oauth_endpoint 'http://localhost:8181/oauth/tokens',
                scope 'PRINCIPAL_ROLE:ALL',
                enable_vended_credentials 'true',
                location_prefix 's3://bucket/prefix',
                catalog_name 'my_catalog'
            )
        """,
        superuser_conn,
    )
    superuser_conn.rollback()


def test_create_rest_server_minimal(superuser_conn, extension):
    """A server with just rest_endpoint should be accepted."""
    run_command(
        """
        CREATE SERVER test_rest_minimal TYPE 'rest'
            FOREIGN DATA WRAPPER iceberg_catalog
            OPTIONS (rest_endpoint 'http://localhost:8181')
        """,
        superuser_conn,
    )
    superuser_conn.rollback()


def test_create_server_without_type(superuser_conn, extension):
    """A server without TYPE should be accepted (defaults to rest)."""
    run_command(
        """
        CREATE SERVER test_no_type
            FOREIGN DATA WRAPPER iceberg_catalog
            OPTIONS (rest_endpoint 'http://localhost:8181')
        """,
        superuser_conn,
    )
    superuser_conn.rollback()


def test_create_server_horizon_auth(superuser_conn, extension):
    """Horizon auth type should be accepted."""
    run_command(
        """
        CREATE SERVER test_horizon TYPE 'rest'
            FOREIGN DATA WRAPPER iceberg_catalog
            OPTIONS (
                rest_endpoint 'https://horizon.example.com',
                rest_auth_type 'horizon'
            )
        """,
        superuser_conn,
    )
    superuser_conn.rollback()


# ── CREATE SERVER with invalid options ─────────────────────────────────────


def test_reject_unknown_server_option(superuser_conn, extension):
    """Unknown options should be rejected by the validator."""
    err = run_command(
        """
        CREATE SERVER test_bad_opt TYPE 'rest'
            FOREIGN DATA WRAPPER iceberg_catalog
            OPTIONS (rest_endpoint 'http://localhost:8181', bogus_option 'x')
        """,
        superuser_conn,
        raise_error=False,
    )
    assert "invalid option" in str(err)
    assert "bogus_option" in str(err)
    superuser_conn.rollback()


def test_reject_invalid_auth_type(superuser_conn, extension):
    """Only 'default' and 'horizon' are valid for rest_auth_type."""
    err = run_command(
        """
        CREATE SERVER test_bad_auth TYPE 'rest'
            FOREIGN DATA WRAPPER iceberg_catalog
            OPTIONS (rest_endpoint 'http://localhost:8181', rest_auth_type 'oauth2')
        """,
        superuser_conn,
        raise_error=False,
    )
    assert "invalid rest_auth_type" in str(err)
    superuser_conn.rollback()


def test_reject_invalid_vended_creds(superuser_conn, extension):
    """enable_vended_credentials must be a valid boolean."""
    err = run_command(
        """
        CREATE SERVER test_bad_bool TYPE 'rest'
            FOREIGN DATA WRAPPER iceberg_catalog
            OPTIONS (rest_endpoint 'http://localhost:8181', enable_vended_credentials 'maybe')
        """,
        superuser_conn,
        raise_error=False,
    )
    assert err is not None
    superuser_conn.rollback()


def test_reject_options_on_non_server(superuser_conn, extension):
    """Options on the FDW itself should be rejected."""
    err = run_command(
        """
        ALTER FOREIGN DATA WRAPPER iceberg_catalog OPTIONS (ADD rest_endpoint 'http://x')
        """,
        superuser_conn,
        raise_error=False,
    )
    assert "only valid for SERVER" in str(err)
    superuser_conn.rollback()


def test_reject_secrets_in_server_options(superuser_conn, extension):
    """client_id and client_secret are not valid server options (they belong to user mappings)."""
    err = run_command(
        """
        CREATE SERVER test_secrets_srv TYPE 'rest'
            FOREIGN DATA WRAPPER iceberg_catalog
            OPTIONS (rest_endpoint 'http://localhost:8181', client_id 'id')
        """,
        superuser_conn,
        raise_error=False,
    )
    assert "invalid option" in str(err)
    assert "client_id" in str(err)
    superuser_conn.rollback()


# ── CREATE USER MAPPING ───────────────────────────────────────────────────


def test_create_user_mapping_valid(superuser_conn, extension):
    """client_id, client_secret, and scope should be accepted as user mapping options."""
    run_command(
        """
        CREATE SERVER test_um_srv TYPE 'rest'
            FOREIGN DATA WRAPPER iceberg_catalog
            OPTIONS (rest_endpoint 'http://localhost:8181')
        """,
        superuser_conn,
    )
    run_command(
        """
        CREATE USER MAPPING FOR CURRENT_USER SERVER test_um_srv
            OPTIONS (client_id 'my-id', client_secret 'my-secret', scope 'PRINCIPAL_ROLE:ADMIN')
        """,
        superuser_conn,
    )
    superuser_conn.rollback()


def test_create_user_mapping_public(superuser_conn, extension):
    """A PUBLIC user mapping should be accepted."""
    run_command(
        """
        CREATE SERVER test_um_pub_srv TYPE 'rest'
            FOREIGN DATA WRAPPER iceberg_catalog
            OPTIONS (rest_endpoint 'http://localhost:8181')
        """,
        superuser_conn,
    )
    run_command(
        """
        CREATE USER MAPPING FOR PUBLIC SERVER test_um_pub_srv
            OPTIONS (client_id 'pub-id', client_secret 'pub-secret')
        """,
        superuser_conn,
    )
    superuser_conn.rollback()


def test_reject_unknown_user_mapping_option(superuser_conn, extension):
    """Unknown options in user mapping should be rejected."""
    run_command(
        """
        CREATE SERVER test_um_bad_srv TYPE 'rest'
            FOREIGN DATA WRAPPER iceberg_catalog
            OPTIONS (rest_endpoint 'http://localhost:8181')
        """,
        superuser_conn,
    )
    err = run_command(
        """
        CREATE USER MAPPING FOR CURRENT_USER SERVER test_um_bad_srv
            OPTIONS (client_id 'id', unknown_opt 'x')
        """,
        superuser_conn,
        raise_error=False,
    )
    assert "invalid option" in str(err)
    assert "unknown_opt" in str(err)
    superuser_conn.rollback()


def test_reject_server_options_in_user_mapping(superuser_conn, extension):
    """Server-level options like rest_endpoint should not be accepted in user mapping."""
    run_command(
        """
        CREATE SERVER test_um_wrong_srv TYPE 'rest'
            FOREIGN DATA WRAPPER iceberg_catalog
            OPTIONS (rest_endpoint 'http://localhost:8181')
        """,
        superuser_conn,
    )
    err = run_command(
        """
        CREATE USER MAPPING FOR CURRENT_USER SERVER test_um_wrong_srv
            OPTIONS (rest_endpoint 'http://other:8181')
        """,
        superuser_conn,
        raise_error=False,
    )
    assert "invalid option" in str(err)
    superuser_conn.rollback()


# ── Creating foreign tables on iceberg_catalog should fail ─────────────────


def test_cannot_query_foreign_table_on_catalog_server(superuser_conn, extension):
    """iceberg_catalog has no handler, so querying a foreign table should fail.

    PostgreSQL allows CREATE FOREIGN TABLE on a handler-less FDW; the error
    only surfaces at query time when GetFdwRoutineByServerId() is called.
    """
    run_command(
        """
        CREATE SERVER test_ft_server TYPE 'rest'
            FOREIGN DATA WRAPPER iceberg_catalog
            OPTIONS (rest_endpoint 'http://localhost:8181')
        """,
        superuser_conn,
    )

    run_command(
        """
        CREATE FOREIGN TABLE test_ft_table (id int)
            SERVER test_ft_server
        """,
        superuser_conn,
    )

    err = run_command(
        "SELECT * FROM test_ft_table",
        superuser_conn,
        raise_error=False,
    )
    assert "has no handler" in str(err)
    superuser_conn.rollback()


# ── ALTER SERVER ───────────────────────────────────────────────────────────


def test_alter_server_add_option(superuser_conn, extension):
    """ALTER SERVER should allow adding new server options."""
    run_command(
        """
        CREATE SERVER test_alter TYPE 'rest'
            FOREIGN DATA WRAPPER iceberg_catalog
            OPTIONS (rest_endpoint 'http://localhost:8181')
        """,
        superuser_conn,
    )

    run_command(
        """
        ALTER SERVER test_alter OPTIONS (ADD scope 'PRINCIPAL_ROLE:ADMIN')
        """,
        superuser_conn,
    )

    result = run_query(
        """
        SELECT srvoptions FROM pg_foreign_server WHERE srvname = 'test_alter'
        """,
        superuser_conn,
    )
    opts = result[0]["srvoptions"]
    assert "scope=PRINCIPAL_ROLE:ADMIN" in opts
    superuser_conn.rollback()


def test_alter_server_set_option(superuser_conn, extension):
    """ALTER SERVER should allow changing existing options."""
    run_command(
        """
        CREATE SERVER test_alter_set TYPE 'rest'
            FOREIGN DATA WRAPPER iceberg_catalog
            OPTIONS (rest_endpoint 'http://localhost:8181')
        """,
        superuser_conn,
    )

    run_command(
        """
        ALTER SERVER test_alter_set OPTIONS (SET rest_endpoint 'http://new-host:8181')
        """,
        superuser_conn,
    )

    result = run_query(
        """
        SELECT srvoptions FROM pg_foreign_server WHERE srvname = 'test_alter_set'
        """,
        superuser_conn,
    )
    opts = result[0]["srvoptions"]
    assert "rest_endpoint=http://new-host:8181" in opts
    superuser_conn.rollback()


def test_alter_server_reject_unknown_option(superuser_conn, extension):
    """ALTER SERVER should reject unknown options."""
    run_command(
        """
        CREATE SERVER test_alter_bad TYPE 'rest'
            FOREIGN DATA WRAPPER iceberg_catalog
            OPTIONS (rest_endpoint 'http://localhost:8181')
        """,
        superuser_conn,
    )

    err = run_command(
        """
        ALTER SERVER test_alter_bad OPTIONS (ADD unknown_opt 'x')
        """,
        superuser_conn,
        raise_error=False,
    )
    assert "invalid option" in str(err)
    superuser_conn.rollback()


# ── DROP SERVER ────────────────────────────────────────────────────────────


def test_drop_server(superuser_conn, extension):
    """DROP SERVER should work for iceberg_catalog servers."""
    run_command(
        """
        CREATE SERVER test_drop_srv TYPE 'rest'
            FOREIGN DATA WRAPPER iceberg_catalog
            OPTIONS (rest_endpoint 'http://localhost:8181')
        """,
        superuser_conn,
    )

    run_command("DROP SERVER test_drop_srv", superuser_conn)

    result = run_query(
        "SELECT count(*) FROM pg_foreign_server WHERE srvname = 'test_drop_srv'",
        superuser_conn,
    )
    assert result[0]["count"] == 0
    superuser_conn.rollback()


# ── Using a server-based catalog in CREATE TABLE ───────────────────────────


def test_create_table_with_server_catalog(
    pg_conn, superuser_conn, s3, extension, with_default_location
):
    """CREATE TABLE ... USING iceberg WITH (catalog = '<server_name>') should
    recognize the catalog option as a server-based REST catalog."""
    run_command(
        """
        CREATE SERVER test_srv_catalog TYPE 'rest'
            FOREIGN DATA WRAPPER iceberg_catalog
            OPTIONS (rest_endpoint 'http://localhost:8181')
        """,
        superuser_conn,
    )
    run_command(
        """
        CREATE USER MAPPING FOR CURRENT_USER SERVER test_srv_catalog
            OPTIONS (client_id 'id', client_secret 'secret')
        """,
        superuser_conn,
    )
    superuser_conn.commit()

    err = run_command(
        """
        CREATE TABLE test_srv_tbl ()
            USING iceberg
            WITH (catalog = 'test_srv_catalog', read_only = 'true',
                  catalog_namespace = 'ns', catalog_table_name = 'tbl')
        """,
        pg_conn,
        raise_error=False,
    )
    # The REST endpoint is fake, so we expect a connection error, NOT a
    # "invalid catalog option" error. This proves the server was resolved.
    assert err is not None
    assert "invalid catalog option" not in str(err)
    pg_conn.rollback()

    run_command(
        "DROP USER MAPPING FOR CURRENT_USER SERVER test_srv_catalog", superuser_conn
    )
    run_command("DROP SERVER test_srv_catalog CASCADE", superuser_conn)
    superuser_conn.commit()


def test_invalid_catalog_name_errors(pg_conn, s3, extension, with_default_location):
    """A catalog name that is neither a known literal nor a valid server should error."""
    err = run_command(
        """
        CREATE TABLE test_bad_cat ()
            USING iceberg
            WITH (catalog = 'nonexistent_server', read_only = 'true')
        """,
        pg_conn,
        raise_error=False,
    )
    assert "invalid catalog option" in str(err)
    pg_conn.rollback()


def test_non_iceberg_catalog_server_rejected(
    pg_conn, superuser_conn, s3, extension, with_default_location
):
    """A foreign server not under iceberg_catalog FDW should not be accepted
    as a catalog value."""
    err = run_command(
        """
        CREATE TABLE test_wrong_fdw ()
            USING iceberg
            WITH (catalog = 'pg_lake_iceberg', read_only = 'true')
        """,
        pg_conn,
        raise_error=False,
    )
    assert "invalid catalog option" in str(err)
    pg_conn.rollback()


def test_server_without_type_treated_as_rest(
    pg_conn, superuser_conn, s3, extension, with_default_location
):
    """A server without explicit TYPE should default to rest catalog behavior."""
    run_command(
        """
        CREATE SERVER test_no_type_srv
            FOREIGN DATA WRAPPER iceberg_catalog
            OPTIONS (rest_endpoint 'http://localhost:8181')
        """,
        superuser_conn,
    )
    run_command(
        """
        CREATE USER MAPPING FOR CURRENT_USER SERVER test_no_type_srv
            OPTIONS (client_id 'id', client_secret 'secret')
        """,
        superuser_conn,
    )
    superuser_conn.commit()

    err = run_command(
        """
        CREATE TABLE test_no_type_tbl ()
            USING iceberg
            WITH (catalog = 'test_no_type_srv', read_only = 'true',
                  catalog_namespace = 'ns', catalog_table_name = 'tbl')
        """,
        pg_conn,
        raise_error=False,
    )
    # Should be treated as REST, not rejected as invalid catalog
    assert "invalid catalog option" not in str(err)
    pg_conn.rollback()

    run_command(
        "DROP USER MAPPING FOR CURRENT_USER SERVER test_no_type_srv", superuser_conn
    )
    run_command("DROP SERVER test_no_type_srv CASCADE", superuser_conn)
    superuser_conn.commit()


# ── Credential resolution: catalogs.conf ──────────────────────────────────


@pytest.fixture
def pg_data_dir(superuser_conn):
    """Returns the PGDATA directory path."""
    result = run_query("SHOW data_directory", superuser_conn)
    return result[0]["data_directory"]


@pytest.fixture
def catalogs_conf(pg_data_dir):
    """Write a temporary catalogs.conf in $PGDATA and clean up after."""
    conf_path = os.path.join(pg_data_dir, "catalogs.conf")
    created = not os.path.exists(conf_path)
    original_content = None
    if not created:
        with open(conf_path, "r") as f:
            original_content = f.read()

    def _write(content):
        with open(conf_path, "w") as f:
            f.write(content)

    yield _write

    if created:
        if os.path.exists(conf_path):
            os.remove(conf_path)
    else:
        with open(conf_path, "w") as f:
            f.write(original_content or "")


def test_credentials_from_catalogs_conf(
    pg_conn,
    superuser_conn,
    s3,
    extension,
    with_default_location,
    catalogs_conf,
):
    """Credentials should be resolved from $PGDATA/catalogs.conf when no
    user mapping exists."""
    catalogs_conf(
        "test_conf_srv.client_id = 'conf-id'\n"
        "test_conf_srv.client_secret = 'conf-secret'\n"
    )
    run_command(
        """
        CREATE SERVER test_conf_srv TYPE 'rest'
            FOREIGN DATA WRAPPER iceberg_catalog
            OPTIONS (rest_endpoint 'http://localhost:8181')
        """,
        superuser_conn,
    )
    superuser_conn.commit()

    err = run_command(
        """
        CREATE TABLE test_conf_tbl ()
            USING iceberg
            WITH (catalog = 'test_conf_srv', read_only = 'true',
                  catalog_namespace = 'ns', catalog_table_name = 'tbl')
        """,
        pg_conn,
        raise_error=False,
    )
    # Should get a connection error (fake endpoint), not a credentials error
    assert err is not None
    assert "no credentials found" not in str(err)
    assert "invalid catalog option" not in str(err)
    pg_conn.rollback()

    run_command("DROP SERVER test_conf_srv CASCADE", superuser_conn)
    superuser_conn.commit()


def test_user_mapping_overrides_catalogs_conf(
    pg_conn,
    superuser_conn,
    s3,
    extension,
    with_default_location,
    catalogs_conf,
):
    """User mapping credentials should take priority over catalogs.conf."""
    catalogs_conf(
        "test_override_srv.client_id = 'conf-id'\n"
        "test_override_srv.client_secret = 'conf-secret'\n"
    )
    run_command(
        """
        CREATE SERVER test_override_srv TYPE 'rest'
            FOREIGN DATA WRAPPER iceberg_catalog
            OPTIONS (rest_endpoint 'http://localhost:8181')
        """,
        superuser_conn,
    )
    run_command(
        """
        CREATE USER MAPPING FOR CURRENT_USER SERVER test_override_srv
            OPTIONS (client_id 'um-id', client_secret 'um-secret')
        """,
        superuser_conn,
    )
    superuser_conn.commit()

    err = run_command(
        """
        CREATE TABLE test_override_tbl ()
            USING iceberg
            WITH (catalog = 'test_override_srv', read_only = 'true',
                  catalog_namespace = 'ns', catalog_table_name = 'tbl')
        """,
        pg_conn,
        raise_error=False,
    )
    assert err is not None
    assert "no credentials found" not in str(err)
    assert "invalid catalog option" not in str(err)
    pg_conn.rollback()

    run_command(
        "DROP USER MAPPING FOR CURRENT_USER SERVER test_override_srv", superuser_conn
    )
    run_command("DROP SERVER test_override_srv CASCADE", superuser_conn)
    superuser_conn.commit()


def test_no_credentials_errors(
    pg_conn,
    superuser_conn,
    s3,
    extension,
    with_default_location,
):
    """Without user mapping, catalogs.conf, or GUCs, credential resolution should error."""
    run_command(
        """
        CREATE SERVER test_no_creds TYPE 'rest'
            FOREIGN DATA WRAPPER iceberg_catalog
            OPTIONS (rest_endpoint 'http://localhost:8181')
        """,
        superuser_conn,
    )
    superuser_conn.commit()

    err = run_command(
        """
        CREATE TABLE test_no_creds_tbl ()
            USING iceberg
            WITH (catalog = 'test_no_creds', read_only = 'true',
                  catalog_namespace = 'ns', catalog_table_name = 'tbl')
        """,
        pg_conn,
        raise_error=False,
    )
    assert err is not None
    assert "no credentials found" in str(err)
    pg_conn.rollback()

    run_command("DROP SERVER test_no_creds CASCADE", superuser_conn)
    superuser_conn.commit()


def test_scope_from_catalogs_conf(
    pg_conn,
    superuser_conn,
    s3,
    extension,
    with_default_location,
    catalogs_conf,
):
    """catalogs.conf can also provide scope alongside credentials."""
    catalogs_conf(
        "test_scope_srv.client_id = 'conf-id'\n"
        "test_scope_srv.client_secret = 'conf-secret'\n"
        "test_scope_srv.scope = 'PRINCIPAL_ROLE:ADMIN'\n"
    )
    run_command(
        """
        CREATE SERVER test_scope_srv TYPE 'rest'
            FOREIGN DATA WRAPPER iceberg_catalog
            OPTIONS (rest_endpoint 'http://localhost:8181')
        """,
        superuser_conn,
    )
    superuser_conn.commit()

    err = run_command(
        """
        CREATE TABLE test_scope_tbl ()
            USING iceberg
            WITH (catalog = 'test_scope_srv', read_only = 'true',
                  catalog_namespace = 'ns', catalog_table_name = 'tbl')
        """,
        pg_conn,
        raise_error=False,
    )
    assert err is not None
    assert "no credentials found" not in str(err)
    pg_conn.rollback()

    run_command("DROP SERVER test_scope_srv CASCADE", superuser_conn)
    superuser_conn.commit()


# ── Backward compatibility ─────────────────────────────────────────────────


def test_catalog_rest_literal_still_works(
    pg_conn, s3, extension, with_default_location
):
    """catalog='rest' (literal) should still work via GUC fallback."""
    err = run_command(
        """
        CREATE TABLE test_rest_literal ()
            USING iceberg
            WITH (catalog = 'rest', read_only = 'true',
                  catalog_namespace = 'ns', catalog_table_name = 'tbl')
        """,
        pg_conn,
        raise_error=False,
    )
    # Will fail because REST GUCs aren't configured, but should NOT fail
    # with "invalid catalog option"
    if err is not None:
        assert "invalid catalog option" not in str(err)
    pg_conn.rollback()


def test_catalog_postgres_literal_still_works(
    pg_conn, s3, extension, with_default_location
):
    """catalog='postgres' (literal) should still work."""
    run_command(
        """
        CREATE TABLE test_pg_literal (id int)
            USING iceberg
            WITH (catalog = 'postgres')
        """,
        pg_conn,
    )
    pg_conn.rollback()


def test_catalog_object_store_literal_still_works(
    pg_conn,
    superuser_conn,
    s3,
    extension,
    with_default_location,
    adjust_object_store_settings,
):
    """catalog='object_store' (literal) should still work."""
    run_command(
        """
        CREATE TABLE test_os_literal (id int)
            USING iceberg
            WITH (catalog = 'object_store')
        """,
        pg_conn,
    )
    pg_conn.rollback()


# ── Query string scrubbing for user mapping DDL ───────────────────────────


def test_scrub_create_user_mapping_in_pg_stat_statements(
    installcheck, superuser_conn, extension
):
    """CREATE USER MAPPING secrets should be scrubbed in pg_stat_statements."""
    if installcheck:
        return

    run_command("CREATE EXTENSION IF NOT EXISTS pg_stat_statements", superuser_conn)
    run_command("SELECT pg_stat_statements_reset()", superuser_conn)
    superuser_conn.commit()

    run_command(
        """
        CREATE SERVER test_scrub_srv TYPE 'rest'
            FOREIGN DATA WRAPPER iceberg_catalog
            OPTIONS (rest_endpoint 'http://localhost:8181')
        """,
        superuser_conn,
    )
    superuser_conn.commit()

    run_command(
        """
        CREATE USER MAPPING FOR CURRENT_USER SERVER test_scrub_srv
            OPTIONS (client_id 'secret_id_value', client_secret 'secret_key_value')
        """,
        superuser_conn,
    )
    superuser_conn.commit()

    result = run_query(
        """
        SELECT query FROM pg_stat_statements
        WHERE query ILIKE '%CREATE USER MAPPING%test_scrub_srv%'
        """,
        superuser_conn,
    )

    assert len(result) >= 1
    query_text = result[0]["query"]
    assert "secret_id_value" not in query_text
    assert "secret_key_value" not in query_text
    assert "'***'" in query_text

    run_command(
        "DROP USER MAPPING FOR CURRENT_USER SERVER test_scrub_srv", superuser_conn
    )
    run_command("DROP SERVER test_scrub_srv", superuser_conn)
    superuser_conn.commit()


def test_scrub_alter_user_mapping_in_pg_stat_statements(
    installcheck, superuser_conn, extension
):
    """ALTER USER MAPPING secrets should also be scrubbed in pg_stat_statements."""
    if installcheck:
        return

    run_command("CREATE EXTENSION IF NOT EXISTS pg_stat_statements", superuser_conn)
    run_command("SELECT pg_stat_statements_reset()", superuser_conn)
    superuser_conn.commit()

    run_command(
        """
        CREATE SERVER test_scrub_alter_srv TYPE 'rest'
            FOREIGN DATA WRAPPER iceberg_catalog
            OPTIONS (rest_endpoint 'http://localhost:8181')
        """,
        superuser_conn,
    )
    run_command(
        """
        CREATE USER MAPPING FOR CURRENT_USER SERVER test_scrub_alter_srv
            OPTIONS (client_id 'old_id', client_secret 'old_secret')
        """,
        superuser_conn,
    )
    superuser_conn.commit()

    run_command("SELECT pg_stat_statements_reset()", superuser_conn)
    superuser_conn.commit()

    run_command(
        """
        ALTER USER MAPPING FOR CURRENT_USER SERVER test_scrub_alter_srv
            OPTIONS (SET client_id 'new_secret_id', SET client_secret 'new_secret_key')
        """,
        superuser_conn,
    )
    superuser_conn.commit()

    result = run_query(
        """
        SELECT query FROM pg_stat_statements
        WHERE query ILIKE '%ALTER USER MAPPING%test_scrub_alter_srv%'
        """,
        superuser_conn,
    )

    assert len(result) >= 1
    query_text = result[0]["query"]
    assert "new_secret_id" not in query_text
    assert "new_secret_key" not in query_text
    assert "'***'" in query_text

    run_command(
        "DROP USER MAPPING FOR CURRENT_USER SERVER test_scrub_alter_srv",
        superuser_conn,
    )
    run_command("DROP SERVER test_scrub_alter_srv", superuser_conn)
    superuser_conn.commit()


def test_scrub_preserves_actual_credentials(superuser_conn, extension):
    """Scrubbing the query string should not affect the stored credentials."""
    run_command(
        """
        CREATE SERVER test_scrub_creds_srv TYPE 'rest'
            FOREIGN DATA WRAPPER iceberg_catalog
            OPTIONS (rest_endpoint 'http://localhost:8181')
        """,
        superuser_conn,
    )
    run_command(
        """
        CREATE USER MAPPING FOR CURRENT_USER SERVER test_scrub_creds_srv
            OPTIONS (client_id 'real_id', client_secret 'real_secret')
        """,
        superuser_conn,
    )

    result = run_query(
        """
        SELECT umoptions FROM pg_user_mapping um
            JOIN pg_foreign_server fs ON um.umserver = fs.oid
        WHERE fs.srvname = 'test_scrub_creds_srv'
        """,
        superuser_conn,
    )

    assert len(result) == 1
    opts = result[0]["umoptions"]
    assert "client_id=real_id" in opts
    assert "client_secret=real_secret" in opts

    superuser_conn.rollback()
