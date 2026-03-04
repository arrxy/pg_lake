package handlers

import (
	"net/http"

	"github.com/snowflake-labs/pg_lake/pglake_rest_catalog/internal/auth"
	"github.com/snowflake-labs/pg_lake/pglake_rest_catalog/internal/catalog"
	"github.com/snowflake-labs/pg_lake/pglake_rest_catalog/internal/config"
)

// NewRouter creates the HTTP handler with all routes registered.
func NewRouter(
	svc *catalog.Service,
	credStore *auth.CredentialStore,
	tokenSigner *auth.TokenSigner,
	cfg *config.Config,
) http.Handler {
	mux := http.NewServeMux()

	// Public endpoints (no authentication required)
	mux.HandleFunc("GET /v1/config", ConfigHandler(cfg))
	mux.HandleFunc("POST /v1/oauth/tokens", OAuthTokenHandler(credStore, tokenSigner))

	// Protected endpoints (require Bearer token)
	protected := http.NewServeMux()
	protected.HandleFunc("GET /v1/{prefix}/namespaces", ListNamespacesHandler(svc))
	protected.HandleFunc("GET /v1/{prefix}/namespaces/{namespace}", GetNamespaceHandler(svc))
	protected.HandleFunc("GET /v1/{prefix}/namespaces/{namespace}/tables", ListTablesHandler(svc))
	protected.HandleFunc("GET /v1/{prefix}/namespaces/{namespace}/tables/{table}", LoadTableHandler(svc))

	// Future write endpoints would be added here:
	// protected.HandleFunc("POST /v1/{prefix}/namespaces", CreateNamespaceHandler(svc))
	// protected.HandleFunc("DELETE /v1/{prefix}/namespaces/{namespace}", DropNamespaceHandler(svc))
	// protected.HandleFunc("POST /v1/{prefix}/namespaces/{namespace}/tables", CreateTableHandler(svc))
	// protected.HandleFunc("DELETE /v1/{prefix}/namespaces/{namespace}/tables/{table}", DropTableHandler(svc))
	// protected.HandleFunc("POST /v1/{prefix}/namespaces/{namespace}/tables/{table}", CommitTableHandler(svc))
	// protected.HandleFunc("POST /v1/{prefix}/tables/rename", RenameTableHandler(svc))
	// protected.HandleFunc("POST /v1/{prefix}/transactions/commit", TransactionCommitHandler(svc))

	// Apply auth middleware to protected routes
	authMiddleware := auth.Middleware(tokenSigner)
	mux.Handle("/v1/", authMiddleware(protected))

	// Apply logging and recovery middleware
	var handler http.Handler = mux
	handler = LoggingMiddleware(cfg.Debug)(handler)
	handler = RecoveryMiddleware(handler)

	return handler
}
