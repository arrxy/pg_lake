package handlers

import (
	"net/http"

	"github.com/snowflake-labs/pg_lake/pglake_rest_catalog/internal/config"
)

// ConfigHandler handles GET /v1/config.
// Returns server configuration including the default warehouse.
func ConfigHandler(cfg *config.Config) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodGet {
			writeError(w, http.StatusMethodNotAllowed, "BadRequestException", "method not allowed")
			return
		}

		warehouse := r.URL.Query().Get("warehouse")
		if warehouse == "" {
			warehouse = cfg.Warehouse
		}

		resp := map[string]interface{}{
			"defaults": map[string]string{},
			"overrides": map[string]string{
				"prefix": warehouse,
			},
		}
		writeJSON(w, http.StatusOK, resp)
	}
}
