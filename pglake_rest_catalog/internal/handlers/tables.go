package handlers

import (
	"errors"
	"fmt"
	"net/http"
	"net/url"

	"github.com/snowflake-labs/pg_lake/pglake_rest_catalog/internal/catalog"
)

// ListTablesHandler handles GET /v1/{prefix}/namespaces/{namespace}/tables.
func ListTablesHandler(svc *catalog.Service) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodGet {
			writeError(w, http.StatusMethodNotAllowed, "BadRequestException", "method not allowed")
			return
		}

		prefix := r.PathValue("prefix")
		namespace, err := url.PathUnescape(r.PathValue("namespace"))
		if err != nil {
			writeError(w, http.StatusBadRequest, "BadRequestException",
				fmt.Sprintf("invalid namespace encoding: %v", err))
			return
		}

		tables, err := svc.ListTables(r.Context(), prefix, namespace)
		if err != nil {
			writeError(w, http.StatusInternalServerError, "InternalServerError", err.Error())
			return
		}

		resp := map[string]interface{}{
			"identifiers": tables,
		}
		writeJSON(w, http.StatusOK, resp)
	}
}

// LoadTableHandler handles GET /v1/{prefix}/namespaces/{namespace}/tables/{table}.
func LoadTableHandler(svc *catalog.Service) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodGet {
			writeError(w, http.StatusMethodNotAllowed, "BadRequestException", "method not allowed")
			return
		}

		prefix := r.PathValue("prefix")
		namespace, err := url.PathUnescape(r.PathValue("namespace"))
		if err != nil {
			writeError(w, http.StatusBadRequest, "BadRequestException",
				fmt.Sprintf("invalid namespace encoding: %v", err))
			return
		}
		table, err := url.PathUnescape(r.PathValue("table"))
		if err != nil {
			writeError(w, http.StatusBadRequest, "BadRequestException",
				fmt.Sprintf("invalid table name encoding: %v", err))
			return
		}

		result, err := svc.LoadTable(r.Context(), prefix, namespace, table)
		if err != nil {
			if errors.Is(err, catalog.ErrTableNotFound) {
				writeError(w, http.StatusNotFound, "NoSuchTableException",
					fmt.Sprintf("Table does not exist: %s.%s", namespace, table))
				return
			}
			writeError(w, http.StatusInternalServerError, "InternalServerError", err.Error())
			return
		}

		writeJSON(w, http.StatusOK, result)
	}
}
