package handlers

import (
	"errors"
	"fmt"
	"net/http"
	"net/url"

	"github.com/snowflake-labs/pg_lake/pglake_rest_catalog/internal/catalog"
)

// ListNamespacesHandler handles GET /v1/{prefix}/namespaces.
func ListNamespacesHandler(svc *catalog.Service) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodGet {
			writeError(w, http.StatusMethodNotAllowed, "BadRequestException", "method not allowed")
			return
		}

		prefix := r.PathValue("prefix")

		namespaces, err := svc.ListNamespaces(r.Context(), prefix)
		if err != nil {
			writeError(w, http.StatusInternalServerError, "InternalServerError", err.Error())
			return
		}

		resp := map[string]interface{}{
			"namespaces": namespaces,
		}
		writeJSON(w, http.StatusOK, resp)
	}
}

// GetNamespaceHandler handles GET /v1/{prefix}/namespaces/{namespace}.
func GetNamespaceHandler(svc *catalog.Service) http.HandlerFunc {
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

		props, err := svc.GetNamespace(r.Context(), prefix, namespace)
		if err != nil {
			if errors.Is(err, catalog.ErrNamespaceNotFound) {
				writeError(w, http.StatusNotFound, "NoSuchNamespaceException",
					fmt.Sprintf("Namespace does not exist: %s", namespace))
				return
			}
			writeError(w, http.StatusInternalServerError, "InternalServerError", err.Error())
			return
		}

		resp := map[string]interface{}{
			"namespace":  []string{namespace},
			"properties": props,
		}
		writeJSON(w, http.StatusOK, resp)
	}
}
