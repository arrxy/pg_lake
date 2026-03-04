package handlers

import (
	"encoding/json"
	"net/http"
)

// ErrorResponse follows the Iceberg REST error format, compatible with
// the existing pg_lake REST catalog client (ReportHTTPError in rest_catalog.c).
type ErrorResponse struct {
	Error ErrorBody `json:"error"`
}

type ErrorBody struct {
	Message string `json:"message"`
	Type    string `json:"type"`
	Code    int    `json:"code"`
}

func writeError(w http.ResponseWriter, code int, errType, message string) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(code)
	json.NewEncoder(w).Encode(ErrorResponse{
		Error: ErrorBody{
			Message: message,
			Type:    errType,
			Code:    code,
		},
	})
}

func writeJSON(w http.ResponseWriter, code int, v interface{}) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(code)
	json.NewEncoder(w).Encode(v)
}
