package auth

import (
	"context"
	"fmt"
	"net/http"
	"strings"
)

type contextKey string

const ClaimsContextKey contextKey = "claims"

// Middleware returns HTTP middleware that validates Bearer tokens
// on all requests. The skipPaths set contains path prefixes that
// do not require authentication (e.g., /v1/config, /v1/oauth/tokens).
func Middleware(tokenSigner *TokenSigner) func(http.Handler) http.Handler {
	return func(next http.Handler) http.Handler {
		return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			authHeader := r.Header.Get("Authorization")
			if authHeader == "" {
				writeAuthError(w, http.StatusUnauthorized, "missing Authorization header")
				return
			}

			if !strings.HasPrefix(authHeader, "Bearer ") {
				writeAuthError(w, http.StatusUnauthorized, "Authorization header must use Bearer scheme")
				return
			}

			tokenString := strings.TrimPrefix(authHeader, "Bearer ")
			claims, err := tokenSigner.ValidateToken(tokenString)
			if err != nil {
				writeAuthError(w, http.StatusUnauthorized, "invalid or expired token")
				return
			}

			ctx := context.WithValue(r.Context(), ClaimsContextKey, claims)
			next.ServeHTTP(w, r.WithContext(ctx))
		})
	}
}

func writeAuthError(w http.ResponseWriter, code int, message string) {
	w.Header().Set("Content-Type", "application/json")
	w.Header().Set("WWW-Authenticate", "Bearer")
	w.WriteHeader(code)
	// Use the Iceberg REST error format
	fmt.Fprintf(w, `{"error":{"message":"%s","type":"NotAuthorizedException","code":%d}}`, message, code)
}
