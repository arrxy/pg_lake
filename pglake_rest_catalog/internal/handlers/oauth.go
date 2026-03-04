package handlers

import (
	"net/http"

	"github.com/snowflake-labs/pg_lake/pglake_rest_catalog/internal/auth"
)

// OAuthTokenHandler handles POST /v1/oauth/tokens.
// Implements the OAuth2 client_credentials grant type.
func OAuthTokenHandler(credStore *auth.CredentialStore, tokenSigner *auth.TokenSigner) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost {
			writeError(w, http.StatusMethodNotAllowed, "BadRequestException", "method not allowed")
			return
		}

		if err := r.ParseForm(); err != nil {
			writeError(w, http.StatusBadRequest, "BadRequestException", "invalid form data")
			return
		}

		grantType := r.FormValue("grant_type")
		if grantType != "client_credentials" {
			writeError(w, http.StatusBadRequest, "BadRequestException",
				"unsupported grant_type: must be client_credentials")
			return
		}

		// Try Basic auth header first (standard OAuth2), then form body (horizon mode)
		clientID, clientSecret, hasBasic := r.BasicAuth()
		if !hasBasic {
			clientID = r.FormValue("client_id")
			clientSecret = r.FormValue("client_secret")
		}

		if clientID == "" || clientSecret == "" {
			writeError(w, http.StatusUnauthorized, "NotAuthorizedException",
				"client credentials required via Basic auth or form body")
			return
		}

		if !credStore.Validate(clientID, clientSecret) {
			writeError(w, http.StatusUnauthorized, "NotAuthorizedException",
				"invalid client credentials")
			return
		}

		scope := r.FormValue("scope")
		token, expiresIn, err := tokenSigner.IssueToken(clientID, scope)
		if err != nil {
			writeError(w, http.StatusInternalServerError, "InternalServerError",
				"failed to issue token")
			return
		}

		resp := map[string]interface{}{
			"access_token":     token,
			"token_type":       "bearer",
			"expires_in":       expiresIn,
			"issued_token_type": "urn:ietf:params:oauth:token-type:access_token",
		}
		writeJSON(w, http.StatusOK, resp)
	}
}
