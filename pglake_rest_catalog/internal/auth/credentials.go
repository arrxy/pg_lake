package auth

import (
	"crypto/subtle"
)

// CredentialStore validates OAuth2 client credentials.
// Currently supports a single client pair; the struct is designed
// for future expansion to multiple clients (e.g., stored in PostgreSQL).
type CredentialStore struct {
	clientID     string
	clientSecret string
}

func NewCredentialStore(clientID, clientSecret string) *CredentialStore {
	return &CredentialStore{
		clientID:     clientID,
		clientSecret: clientSecret,
	}
}

func (cs *CredentialStore) Validate(clientID, clientSecret string) bool {
	idMatch := subtle.ConstantTimeCompare([]byte(cs.clientID), []byte(clientID)) == 1
	secretMatch := subtle.ConstantTimeCompare([]byte(cs.clientSecret), []byte(clientSecret)) == 1
	return idMatch && secretMatch
}
