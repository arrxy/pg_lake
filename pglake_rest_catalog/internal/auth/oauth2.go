package auth

import (
	"fmt"
	"time"

	"github.com/golang-jwt/jwt/v5"
)

// TokenSigner creates and validates JWT tokens for OAuth2.
type TokenSigner struct {
	secret        []byte
	expirySeconds int
}

func NewTokenSigner(secret string, expirySeconds int) *TokenSigner {
	return &TokenSigner{
		secret:        []byte(secret),
		expirySeconds: expirySeconds,
	}
}

// IssueToken creates a signed JWT for the given client.
func (ts *TokenSigner) IssueToken(clientID, scope string) (string, int, error) {
	now := time.Now()
	claims := jwt.MapClaims{
		"sub":   clientID,
		"scope": scope,
		"iat":   now.Unix(),
		"exp":   now.Add(time.Duration(ts.expirySeconds) * time.Second).Unix(),
	}

	token := jwt.NewWithClaims(jwt.SigningMethodHS256, claims)
	tokenString, err := token.SignedString(ts.secret)
	if err != nil {
		return "", 0, fmt.Errorf("signing token: %w", err)
	}

	return tokenString, ts.expirySeconds, nil
}

// ValidateToken parses and validates a JWT token string.
// Returns the claims if valid, or an error if invalid/expired.
func (ts *TokenSigner) ValidateToken(tokenString string) (jwt.MapClaims, error) {
	token, err := jwt.Parse(tokenString, func(token *jwt.Token) (interface{}, error) {
		if _, ok := token.Method.(*jwt.SigningMethodHMAC); !ok {
			return nil, fmt.Errorf("unexpected signing method: %v", token.Header["alg"])
		}
		return ts.secret, nil
	})
	if err != nil {
		return nil, fmt.Errorf("parsing token: %w", err)
	}

	claims, ok := token.Claims.(jwt.MapClaims)
	if !ok || !token.Valid {
		return nil, fmt.Errorf("invalid token")
	}

	return claims, nil
}
