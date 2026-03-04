package config

import (
	"crypto/rand"
	"encoding/hex"
	"flag"
	"fmt"
	"os"
)

type Config struct {
	Port               int
	PostgresConnString string
	ClientID           string
	ClientSecret       string
	JWTSecret          string
	TokenExpirySeconds int
	TLSCertFile        string
	TLSKeyFile         string
	Warehouse          string
	PidfilePath        string
	Debug              bool
}

func Parse(args []string) *Config {
	cfg := &Config{}
	fs := flag.NewFlagSet("pglake_rest_catalog", flag.ExitOnError)

	fs.IntVar(&cfg.Port, "port", 8181, "HTTP listen port")
	fs.StringVar(&cfg.PostgresConnString, "postgres_conn_string",
		"host=/tmp port=5432 dbname=postgres user=postgres",
		"PostgreSQL connection string")
	fs.StringVar(&cfg.ClientID, "client_id", "", "OAuth2 client ID (required)")
	fs.StringVar(&cfg.ClientSecret, "client_secret", "", "OAuth2 client secret (required)")
	fs.StringVar(&cfg.JWTSecret, "jwt_secret", "", "JWT signing secret (auto-generated if empty)")
	fs.IntVar(&cfg.TokenExpirySeconds, "token_expiry_seconds", 3600, "OAuth2 token expiry in seconds")
	fs.StringVar(&cfg.TLSCertFile, "tls_cert_file", "", "TLS certificate file path")
	fs.StringVar(&cfg.TLSKeyFile, "tls_key_file", "", "TLS private key file path")
	fs.StringVar(&cfg.Warehouse, "warehouse", "postgres", "Default warehouse/catalog name")
	fs.StringVar(&cfg.PidfilePath, "pidfile", "", "PID file path")
	fs.BoolVar(&cfg.Debug, "debug", false, "Enable debug logging")

	fs.Usage = func() {
		fmt.Fprintf(os.Stderr, "Usage: pglake_rest_catalog [options]\n\n")
		fmt.Fprintf(os.Stderr, "Iceberg REST Catalog server backed by PostgreSQL/pg_lake.\n\n")
		fmt.Fprintf(os.Stderr, "Options:\n")
		fs.PrintDefaults()
	}

	_ = fs.Parse(args)

	if cfg.ClientID == "" || cfg.ClientSecret == "" {
		fmt.Fprintf(os.Stderr, "Error: --client_id and --client_secret are required\n")
		fs.Usage()
		os.Exit(1)
	}

	if (cfg.TLSCertFile == "") != (cfg.TLSKeyFile == "") {
		fmt.Fprintf(os.Stderr, "Error: --tls_cert_file and --tls_key_file must both be provided or both omitted\n")
		os.Exit(1)
	}

	if cfg.JWTSecret == "" {
		b := make([]byte, 32)
		if _, err := rand.Read(b); err != nil {
			fmt.Fprintf(os.Stderr, "Error generating JWT secret: %v\n", err)
			os.Exit(1)
		}
		cfg.JWTSecret = hex.EncodeToString(b)
	}

	return cfg
}
