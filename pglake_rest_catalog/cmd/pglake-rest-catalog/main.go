package main

import (
	"context"
	"fmt"
	"log"
	"net"
	"net/http"
	"os"
	"os/signal"
	"strconv"
	"syscall"

	"github.com/snowflake-labs/pg_lake/pglake_rest_catalog/internal/auth"
	"github.com/snowflake-labs/pg_lake/pglake_rest_catalog/internal/catalog"
	"github.com/snowflake-labs/pg_lake/pglake_rest_catalog/internal/config"
	"github.com/snowflake-labs/pg_lake/pglake_rest_catalog/internal/db"
	"github.com/snowflake-labs/pg_lake/pglake_rest_catalog/internal/handlers"
	tlspkg "github.com/snowflake-labs/pg_lake/pglake_rest_catalog/internal/tls"
)

func main() {
	cfg := config.Parse(os.Args[1:])

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// Write PID file
	if cfg.PidfilePath != "" {
		if err := os.WriteFile(cfg.PidfilePath, []byte(strconv.Itoa(os.Getpid())), 0644); err != nil {
			log.Fatalf("Failed to write PID file: %v", err)
		}
		defer os.Remove(cfg.PidfilePath)
	}

	// Connect to PostgreSQL
	pool, err := db.NewPool(ctx, cfg.PostgresConnString)
	if err != nil {
		log.Fatalf("Failed to connect to PostgreSQL: %v", err)
	}
	defer pool.Close()

	// Initialize auth
	credStore := auth.NewCredentialStore(cfg.ClientID, cfg.ClientSecret)
	tokenSigner := auth.NewTokenSigner(cfg.JWTSecret, cfg.TokenExpirySeconds)

	// Initialize catalog service and HTTP router
	svc := catalog.NewService(pool)
	router := handlers.NewRouter(svc, credStore, tokenSigner, cfg)

	// Configure HTTP server
	addr := fmt.Sprintf(":%d", cfg.Port)
	server := &http.Server{
		Addr:    addr,
		Handler: router,
	}

	// Configure TLS if certificates are provided
	useTLS := cfg.TLSCertFile != ""
	if useTLS {
		tlsConfig, err := tlspkg.LoadConfig(cfg.TLSCertFile, cfg.TLSKeyFile)
		if err != nil {
			log.Fatalf("Failed to load TLS config: %v", err)
		}
		server.TLSConfig = tlsConfig
	}

	// Handle graceful shutdown on SIGINT/SIGTERM
	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, syscall.SIGINT, syscall.SIGTERM)

	go func() {
		<-sigCh
		log.Println("Shutting down...")
		cancel()
		server.Shutdown(context.Background())
	}()

	// Start server
	protocol := "HTTP"
	if useTLS {
		protocol = "HTTPS"
	}

	listener, err := net.Listen("tcp", addr)
	if err != nil {
		log.Fatalf("Failed to listen on %s: %v", addr, err)
	}

	log.Printf("pglake_rest_catalog listening on %s (%s), warehouse=%s",
		addr, protocol, cfg.Warehouse)

	if useTLS {
		err = server.ServeTLS(listener, cfg.TLSCertFile, cfg.TLSKeyFile)
	} else {
		err = server.Serve(listener)
	}

	if err != nil && err != http.ErrServerClosed {
		log.Fatalf("Server error: %v", err)
	}

	log.Println("Server stopped.")
}
