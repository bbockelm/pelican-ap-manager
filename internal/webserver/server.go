package webserver

import (
	"context"
	"crypto/tls"
	"fmt"
	"net"
	"net/http"
	"os"
	"path/filepath"
	"time"

	htcondorconfig "github.com/bbockelm/golang-htcondor/config"
	"github.com/bbockelm/golang-htcondor/httpserver"
	htcondorlogging "github.com/bbockelm/golang-htcondor/logging"
)

type Server struct {
	db              *DB
	handlers        *Handlers
	htcondorHandler *httpserver.Handler
	httpServer      *http.Server
	logger          *htcondorlogging.Logger
	listenAddr      string
	socketPath      string
	tlsCert         string
	tlsKey          string
	cleanupStop     chan struct{}
	listener        net.Listener
}

func NewServer(listenAddr, socketPath, tlsCert, tlsKey, dbPath string, logger *htcondorlogging.Logger) (*Server, error) {
	// If TLS paths are not provided, derive defaults from HTCondor config using $(SPOOL)
	if tlsCert == "" || tlsKey == "" {
		condorCfg, err := htcondorconfig.New()
		if err != nil {
			return nil, fmt.Errorf("failed to load HTCondor config for TLS defaults: %w", err)
		}

		spool, _ := condorCfg.Get("SPOOL")
		if spool == "" {
			spool = "./data"
		}

		base := filepath.Join(spool, "pelican-certs")
		if tlsCert == "" {
			tlsCert = filepath.Join(base, "server.crt")
		}
		if tlsKey == "" {
			tlsKey = filepath.Join(base, "server.key")
		}
	}
	db, err := NewDB(dbPath)
	if err != nil {
		return nil, fmt.Errorf("failed to initialize database: %w", err)
	}

	handlers := NewHandlers(db, logger, socketPath, listenAddr)

	// Create HTCondor HTTP handler (best effort). If it fails due to missing
	// HTCondor configuration (common in unit tests), we continue without it.
	var htcondorHandler *httpserver.Handler
	if htcondorConfig, err := htcondorconfig.New(); err == nil {
		// Try to get schedd address from config
		scheddAddr := ""
		if host, _ := htcondorConfig.Get("SCHEDD_HOST"); host != "" {
			port, _ := htcondorConfig.Get("SCHEDD_PORT")
			if port == "" {
				port = "9618" // Default schedd port
			}
			scheddAddr = host + ":" + port
		}

		// Get database paths from config or use defaults
		oauth2DBPath, _ := htcondorConfig.Get("HTTP_API_OAUTH2_DB_PATH")
		idpDBPath, _ := htcondorConfig.Get("HTTP_API_IDP_DB_PATH")

		htcondorHandlerCfg := httpserver.HandlerConfig{
			Logger:         logger,
			HTCondorConfig: htcondorConfig,
			ScheddAddr:     scheddAddr,
			OAuth2DBPath:   oauth2DBPath,
			IDPDBPath:      idpDBPath,
			EnableMCP:      false, // OAuth2 issuer not needed yet
		}

		if handler, err := httpserver.NewHandler(htcondorHandlerCfg); err == nil {
			htcondorHandler = handler
		} else {
			logger.Warnf(htcondorlogging.DestinationGeneral, "HTCondor handler disabled: %v", err)
		}
	} else {
		logger.Warnf(htcondorlogging.DestinationGeneral, "HTCondor config unavailable: %v", err)
	}

	mux := http.NewServeMux()

	// Mount pelican-specific handlers
	mux.HandleFunc("/api/v1/sandbox/register", handlers.HandleRegister)
	mux.HandleFunc("/sandboxes/", func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path[len(r.URL.Path)-6:] == "/input" {
			handlers.HandleGetSandbox(w, r)
		} else if r.URL.Path[len(r.URL.Path)-7:] == "/output" {
			handlers.HandlePutSandbox(w, r)
		} else {
			http.Error(w, "Not found", http.StatusNotFound)
		}
	})

	// Mount golang-htcondor handler at /api/ if available
	// Note: golang-htcondor handler expects paths like /v1/ping, /v1/jobs, etc.
	// so we strip /api to give it the rest of the path
	if htcondorHandler != nil {
		logger.Infof(htcondorlogging.DestinationGeneral, "Mounting golang-htcondor HTTP API at /api/")
		mux.Handle("/api/", http.StripPrefix("/api", htcondorHandler))
	}

	srv := &Server{
		db:              db,
		handlers:        handlers,
		htcondorHandler: htcondorHandler,
		httpServer: &http.Server{
			Handler:      mux,
			ReadTimeout:  30 * time.Second,
			WriteTimeout: 30 * time.Second,
			IdleTimeout:  60 * time.Second,
		},
		logger:      logger,
		listenAddr:  listenAddr,
		socketPath:  socketPath,
		tlsCert:     tlsCert,
		tlsKey:      tlsKey,
		cleanupStop: make(chan struct{}),
	}

	return srv, nil
}

func (s *Server) Start(ctx context.Context) error {
	go s.cleanupLoop(ctx)

	var err error

	// Generate or use existing TLS certificates
	if err := s.ensureCertificates(); err != nil {
		return fmt.Errorf("failed to ensure certificates: %w", err)
	}

	// TLS is required for both domain socket and TCP
	if s.tlsCert == "" || s.tlsKey == "" {
		return fmt.Errorf("TLS certificate and key are required")
	}

	// Load TLS certificate
	cert, err := tls.LoadX509KeyPair(s.tlsCert, s.tlsKey)
	if err != nil {
		return fmt.Errorf("failed to load TLS certificate: %w", err)
	}

	tlsConfig := &tls.Config{
		MinVersion:   tls.VersionTLS12,
		Certificates: []tls.Certificate{cert},
	}
	s.httpServer.TLSConfig = tlsConfig

	// Decide whether to use Unix socket or TCP
	if s.socketPath != "" {
		// Remove existing socket file if present
		if _, err := os.Stat(s.socketPath); err == nil {
			if err := os.Remove(s.socketPath); err != nil {
				return fmt.Errorf("failed to remove existing socket: %w", err)
			}
		}

		unixListener, err := net.Listen("unix", s.socketPath)
		if err != nil {
			return fmt.Errorf("failed to listen on Unix socket %s: %w", s.socketPath, err)
		}
		defer os.Remove(s.socketPath)

		// Set socket permissions to allow access
		if err := os.Chmod(s.socketPath, 0666); err != nil {
			s.logger.Errorf(htcondorlogging.DestinationGeneral, "Failed to set socket permissions: %v", err)
		}

		// Wrap Unix socket listener with TLS
		s.listener = tls.NewListener(unixListener, tlsConfig)
		s.logger.Infof(htcondorlogging.DestinationGeneral, "Starting TLS web server on Unix socket %s", s.socketPath)
	} else if s.listenAddr != "" {
		s.logger.Infof(htcondorlogging.DestinationGeneral, "Starting TLS HTTPS server on %s with cert=%s key=%s", s.listenAddr, s.tlsCert, s.tlsKey)

		tcpListener, err := net.Listen("tcp", s.listenAddr)
		if err != nil {
			return fmt.Errorf("failed to create TCP listener: %w", err)
		}

		// Wrap TCP listener with TLS
		s.listener = tls.NewListener(tcpListener, tlsConfig)
	} else {
		return fmt.Errorf("either listenAddr or socketPath must be specified")
	}

	errChan := make(chan error, 1)
	go func() {
		if err := s.httpServer.Serve(s.listener); err != nil && err != http.ErrServerClosed {
			errChan <- fmt.Errorf("server error: %w", err)
		}
		close(errChan)
	}()

	select {
	case <-ctx.Done():
		s.logger.Infof(htcondorlogging.DestinationGeneral, "Web server shutting down")
		shutdownCtx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()

		close(s.cleanupStop)

		if err := s.httpServer.Shutdown(shutdownCtx); err != nil {
			s.logger.Errorf(htcondorlogging.DestinationGeneral, "Error during shutdown: %v", err)
		}

		if err := s.db.Close(); err != nil {
			s.logger.Errorf(htcondorlogging.DestinationGeneral, "Error closing database: %v", err)
		}

		return ctx.Err()
	case err := <-errChan:
		close(s.cleanupStop)
		s.db.Close()
		return err
	}
}

// ensureCertificates generates TLS certificates if they don't already exist
// Creates the certificate directory if needed
func (s *Server) ensureCertificates() error {
	// If certificates already exist, nothing to do
	if _, err := os.Stat(s.tlsCert); err == nil {
		if _, err := os.Stat(s.tlsKey); err == nil {
			s.logger.Infof(htcondorlogging.DestinationGeneral, "Using existing TLS certificates: %s", s.tlsCert)
			return nil
		}
	}

	// Create certificate directory if it doesn't exist
	certDir := filepath.Dir(s.tlsCert)
	if err := os.MkdirAll(certDir, 0755); err != nil {
		return fmt.Errorf("failed to create certificate directory %s: %w", certDir, err)
	}
	// Create key directory if different
	keyDir := filepath.Dir(s.tlsKey)
	if keyDir != certDir {
		if err := os.MkdirAll(keyDir, 0755); err != nil {
			return fmt.Errorf("failed to create key directory %s: %w", keyDir, err)
		}
	}

	// Generate new certificate chain
	s.logger.Infof(htcondorlogging.DestinationGeneral, "Generating new TLS certificate chain in %s", certDir)

	// Determine hostname for certificate
	hostname := "localhost"
	if s.listenAddr != "" {
		// Extract hostname from address (e.g., "localhost:8080" -> "localhost")
		if h, _, err := net.SplitHostPort(s.listenAddr); err == nil {
			hostname = h
		}
	}

	if err := GenerateTestCertificate(s.tlsCert, s.tlsKey, hostname); err != nil {
		return fmt.Errorf("failed to generate TLS certificates: %w", err)
	}

	s.logger.Infof(htcondorlogging.DestinationGeneral, "Successfully generated TLS certificate chain")
	return nil
}

func (s *Server) cleanupLoop(ctx context.Context) {
	ticker := time.NewTicker(1 * time.Hour)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-s.cleanupStop:
			return
		case <-ticker.C:
			if err := s.db.CleanupExpiredTokens(); err != nil {
				s.logger.Errorf(htcondorlogging.DestinationGeneral, "Failed to cleanup expired tokens: %v", err)
			} else {
				s.logger.Debugf(htcondorlogging.DestinationGeneral, "Cleaned up expired tokens")
			}
		}
	}
}
