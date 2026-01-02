package webserver

import (
	"context"
	"crypto/tls"
	"fmt"
	"net"
	"net/http"
	"os"
	"time"

	htcondorlogging "github.com/bbockelm/golang-htcondor/logging"
)

type Server struct {
	db          *DB
	handlers    *Handlers
	httpServer  *http.Server
	logger      *htcondorlogging.Logger
	listenAddr  string
	socketPath  string
	tlsCert     string
	tlsKey      string
	cleanupStop chan struct{}
	listener    net.Listener
}

func NewServer(listenAddr, socketPath, tlsCert, tlsKey, dbPath string, logger *htcondorlogging.Logger) (*Server, error) {
	db, err := NewDB(dbPath)
	if err != nil {
		return nil, fmt.Errorf("failed to initialize database: %w", err)
	}

	handlers := NewHandlers(db, logger, socketPath, listenAddr)

	mux := http.NewServeMux()
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

	srv := &Server{
		db:       db,
		handlers: handlers,
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

	// Decide whether to use Unix socket or TCP
	if s.socketPath != "" {
		// Remove existing socket file if present
		if _, err := os.Stat(s.socketPath); err == nil {
			if err := os.Remove(s.socketPath); err != nil {
				return fmt.Errorf("failed to remove existing socket: %w", err)
			}
		}

		s.listener, err = net.Listen("unix", s.socketPath)
		if err != nil {
			return fmt.Errorf("failed to listen on Unix socket %s: %w", s.socketPath, err)
		}
		defer os.Remove(s.socketPath)

		// Set socket permissions to allow access
		if err := os.Chmod(s.socketPath, 0666); err != nil {
			s.logger.Errorf(htcondorlogging.DestinationGeneral, "Failed to set socket permissions: %v", err)
		}

		s.logger.Infof(htcondorlogging.DestinationGeneral, "Starting web server on Unix socket %s", s.socketPath)
	} else if s.listenAddr != "" {
		if s.tlsCert != "" && s.tlsKey != "" {
			s.logger.Infof(htcondorlogging.DestinationGeneral, "Starting HTTPS server on %s with cert=%s key=%s", s.listenAddr, s.tlsCert, s.tlsKey)

			tlsConfig := &tls.Config{
				MinVersion: tls.VersionTLS12,
			}
			s.httpServer.TLSConfig = tlsConfig

			s.listener, err = tls.Listen("tcp", s.listenAddr, tlsConfig)
			if err != nil {
				return fmt.Errorf("failed to create TLS listener: %w", err)
			}
		} else {
			s.logger.Infof(htcondorlogging.DestinationGeneral, "Starting HTTP server on %s", s.listenAddr)
			s.listener, err = net.Listen("tcp", s.listenAddr)
			if err != nil {
				return fmt.Errorf("failed to create TCP listener: %w", err)
			}
		}
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
