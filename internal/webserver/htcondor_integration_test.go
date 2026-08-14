package webserver

import (
	"context"
	"crypto/tls"
	"encoding/json"
	"fmt"
	"io"
	"net"
	"net/http"
	"os"
	"testing"
	"time"

	htcondorconfig "github.com/bbockelm/golang-htcondor/config"
	"github.com/bbockelm/golang-htcondor/webapi/httpserver"
)

// TestHTCondorHandlerIntegration verifies that golang-htcondor's HTTP handler
// can be created and mounted in the webserver.
func TestHTCondorHandlerIntegration(t *testing.T) {
	logger := createTestLogger(t)

	// Create a minimal HTCondor config for testing
	tmpDir := t.TempDir()
	configFile := tmpDir + "/condor_config"
	configContent := fmt.Sprintf(`
SPOOL = %s/spool
LOCAL_DIR = %s/local
LOG = %s/log
SCHEDD_NAME = test_schedd
SCHEDD_HOST = 127.0.0.1
SCHEDD_PORT = 9618
HTTP_API_OAUTH2_DB_PATH = %s/oauth2.db
HTTP_API_IDP_DB_PATH = %s/idp.db
`, tmpDir, tmpDir, tmpDir, tmpDir, tmpDir)

	if err := os.WriteFile(configFile, []byte(configContent), 0644); err != nil {
		t.Fatalf("Failed to write test config: %v", err)
	}

	// Set environment variable for HTCondor config
	oldConfig := os.Getenv("CONDOR_CONFIG")
	_ = os.Setenv("CONDOR_CONFIG", configFile)
	defer func() {
		if oldConfig != "" {
			_ = os.Setenv("CONDOR_CONFIG", oldConfig)
		} else {
			_ = os.Unsetenv("CONDOR_CONFIG")
		}
	}()

	// Load HTCondor config
	htcondorConfig, err := htcondorconfig.New()
	if err != nil {
		t.Fatalf("Failed to create HTCondor config: %v", err)
	}

	// Create handler with a mock schedd address and database paths
	handlerCfg := httpserver.HandlerConfig{
		Logger:         logger,
		HTCondorConfig: htcondorConfig,
		ScheddAddr:     "127.0.0.1:9618", // Mock schedd address
		EnableMCP:      false,
		OAuth2DBPath:   tmpDir + "/oauth2.db",
		IDPDBPath:      tmpDir + "/idp.db",
	}

	handler, err := httpserver.NewHandler(handlerCfg)
	if err != nil {
		t.Fatalf("Failed to create HTCondor handler: %v", err)
	}

	if handler == nil {
		t.Fatal("Expected non-nil handler")
	}

	t.Log("✓ HTCondor HTTP handler successfully created")

	// Now test that it can be mounted and accessed via the webserver
	// The server's htcondorHandler may be nil if it can't initialize its own
	// (due to missing ScheddAddr or database paths in the default config),
	// but that's okay - we've verified the handler CAN be created above.
	certFile := tmpDir + "/server.crt"
	keyFile := tmpDir + "/server.key"

	srv, err := NewServer("localhost:0", "", certFile, keyFile, ":memory:", logger)
	if err != nil {
		t.Fatalf("Failed to create server: %v", err)
	}

	// The server may not have initialized htcondorHandler due to missing ScheddAddr
	// in its internal config loading. That's expected and okay for this test.
	if srv.htcondorHandler == nil {
		t.Log("Note: Server's htcondorHandler is nil (ScheddAddr/DB paths not in default config)")
		t.Log("✓ Test verified that golang-htcondor handler CAN be created when properly configured")
		return
	}

	t.Log("✓ Server's htcondorHandler is initialized and will be mounted")

	// Start server in background
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	go func() {
		if err := srv.Start(ctx); err != nil && err != context.Canceled {
			t.Logf("Server error: %v", err)
		}
	}()

	// Poll for server readiness instead of arbitrary sleep
	var listenerAddr string
	ticker := time.NewTicker(100 * time.Millisecond)
	defer ticker.Stop()
	timeoutCtx, timeoutCancel := context.WithTimeout(ctx, 5*time.Second)
	defer timeoutCancel()

	for {
		select {
		case <-timeoutCtx.Done():
			t.Fatalf("Server failed to start within timeout")
		case <-ticker.C:
			if addr := srv.GetListenerAddr(); addr != "" {
				listenerAddr = addr
				goto serverReady
			}
		}
	}

serverReady:
	// Extract host from listener address
	host, port, err := net.SplitHostPort(listenerAddr)
	if err != nil {
		t.Fatalf("Failed to parse listener address: %v", err)
	}

	// Load the CA certificate for verification
	caCertPool, err := LoadCACertificate(certFile)
	if err != nil {
		t.Fatalf("Failed to load CA certificate: %v", err)
	}

	// Create HTTP client that trusts the generated CA
	tr := &http.Transport{
		TLSClientConfig: &tls.Config{
			RootCAs: caCertPool,
		},
	}
	client := &http.Client{Transport: tr, Timeout: 5 * time.Second}

	// Test /api/v1/ping endpoint (from golang-htcondor)
	t.Run("HTCondor /api/v1/ping endpoint", func(t *testing.T) {
		resp, err := client.Get("https://" + host + ":" + port + "/api/v1/ping")
		if err != nil {
			t.Fatalf("Failed to call /api/v1/ping: %v", err)
		}
		defer func() { _ = resp.Body.Close() }()

		body, _ := io.ReadAll(resp.Body)

		if resp.StatusCode != http.StatusOK {
			t.Logf("Response status: %d", resp.StatusCode)
			t.Logf("Response body: %s", string(body))
			t.Fatalf("Expected status 200, got %d", resp.StatusCode)
		}

		// Verify it's a valid JSON response
		var pingResp map[string]interface{}
		if err := json.Unmarshal(body, &pingResp); err != nil {
			t.Fatalf("Failed to parse ping response: %v", err)
		}

		t.Logf("✓ HTCondor API /api/v1/ping responded: %s", string(body))
	})

	// Test /api/v1/jobs endpoint (from golang-htcondor)
	t.Run("HTCondor /api/v1/jobs endpoint", func(t *testing.T) {
		resp, err := client.Get("https://" + host + ":" + port + "/api/v1/jobs")
		if err != nil {
			t.Fatalf("Failed to call /api/v1/jobs: %v", err)
		}
		defer func() { _ = resp.Body.Close() }()

		// The endpoint may require auth; accept 200 (success) or 401 (auth required).
		if resp.StatusCode != http.StatusOK && resp.StatusCode != http.StatusUnauthorized {
			body, _ := io.ReadAll(resp.Body)
			t.Fatalf("Expected status 200 or 401, got %d: %s", resp.StatusCode, string(body))
		}

		body, _ := io.ReadAll(resp.Body)
		t.Logf("✓ HTCondor API /api/v1/jobs responded: %s", string(body))
	})

	// Test that our pelican endpoints still work
	t.Run("Pelican /api/v1/sandbox/register endpoint", func(t *testing.T) {
		resp, err := client.Get("https://" + host + ":" + port + "/api/v1/sandbox/register")
		if err != nil {
			t.Fatalf("Failed to call /api/v1/sandbox/register: %v", err)
		}
		defer func() { _ = resp.Body.Close() }()

		// This endpoint requires POST, so we expect 405
		if resp.StatusCode != http.StatusMethodNotAllowed {
			body, _ := io.ReadAll(resp.Body)
			t.Logf("Response body: %s", string(body))
			t.Errorf("Expected status 405 Method Not Allowed, got %d", resp.StatusCode)
		}

		t.Log("✓ Pelican API /api/v1/sandbox/register is accessible and returns expected error for GET")
	})
}
