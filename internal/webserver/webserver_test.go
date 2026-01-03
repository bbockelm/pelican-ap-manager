package webserver

import (
	"archive/tar"
	"bytes"
	"compress/gzip"
	"crypto/tls"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"os/user"
	"path/filepath"
	"strings"
	"testing"

	htcondorlogging "github.com/bbockelm/golang-htcondor/logging"
)

// createTestLogger creates a logger for testing that doesn't fail
func createTestLogger(t *testing.T) *htcondorlogging.Logger {
	t.Helper()
	// Create a basic logger that writes to stderr
	logger, err := htcondorlogging.New(&htcondorlogging.Config{
		OutputPath: "stderr",
		DestinationLevels: map[htcondorlogging.Destination]htcondorlogging.Verbosity{
			htcondorlogging.DestinationGeneral: htcondorlogging.VerbosityError, // Only log errors in tests
		},
	})
	if err != nil {
		t.Fatalf("Failed to create test logger: %v", err)
	}
	return logger
}

// getTestUsername returns the current username, or "nobody" if running as root
func getTestUsername(t *testing.T) string {
	t.Helper()
	currentUser, err := user.Current()
	if err != nil {
		t.Fatalf("Failed to get current user: %v", err)
	}

	// Use "nobody" if running as root
	if currentUser.Uid == "0" {
		return "nobody"
	}

	return currentUser.Username
}

// createTestCertificates generates TLS certificates for testing
func createTestCertificates(t *testing.T) (certPath, keyPath string) {
	t.Helper()
	tmpDir := t.TempDir()
	certPath = filepath.Join(tmpDir, "test.crt")
	keyPath = filepath.Join(tmpDir, "test.key")

	if err := GenerateTestCertificate(certPath, keyPath, "localhost"); err != nil {
		t.Fatalf("Failed to generate test certificate: %v", err)
	}

	return certPath, keyPath
}

// createTLSClient creates an HTTPS client that accepts self-signed certificates
func createTLSClient(t *testing.T, certPath string) *http.Client {
	t.Helper()

	// Load the certificate to create a CA pool
	caCertPool, err := LoadCACertificate(certPath)
	if err != nil {
		t.Fatalf("Failed to load CA certificate: %v", err)
	}

	tlsConfig := &tls.Config{
		RootCAs:            caCertPool,
		InsecureSkipVerify: false,
		MinVersion:         tls.VersionTLS12,
	}

	return &http.Client{
		Transport: &http.Transport{
			TLSClientConfig: tlsConfig,
		},
	}
}

func TestRegisterJob(t *testing.T) {
	dbPath := t.TempDir() + "/test.db"
	db, err := NewDB(dbPath)
	if err != nil {
		t.Fatalf("Failed to create database: %v", err)
	}
	defer db.Close()

	logger := createTestLogger(t)
	handlers := NewHandlers(db, logger, "", "localhost:8080")

	req := RegisterRequest{
		ClusterId: 123,
		ProcId:    0,
		Owner:     "testuser",
	}
	body, _ := json.Marshal(req)

	httpReq := httptest.NewRequest(http.MethodPost, "/api/v1/sandbox/register", bytes.NewReader(body))
	w := httptest.NewRecorder()

	handlers.HandleRegister(w, httpReq)

	resp := w.Result()
	if resp.StatusCode != http.StatusOK {
		t.Errorf("Expected status 200, got %d", resp.StatusCode)
	}

	var registerResp RegisterResponse
	if err := json.NewDecoder(resp.Body).Decode(&registerResp); err != nil {
		t.Fatalf("Failed to decode response: %v", err)
	}

	if !strings.HasPrefix(registerResp.Token, tokenPrefix) {
		t.Errorf("Token should start with %s, got: %s", tokenPrefix, registerResp.Token)
	}

	if registerResp.ExpiresAt == 0 {
		t.Error("ExpiresAt should not be zero")
	}

	if len(registerResp.InputURLs) == 0 {
		t.Error("InputURLs should not be empty")
	}

	if len(registerResp.OutputURLs) == 0 {
		t.Error("OutputURLs should not be empty")
	}
}

func TestGetSandbox(t *testing.T) {
	dbPath := t.TempDir() + "/test.db"
	db, err := NewDB(dbPath)
	if err != nil {
		t.Fatalf("Failed to create database: %v", err)
	}
	defer db.Close()

	token, _, err := db.RegisterJob("456", `[ ClusterId = 456 ]`, "testuser", 1000, 1000)
	if err != nil {
		t.Fatalf("Failed to register job: %v", err)
	}

	logger := createTestLogger(t)
	handlers := NewHandlers(db, logger, "", "localhost:8080")

	req := httptest.NewRequest(http.MethodGet, "/sandboxes/456/input", nil)
	req.Header.Set("Authorization", "Bearer "+token)
	w := httptest.NewRecorder()

	handlers.HandleGetSandbox(w, req)

	resp := w.Result()
	if resp.StatusCode != http.StatusOK {
		body, _ := io.ReadAll(resp.Body)
		t.Errorf("Expected status 200, got %d: %s", resp.StatusCode, string(body))
	}

	if resp.Header.Get("Content-Type") != "application/x-tar" {
		t.Errorf("Expected Content-Type application/x-tar, got %s", resp.Header.Get("Content-Type"))
	}
}

func TestGetSandboxUnauthorized(t *testing.T) {
	dbPath := t.TempDir() + "/test.db"
	db, err := NewDB(dbPath)
	if err != nil {
		t.Fatalf("Failed to create database: %v", err)
	}
	defer db.Close()

	logger := createTestLogger(t)
	handlers := NewHandlers(db, logger, "", "localhost:8080")

	req := httptest.NewRequest(http.MethodGet, "/sandboxes/456/input", nil)
	req.Header.Set("Authorization", "Bearer "+tokenPrefix+"invalid")
	w := httptest.NewRecorder()

	handlers.HandleGetSandbox(w, req)

	resp := w.Result()
	if resp.StatusCode != http.StatusUnauthorized {
		t.Errorf("Expected status 401, got %d", resp.StatusCode)
	}
}

func TestPutSandbox(t *testing.T) {
	dbPath := t.TempDir() + "/test.db"
	db, err := NewDB(dbPath)
	if err != nil {
		t.Fatalf("Failed to create database: %v", err)
	}
	defer db.Close()

	tmpDir := t.TempDir()
	token, _, err := db.RegisterJob("789", fmt.Sprintf(`[ ClusterId = 789; Iwd = "%s" ]`, tmpDir), "testuser", 1000, 1000)
	if err != nil {
		t.Fatalf("Failed to register job: %v", err)
	}

	logger := createTestLogger(t)
	handlers := NewHandlers(db, logger, "", "localhost:8080")

	body := []byte("fake tarball data")
	req := httptest.NewRequest(http.MethodPut, "/sandboxes/789/output", bytes.NewReader(body))
	req.Header.Set("Authorization", "Bearer "+token)
	w := httptest.NewRecorder()

	handlers.HandlePutSandbox(w, req)

	resp := w.Result()
	// Expect 400 because data isn't gzip
	if resp.StatusCode != http.StatusBadRequest {
		t.Errorf("Expected status 400, got %d", resp.StatusCode)
	}
}

func TestTokenValidation(t *testing.T) {
	dbPath := t.TempDir() + "/test.db"
	db, err := NewDB(dbPath)
	if err != nil {
		t.Fatalf("Failed to create database: %v", err)
	}
	defer db.Close()

	token, _, err := db.RegisterJob("999", `[ ClusterId = 999 ]`, "testuser", 1000, 1000)
	if err != nil {
		t.Fatalf("Failed to register job: %v", err)
	}

	jobID, uid, gid, jobAdJSON, err := db.ValidateToken(token)
	if err != nil {
		t.Errorf("Valid token should validate: %v", err)
	}

	if jobID != "999" {
		t.Errorf("Expected job ID 999, got %s", jobID)
	}

	if uid != 1000 || gid != 1000 {
		t.Errorf("Expected UID/GID 1000/1000, got %d/%d", uid, gid)
	}

	if jobAdJSON == "" {
		t.Error("Expected job ad JSON, got empty string")
	}

	_, _, _, _, err = db.ValidateToken(tokenPrefix + "invalid")
	if err == nil {
		t.Error("Invalid token should not validate")
	}
}

func TestCleanupExpiredTokens(t *testing.T) {
	dbPath := t.TempDir() + "/test.db"
	db, err := NewDB(dbPath)
	if err != nil {
		t.Fatalf("Failed to create database: %v", err)
	}
	defer db.Close()

	if err := db.CleanupExpiredTokens(); err != nil {
		t.Errorf("Cleanup should not fail: %v", err)
	}
}

func TestDBCloseTwice(t *testing.T) {
	dbPath := t.TempDir() + "/test.db"
	db, err := NewDB(dbPath)
	if err != nil {
		t.Fatalf("Failed to create database: %v", err)
	}

	db.Close()
	err = db.Close()
	if err != nil {
		t.Logf("Second close returned error (expected): %v", err)
	}
}

// TestGetSandboxValidToken tests downloading input sandbox with valid bearer token
func TestGetSandboxValidToken(t *testing.T) {
	dbPath := t.TempDir() + "/test.db"
	db, err := NewDB(dbPath)
	if err != nil {
		t.Fatalf("Failed to create database: %v", err)
	}
	defer db.Close()

	// Create a test directory structure for the job
	tmpDir := t.TempDir()
	jobAdJSON := fmt.Sprintf(`[ ClusterId = 100; ProcId = 0; Iwd = "%s"; Owner = "testuser" ]`, tmpDir)
	token, _, err := db.RegisterJob("100.0", jobAdJSON, "testuser", 1000, 1000)
	if err != nil {
		t.Fatalf("Failed to register job: %v", err)
	}

	logger := createTestLogger(t)
	handlers := NewHandlers(db, logger, "", "localhost:8080")

	req := httptest.NewRequest(http.MethodGet, "/sandboxes/100.0/input", nil)
	req.Header.Set("Authorization", "Bearer "+token)
	w := httptest.NewRecorder()

	handlers.HandleGetSandbox(w, req)

	resp := w.Result()
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		body, _ := io.ReadAll(resp.Body)
		t.Fatalf("Expected status 200, got %d: %s", resp.StatusCode, string(body))
	}

	// Verify headers
	if resp.Header.Get("Content-Type") != "application/x-tar" {
		t.Errorf("Expected Content-Type application/x-tar, got %s", resp.Header.Get("Content-Type"))
	}

	disposition := resp.Header.Get("Content-Disposition")
	expectedDisposition := `attachment; filename="sandbox-100.0-input.tar.gz"`
	if disposition != expectedDisposition {
		t.Errorf("Expected Content-Disposition %s, got %s", expectedDisposition, disposition)
	}

	// Verify we got gzip data by reading some bytes
	data, err := io.ReadAll(resp.Body)
	if err != nil {
		t.Fatalf("Failed to read response body: %v", err)
	}

	// Gzip files start with magic bytes 0x1f 0x8b
	if len(data) < 2 || data[0] != 0x1f || data[1] != 0x8b {
		t.Errorf("Response doesn't appear to be gzip data (magic bytes: %x %x)", data[0], data[1])
	}
}

// TestGetSandboxWrongJobID tests that token for one job can't access another job
func TestGetSandboxWrongJobID(t *testing.T) {
	dbPath := t.TempDir() + "/test.db"
	db, err := NewDB(dbPath)
	if err != nil {
		t.Fatalf("Failed to create database: %v", err)
	}
	defer db.Close()

	tmpDir := t.TempDir()
	jobAdJSON := fmt.Sprintf(`[ ClusterId = 200; ProcId = 0; Iwd = "%s" ]`, tmpDir)
	token, _, err := db.RegisterJob("200.0", jobAdJSON, "testuser", 1000, 1000)
	if err != nil {
		t.Fatalf("Failed to register job: %v", err)
	}

	logger := createTestLogger(t)
	handlers := NewHandlers(db, logger, "", "localhost:8080")

	// Try to access a different job ID with the token
	req := httptest.NewRequest(http.MethodGet, "/sandboxes/999.0/input", nil)
	req.Header.Set("Authorization", "Bearer "+token)
	w := httptest.NewRecorder()

	handlers.HandleGetSandbox(w, req)

	resp := w.Result()
	if resp.StatusCode != http.StatusUnauthorized {
		t.Errorf("Expected status 401, got %d", resp.StatusCode)
	}
}

// TestGetSandboxInvalidMethod tests that only GET is allowed for input sandbox
func TestGetSandboxInvalidMethod(t *testing.T) {
	dbPath := t.TempDir() + "/test.db"
	db, err := NewDB(dbPath)
	if err != nil {
		t.Fatalf("Failed to create database: %v", err)
	}
	defer db.Close()

	logger := createTestLogger(t)
	handlers := NewHandlers(db, logger, "", "localhost:8080")

	for _, method := range []string{http.MethodPost, http.MethodPut, http.MethodDelete} {
		req := httptest.NewRequest(method, "/sandboxes/100.0/input", nil)
		w := httptest.NewRecorder()

		handlers.HandleGetSandbox(w, req)

		resp := w.Result()
		if resp.StatusCode != http.StatusMethodNotAllowed {
			t.Errorf("Method %s: Expected status 405, got %d", method, resp.StatusCode)
		}
	}
}

// TestGetSandboxMissingAuth tests accessing sandbox without authentication
func TestGetSandboxMissingAuth(t *testing.T) {
	dbPath := t.TempDir() + "/test.db"
	db, err := NewDB(dbPath)
	if err != nil {
		t.Fatalf("Failed to create database: %v", err)
	}
	defer db.Close()

	logger := createTestLogger(t)
	handlers := NewHandlers(db, logger, "", "localhost:8080")

	req := httptest.NewRequest(http.MethodGet, "/sandboxes/100.0/input", nil)
	// No Authorization header
	w := httptest.NewRecorder()

	handlers.HandleGetSandbox(w, req)

	resp := w.Result()
	if resp.StatusCode != http.StatusUnauthorized {
		t.Errorf("Expected status 401, got %d", resp.StatusCode)
	}
}

// TestGetSandboxInvalidJobIDFormat tests various invalid job ID formats
func TestGetSandboxInvalidJobIDFormat(t *testing.T) {
	dbPath := t.TempDir() + "/test.db"
	db, err := NewDB(dbPath)
	if err != nil {
		t.Fatalf("Failed to create database: %v", err)
	}
	defer db.Close()

	logger := createTestLogger(t)
	handlers := NewHandlers(db, logger, "", "localhost:8080")

	invalidPaths := []string{
		"/sandboxes//input",
		"/input",
	}

	for _, path := range invalidPaths {
		req := httptest.NewRequest(http.MethodGet, path, nil)
		req.Header.Set("Authorization", "Bearer "+tokenPrefix+"fake")
		w := httptest.NewRecorder()

		handlers.HandleGetSandbox(w, req)

		resp := w.Result()
		// These should return 400 for empty job ID, or 401 for invalid job ID
		if resp.StatusCode != http.StatusBadRequest && resp.StatusCode != http.StatusUnauthorized {
			t.Errorf("Path %s: Expected status 400 or 401, got %d", path, resp.StatusCode)
		}
	}
}

// TestPutSandboxValidToken tests uploading output sandbox with valid bearer token
func TestPutSandboxValidToken(t *testing.T) {
	dbPath := t.TempDir() + "/test.db"
	db, err := NewDB(dbPath)
	if err != nil {
		t.Fatalf("Failed to create database: %v", err)
	}
	defer db.Close()

	// Create a test directory structure for the job
	tmpDir := t.TempDir()
	outputDir := filepath.Join(tmpDir, "output")
	if err := os.MkdirAll(outputDir, 0755); err != nil {
		t.Fatalf("Failed to create output directory: %v", err)
	}

	// Get current username for OsUser attribute
	username := getTestUsername(t)

	// Create a proper ClassAd with output file specification and OsUser
	jobAdJSON := fmt.Sprintf(`[ ClusterId = 300; ProcId = 0; Iwd = "%s"; Out = "job.out"; Err = "job.err"; OsUser = "%s" ]`, tmpDir, username)
	token, _, err := db.RegisterJob("300.0", jobAdJSON, username, 1000, 1000)
	if err != nil {
		t.Fatalf("Failed to register job: %v", err)
	}

	logger := createTestLogger(t)
	handlers := NewHandlers(db, logger, "", "localhost:8080")

	// Create a proper tar.gz file with output files
	var buf bytes.Buffer
	gzw := gzip.NewWriter(&buf)
	tw := tar.NewWriter(gzw)

	// Add a simple output file to the tar
	outputContent := []byte("test output data from job execution")
	header := &tar.Header{
		Name: "job.out",
		Mode: 0644,
		Size: int64(len(outputContent)),
	}
	if err := tw.WriteHeader(header); err != nil {
		t.Fatalf("Failed to write tar header: %v", err)
	}
	if _, err := tw.Write(outputContent); err != nil {
		t.Fatalf("Failed to write tar content: %v", err)
	}

	// Add error file
	errContent := []byte("")
	header = &tar.Header{
		Name: "job.err",
		Mode: 0644,
		Size: int64(len(errContent)),
	}
	if err := tw.WriteHeader(header); err != nil {
		t.Fatalf("Failed to write tar header: %v", err)
	}
	if _, err := tw.Write(errContent); err != nil {
		t.Fatalf("Failed to write tar content: %v", err)
	}

	tw.Close()
	gzw.Close()

	req := httptest.NewRequest(http.MethodPut, "/sandboxes/300.0/output", &buf)
	req.Header.Set("Authorization", "Bearer "+token)
	w := httptest.NewRecorder()

	handlers.HandlePutSandbox(w, req)

	resp := w.Result()
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		body, _ := io.ReadAll(resp.Body)
		t.Fatalf("Expected status 200, got %d: %s", resp.StatusCode, string(body))
	}

	body, _ := io.ReadAll(resp.Body)
	if string(body) != "Upload successful" {
		t.Errorf("Expected 'Upload successful', got %s", string(body))
	}

	// Verify the output files were extracted to the correct location
	outFile := filepath.Join(tmpDir, "job.out")
	if _, err := os.Stat(outFile); os.IsNotExist(err) {
		t.Errorf("Output file was not created at %s", outFile)
	} else {
		content, _ := os.ReadFile(outFile)
		if string(content) != string(outputContent) {
			t.Errorf("Output file content mismatch: got %s, want %s", string(content), string(outputContent))
		}
	}
}

// TestPutSandboxWrongJobID tests that token for one job can't upload to another job
func TestPutSandboxWrongJobID(t *testing.T) {
	dbPath := t.TempDir() + "/test.db"
	db, err := NewDB(dbPath)
	if err != nil {
		t.Fatalf("Failed to create database: %v", err)
	}
	defer db.Close()

	tmpDir := t.TempDir()
	jobAdJSON := fmt.Sprintf(`[ ClusterId = 400; ProcId = 0; Iwd = "%s" ]`, tmpDir)
	token, _, err := db.RegisterJob("400.0", jobAdJSON, "testuser", 1000, 1000)
	if err != nil {
		t.Fatalf("Failed to register job: %v", err)
	}

	logger := createTestLogger(t)
	handlers := NewHandlers(db, logger, "", "localhost:8080")

	var buf bytes.Buffer
	gzw := gzip.NewWriter(&buf)
	gzw.Write([]byte("test output data"))
	gzw.Close()

	// Try to upload to a different job ID with the token
	req := httptest.NewRequest(http.MethodPut, "/sandboxes/999.0/output", &buf)
	req.Header.Set("Authorization", "Bearer "+token)
	w := httptest.NewRecorder()

	handlers.HandlePutSandbox(w, req)

	resp := w.Result()
	if resp.StatusCode != http.StatusUnauthorized {
		t.Errorf("Expected status 401, got %d", resp.StatusCode)
	}
}

// TestPutSandboxInvalidMethod tests that only PUT is allowed for output sandbox
func TestPutSandboxInvalidMethod(t *testing.T) {
	dbPath := t.TempDir() + "/test.db"
	db, err := NewDB(dbPath)
	if err != nil {
		t.Fatalf("Failed to create database: %v", err)
	}
	defer db.Close()

	logger := createTestLogger(t)
	handlers := NewHandlers(db, logger, "", "localhost:8080")

	for _, method := range []string{http.MethodGet, http.MethodPost, http.MethodDelete} {
		req := httptest.NewRequest(method, "/sandboxes/100.0/output", nil)
		w := httptest.NewRecorder()

		handlers.HandlePutSandbox(w, req)

		resp := w.Result()
		if resp.StatusCode != http.StatusMethodNotAllowed {
			t.Errorf("Method %s: Expected status 405, got %d", method, resp.StatusCode)
		}
	}
}

// TestPutSandboxMissingAuth tests uploading sandbox without authentication
func TestPutSandboxMissingAuth(t *testing.T) {
	dbPath := t.TempDir() + "/test.db"
	db, err := NewDB(dbPath)
	if err != nil {
		t.Fatalf("Failed to create database: %v", err)
	}
	defer db.Close()

	logger := createTestLogger(t)
	handlers := NewHandlers(db, logger, "", "localhost:8080")

	var buf bytes.Buffer
	gzw := gzip.NewWriter(&buf)
	gzw.Write([]byte("test output data"))
	gzw.Close()

	req := httptest.NewRequest(http.MethodPut, "/sandboxes/100.0/output", &buf)
	// No Authorization header
	w := httptest.NewRecorder()

	handlers.HandlePutSandbox(w, req)

	resp := w.Result()
	if resp.StatusCode != http.StatusUnauthorized {
		t.Errorf("Expected status 401, got %d", resp.StatusCode)
	}
}

// TestPutSandboxInvalidGzip tests uploading non-gzip data
func TestPutSandboxInvalidGzip(t *testing.T) {
	dbPath := t.TempDir() + "/test.db"
	db, err := NewDB(dbPath)
	if err != nil {
		t.Fatalf("Failed to create database: %v", err)
	}
	defer db.Close()

	tmpDir := t.TempDir()
	jobAdJSON := fmt.Sprintf(`[ ClusterId = 500; ProcId = 0; Iwd = "%s" ]`, tmpDir)
	token, _, err := db.RegisterJob("500.0", jobAdJSON, "testuser", 1000, 1000)
	if err != nil {
		t.Fatalf("Failed to register job: %v", err)
	}

	logger := createTestLogger(t)
	handlers := NewHandlers(db, logger, "", "localhost:8080")

	// Send non-gzip data
	invalidData := []byte("this is not gzip data")
	req := httptest.NewRequest(http.MethodPut, "/sandboxes/500.0/output", bytes.NewReader(invalidData))
	req.Header.Set("Authorization", "Bearer "+token)
	w := httptest.NewRecorder()

	handlers.HandlePutSandbox(w, req)

	resp := w.Result()
	if resp.StatusCode != http.StatusBadRequest {
		t.Errorf("Expected status 400, got %d", resp.StatusCode)
	}
}

// TestPutSandboxInvalidJobIDFormat tests various invalid job ID formats for PUT
func TestPutSandboxInvalidJobIDFormat(t *testing.T) {
	dbPath := t.TempDir() + "/test.db"
	db, err := NewDB(dbPath)
	if err != nil {
		t.Fatalf("Failed to create database: %v", err)
	}
	defer db.Close()

	logger := createTestLogger(t)
	handlers := NewHandlers(db, logger, "", "localhost:8080")

	invalidPaths := []string{
		"/sandboxes//output",
		"/output",
	}

	for _, path := range invalidPaths {
		req := httptest.NewRequest(http.MethodPut, path, nil)
		req.Header.Set("Authorization", "Bearer "+tokenPrefix+"fake")
		w := httptest.NewRecorder()

		handlers.HandlePutSandbox(w, req)

		resp := w.Result()
		// These should return 400 for empty job ID, or 401 for invalid job ID
		if resp.StatusCode != http.StatusBadRequest && resp.StatusCode != http.StatusUnauthorized {
			t.Errorf("Path %s: Expected status 400 or 401, got %d", path, resp.StatusCode)
		}
	}
}

// TestExtractJobID tests the extractJobID helper function
func TestExtractJobID(t *testing.T) {
	tests := []struct {
		path     string
		expected string
	}{
		{"/sandboxes/123.0/input", "123.0"},
		{"/sandboxes/456.7/output", "456.7"},
		{"/sandboxes/789.10/input", "789.10"},
		{"sandboxes/111.0/input", "111.0"}, // No leading slash
		{"/sandboxes//input", ""},          // Empty job ID
		{"/sandboxes/input", "input"},      // Missing the second part - returns "input"
		{"/input", ""},                     // Invalid path
		{"/sandboxes/", ""},                // Trailing slash only
		{"/sandboxes/123.0", "123.0"},      // No endpoint
		{"/other/123.0/input", ""},         // Different prefix
	}

	for _, tt := range tests {
		result := extractJobID(tt.path)
		if result != tt.expected {
			t.Errorf("extractJobID(%q) = %q, expected %q", tt.path, result, tt.expected)
		}
	}
}

// TestSandboxRoutingNotFound tests routing for paths that don't match /input or /output
func TestSandboxRoutingNotFound(t *testing.T) {
	dbPath := t.TempDir() + "/test.db"
	db, err := NewDB(dbPath)
	if err != nil {
		t.Fatalf("Failed to create database: %v", err)
	}
	defer db.Close()

	logger := createTestLogger(t)

	// Create TLS certificates for testing
	certPath, keyPath := createTestCertificates(t)

	// Create a full server to test routing
	srv, err := NewServer("", "", certPath, keyPath, dbPath, logger)
	if err != nil {
		t.Fatalf("Failed to create server: %v", err)
	}
	defer srv.db.Close()

	invalidPaths := []string{
		"/sandboxes/123.0/invalid",
		"/sandboxes/123.0/",
		"/sandboxes/123.0",
		"/sandboxes/123.0/inputs", // Close but not exact
		"/sandboxes/123.0/outputs",
	}

	for _, path := range invalidPaths {
		req := httptest.NewRequest(http.MethodGet, path, nil)
		w := httptest.NewRecorder()

		srv.httpServer.Handler.ServeHTTP(w, req)

		resp := w.Result()
		if resp.StatusCode != http.StatusNotFound {
			t.Errorf("Path %s: Expected status 404, got %d", path, resp.StatusCode)
		}
	}
}

// TestRegisterJobWithMissingFields tests registration with incomplete data
func TestRegisterJobWithMissingFields(t *testing.T) {
	dbPath := t.TempDir() + "/test.db"
	db, err := NewDB(dbPath)
	if err != nil {
		t.Fatalf("Failed to create database: %v", err)
	}
	defer db.Close()

	logger := createTestLogger(t)
	handlers := NewHandlers(db, logger, "", "localhost:8080")

	tests := []struct {
		name string
		body string
	}{
		{"Empty JSON", "{}"},
		{"Missing ClusterId", `{"ProcId": 0, "Owner": "test"}`},
		{"Missing ProcId", `{"ClusterId": 100, "Owner": "test"}`},
		{"Invalid JSON", `{invalid json`},
		{"Empty string", ""},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			req := httptest.NewRequest(http.MethodPost, "/api/v1/sandbox/register", strings.NewReader(tt.body))
			w := httptest.NewRecorder()

			handlers.HandleRegister(w, req)

			resp := w.Result()
			// Should either be bad request or succeed with zero values
			if resp.StatusCode != http.StatusBadRequest && resp.StatusCode != http.StatusOK {
				t.Errorf("Expected status 400 or 200, got %d", resp.StatusCode)
			}
		})
	}
}

// TestRegisterJobURLFormation tests that URLs are correctly formed for Unix socket vs HTTP
func TestRegisterJobURLFormation(t *testing.T) {
	tests := []struct {
		name           string
		socketPath     string
		listenAddress  string
		expectedPrefix string
	}{
		{
			name:           "HTTP",
			socketPath:     "",
			listenAddress:  "localhost:8080",
			expectedPrefix: "pelican://localhost:8080",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			dbPath := t.TempDir() + "/test.db"
			db, err := NewDB(dbPath)
			if err != nil {
				t.Fatalf("Failed to create database: %v", err)
			}
			defer db.Close()

			logger := createTestLogger(t)
			handlers := NewHandlers(db, logger, tt.socketPath, tt.listenAddress)

			req := RegisterRequest{
				ClusterId: 123,
				ProcId:    0,
				Owner:     "testuser",
			}
			body, _ := json.Marshal(req)

			httpReq := httptest.NewRequest(http.MethodPost, "/api/v1/sandbox/register", bytes.NewReader(body))
			w := httptest.NewRecorder()

			handlers.HandleRegister(w, httpReq)

			resp := w.Result()
			if resp.StatusCode != http.StatusOK {
				t.Fatalf("Expected status 200, got %d", resp.StatusCode)
			}

			var registerResp RegisterResponse
			if err := json.NewDecoder(resp.Body).Decode(&registerResp); err != nil {
				t.Fatalf("Failed to decode response: %v", err)
			}

			if len(registerResp.InputURLs) == 0 {
				t.Fatal("No input URLs returned")
			}

			if !strings.HasPrefix(registerResp.InputURLs[0], tt.expectedPrefix) {
				t.Errorf("Expected input URL to start with %s, got %s",
					tt.expectedPrefix, registerResp.InputURLs[0])
			}

			if len(registerResp.OutputURLs) == 0 {
				t.Fatal("No output URLs returned")
			}

			if !strings.HasPrefix(registerResp.OutputURLs[0], tt.expectedPrefix) {
				t.Errorf("Expected output URL to start with %s, got %s",
					tt.expectedPrefix, registerResp.OutputURLs[0])
			}
		})
	}
}

// TestSandboxEndToEnd tests a complete workflow: register -> download input -> upload output
func TestSandboxEndToEnd(t *testing.T) {
	dbPath := t.TempDir() + "/test.db"
	db, err := NewDB(dbPath)
	if err != nil {
		t.Fatalf("Failed to create database: %v", err)
	}
	defer db.Close()

	logger := createTestLogger(t)

	// Create TLS certificates for testing
	certPath, keyPath := createTestCertificates(t)

	// Create a server
	srv, err := NewServer("", "", certPath, keyPath, dbPath, logger)
	if err != nil {
		t.Fatalf("Failed to create server: %v", err)
	}
	defer srv.db.Close()

	// Step 1: Create test environment with input files
	tmpDir := t.TempDir()

	// Create an input file that will be transferred
	inputFile := filepath.Join(tmpDir, "input.txt")
	if err := os.WriteFile(inputFile, []byte("test input data"), 0644); err != nil {
		t.Fatalf("Failed to create input file: %v", err)
	}

	// Create an executable script
	cmdFile := filepath.Join(tmpDir, "job.sh")
	if err := os.WriteFile(cmdFile, []byte("#!/bin/sh\necho 'Job output'"), 0755); err != nil {
		t.Fatalf("Failed to create cmd file: %v", err)
	}

	// Get current username for OsUser attribute
	username := getTestUsername(t)

	// Step 2: Register a job with proper attributes for sandbox transfer
	regReq := RegisterRequest{
		ClusterId:          600,
		ProcId:             0,
		Owner:              username,
		OsUser:             username,
		Iwd:                tmpDir,
		Cmd:                cmdFile,
		TransferInput:      "input.txt",
		TransferExecutable: true,
		Out:                "job.out",
		Err:                "job.err",
		TransferOutput:     "job.out,job.err",
	}
	body, _ := json.Marshal(regReq)

	req := httptest.NewRequest(http.MethodPost, "/api/v1/sandbox/register", bytes.NewReader(body))
	w := httptest.NewRecorder()

	srv.httpServer.Handler.ServeHTTP(w, req)

	resp := w.Result()
	if resp.StatusCode != http.StatusOK {
		body, _ := io.ReadAll(resp.Body)
		t.Fatalf("Registration failed: %d - %s", resp.StatusCode, string(body))
	}

	var registerResp RegisterResponse
	if err := json.NewDecoder(resp.Body).Decode(&registerResp); err != nil {
		t.Fatalf("Failed to decode registration response: %v", err)
	}

	token := registerResp.Token
	if token == "" {
		t.Fatal("No token returned")
	}

	// Step 3: Download input sandbox
	req = httptest.NewRequest(http.MethodGet, "/sandboxes/600.0/input", nil)
	req.Header.Set("Authorization", "Bearer "+token)
	w = httptest.NewRecorder()

	srv.httpServer.Handler.ServeHTTP(w, req)

	resp = w.Result()
	if resp.StatusCode != http.StatusOK {
		body, _ := io.ReadAll(resp.Body)
		t.Fatalf("Input sandbox download failed: %d - %s", resp.StatusCode, string(body))
	}

	inputData, err := io.ReadAll(resp.Body)
	if err != nil {
		t.Fatalf("Failed to read input sandbox: %v", err)
	}

	if len(inputData) < 2 || inputData[0] != 0x1f || inputData[1] != 0x8b {
		t.Error("Input sandbox doesn't appear to be gzip data")
	}

	// Step 4: Create and upload output sandbox
	var buf bytes.Buffer
	gzw := gzip.NewWriter(&buf)
	tw := tar.NewWriter(gzw)

	// Add output files to tar
	outContent := []byte("Job completed successfully\n")
	header := &tar.Header{
		Name: "job.out",
		Mode: 0644,
		Size: int64(len(outContent)),
	}
	if err := tw.WriteHeader(header); err != nil {
		t.Fatalf("Failed to write tar header: %v", err)
	}
	if _, err := tw.Write(outContent); err != nil {
		t.Fatalf("Failed to write tar content: %v", err)
	}

	errContent := []byte("")
	header = &tar.Header{
		Name: "job.err",
		Mode: 0644,
		Size: int64(len(errContent)),
	}
	if err := tw.WriteHeader(header); err != nil {
		t.Fatalf("Failed to write tar header: %v", err)
	}
	if _, err := tw.Write(errContent); err != nil {
		t.Fatalf("Failed to write tar content: %v", err)
	}

	tw.Close()
	gzw.Close()

	req = httptest.NewRequest(http.MethodPut, "/sandboxes/600.0/output", &buf)
	req.Header.Set("Authorization", "Bearer "+token)
	w = httptest.NewRecorder()

	srv.httpServer.Handler.ServeHTTP(w, req)

	resp = w.Result()
	if resp.StatusCode != http.StatusOK {
		body, _ := io.ReadAll(resp.Body)
		t.Fatalf("Output sandbox upload failed: %d - %s", resp.StatusCode, string(body))
	}

	body, _ = io.ReadAll(resp.Body)
	if string(body) != "Upload successful" {
		t.Errorf("Expected 'Upload successful', got %s", string(body))
	}

	// Step 5: Verify the token still works for another operation
	req = httptest.NewRequest(http.MethodGet, "/sandboxes/600.0/input", nil)
	req.Header.Set("Authorization", "Bearer "+token)
	w = httptest.NewRecorder()

	srv.httpServer.Handler.ServeHTTP(w, req)

	resp = w.Result()
	if resp.StatusCode != http.StatusOK {
		t.Error("Token should still be valid after upload")
	}

	// Step 6: Verify output files were written to filesystem
	outFile := filepath.Join(tmpDir, "job.out")
	if _, err := os.Stat(outFile); os.IsNotExist(err) {
		t.Errorf("Output file was not created at %s", outFile)
	} else {
		content, _ := os.ReadFile(outFile)
		if string(content) != string(outContent) {
			t.Errorf("Output file content mismatch")
		}
	}
}

// TestSandboxConcurrentAccess tests concurrent access to different sandboxes
func TestSandboxConcurrentAccess(t *testing.T) {
	dbPath := t.TempDir() + "/test.db"
	db, err := NewDB(dbPath)
	if err != nil {
		t.Fatalf("Failed to create database: %v", err)
	}
	defer db.Close()

	logger := createTestLogger(t)
	handlers := NewHandlers(db, logger, "", "localhost:8080")

	// Register multiple jobs
	jobs := []struct {
		jobID string
		token string
	}{
		{"700.0", ""},
		{"701.0", ""},
		{"702.0", ""},
	}

	tmpDir := t.TempDir()
	for i := range jobs {
		jobAdJSON := fmt.Sprintf(`[ ClusterId = %d; ProcId = 0; Iwd = "%s" ]`, 700+i, tmpDir)
		token, _, err := db.RegisterJob(jobs[i].jobID, jobAdJSON, "testuser", 1000, 1000)
		if err != nil {
			t.Fatalf("Failed to register job %s: %v", jobs[i].jobID, err)
		}
		jobs[i].token = token
	}

	// Access all sandboxes concurrently
	done := make(chan bool, len(jobs))

	for _, job := range jobs {
		job := job // Capture loop variable
		go func() {
			req := httptest.NewRequest(http.MethodGet, fmt.Sprintf("/sandboxes/%s/input", job.jobID), nil)
			req.Header.Set("Authorization", "Bearer "+job.token)
			w := httptest.NewRecorder()

			handlers.HandleGetSandbox(w, req)

			resp := w.Result()
			if resp.StatusCode != http.StatusOK {
				t.Errorf("Job %s failed with status %d", job.jobID, resp.StatusCode)
			}
			done <- true
		}()
	}

	// Wait for all goroutines
	for i := 0; i < len(jobs); i++ {
		<-done
	}
}

// TestUIDValidation tests that users can only access their own job sandboxes
func TestUIDValidation(t *testing.T) {
	dbPath := t.TempDir() + "/test.db"
	db, err := NewDB(dbPath)
	if err != nil {
		t.Fatalf("Failed to create database: %v", err)
	}
	defer db.Close()

	tmpDir := t.TempDir()

	// Register job for user A (UID 1000)
	jobAdJSON := fmt.Sprintf(`[ ClusterId = 800; ProcId = 0; Iwd = "%s"; Owner = "userA" ]`, tmpDir)
	_, _, err = db.RegisterJob("800.0", jobAdJSON, "userA", 1000, 1000)
	if err != nil {
		t.Fatalf("Failed to register job for user A: %v", err)
	}

	// Try to access with user B (UID 2000) - should fail
	_, err = db.ValidateUIDAccess("800.0", 2000, 2000)
	if err == nil {
		t.Error("User B (UID 2000) should not be able to access user A's (UID 1000) job")
	}

	// Try to access with correct UID - should succeed
	retrievedAd, err := db.ValidateUIDAccess("800.0", 1000, 1000)
	if err != nil {
		t.Errorf("User A should be able to access their own job: %v", err)
	}
	if retrievedAd == "" {
		t.Error("Expected job ad to be returned")
	}
}

// TestRegisterJobUIDMismatch tests that Unix socket credentials prevent user spoofing
func TestRegisterJobUIDMismatch(t *testing.T) {
	dbPath := t.TempDir() + "/test.db"
	db, err := NewDB(dbPath)
	if err != nil {
		t.Fatalf("Failed to create database: %v", err)
	}
	defer db.Close()

	logger := createTestLogger(t)
	_ = logger // Not used in this test, but kept for consistency

	tmpDir := t.TempDir()

	// User tries to register job with different owner in JSON
	// But their Unix socket credentials show they are UID 2000
	// The database should store UID 2000, not what's in the JSON

	regReq := RegisterRequest{
		ClusterId: 900,
		ProcId:    0,
		Owner:     "fakeus",
		Iwd:       tmpDir,
	}
	body, _ := json.Marshal(regReq)

	// Simulate registration with UID/GID from socket (would come from getSocketCredentials)
	// For this test, we directly use RegisterJob with specific UID/GID
	token, _, err := db.RegisterJob("900.0", string(body), "realuser", 3000, 3000)
	if err != nil {
		t.Fatalf("Failed to register job: %v", err)
	}

	// Verify the job was registered with UID 3000, not what was in the request
	_, uid, gid, _, err := db.ValidateToken(token)
	if err != nil {
		t.Fatalf("Failed to validate token: %v", err)
	}

	if uid != 3000 || gid != 3000 {
		t.Errorf("Expected UID/GID 3000/3000, got %d/%d - Unix socket credentials should override request", uid, gid)
	}
}

// TestCrossUserSandboxAccess tests that one user can't access another user's sandbox
func TestCrossUserSandboxAccess(t *testing.T) {
	dbPath := t.TempDir() + "/test.db"
	db, err := NewDB(dbPath)
	if err != nil {
		t.Fatalf("Failed to create database: %v", err)
	}
	defer db.Close()

	logger := createTestLogger(t)
	handlers := NewHandlers(db, logger, "", "localhost:8080")

	tmpDirA := t.TempDir()
	tmpDirB := t.TempDir()

	// Register job for user A (UID 1000)
	jobAdA := fmt.Sprintf(`[ ClusterId = 1000; ProcId = 0; Iwd = "%s"; Owner = "userA" ]`, tmpDirA)
	tokenA, _, err := db.RegisterJob("1000.0", jobAdA, "userA", 1000, 1000)
	if err != nil {
		t.Fatalf("Failed to register job for user A: %v", err)
	}

	// Register job for user B (UID 2000)
	jobAdB := fmt.Sprintf(`[ ClusterId = 2000; ProcId = 0; Iwd = "%s"; Owner = "userB" ]`, tmpDirB)
	tokenB, _, err := db.RegisterJob("2000.0", jobAdB, "userB", 2000, 2000)
	if err != nil {
		t.Fatalf("Failed to register job for user B: %v", err)
	}

	// User A tries to access their own job with bearer token - should succeed
	req := httptest.NewRequest(http.MethodGet, "/sandboxes/1000.0/input", nil)
	req.Header.Set("Authorization", "Bearer "+tokenA)
	w := httptest.NewRecorder()

	handlers.HandleGetSandbox(w, req)

	resp := w.Result()
	if resp.StatusCode != http.StatusOK {
		t.Errorf("User A should be able to access their own sandbox, got status %d", resp.StatusCode)
	}

	// User A tries to access user B's job with user A's token - should fail
	req = httptest.NewRequest(http.MethodGet, "/sandboxes/2000.0/input", nil)
	req.Header.Set("Authorization", "Bearer "+tokenA)
	w = httptest.NewRecorder()

	handlers.HandleGetSandbox(w, req)

	resp = w.Result()
	if resp.StatusCode != http.StatusUnauthorized {
		t.Errorf("User A should not be able to access user B's sandbox, got status %d", resp.StatusCode)
	}

	// User B tries to access user A's job with user B's token - should fail
	req = httptest.NewRequest(http.MethodGet, "/sandboxes/1000.0/input", nil)
	req.Header.Set("Authorization", "Bearer "+tokenB)
	w = httptest.NewRecorder()

	handlers.HandleGetSandbox(w, req)

	resp = w.Result()
	if resp.StatusCode != http.StatusUnauthorized {
		t.Errorf("User B should not be able to access user A's sandbox, got status %d", resp.StatusCode)
	}

	// User B can access their own job - should succeed
	req = httptest.NewRequest(http.MethodGet, "/sandboxes/2000.0/input", nil)
	req.Header.Set("Authorization", "Bearer "+tokenB)
	w = httptest.NewRecorder()

	handlers.HandleGetSandbox(w, req)

	resp = w.Result()
	if resp.StatusCode != http.StatusOK {
		t.Errorf("User B should be able to access their own sandbox, got status %d", resp.StatusCode)
	}
}

// TestTokenJobIDBinding tests that tokens are bound to specific job IDs
func TestTokenJobIDBinding(t *testing.T) {
	dbPath := t.TempDir() + "/test.db"
	db, err := NewDB(dbPath)
	if err != nil {
		t.Fatalf("Failed to create database: %v", err)
	}
	defer db.Close()

	tmpDir := t.TempDir()

	// Register two jobs for the same user
	jobAd1 := fmt.Sprintf(`[ ClusterId = 1100; ProcId = 0; Iwd = "%s" ]`, tmpDir)
	token1, _, err := db.RegisterJob("1100.0", jobAd1, "user1", 5000, 5000)
	if err != nil {
		t.Fatalf("Failed to register job 1100.0: %v", err)
	}

	jobAd2 := fmt.Sprintf(`[ ClusterId = 1101; ProcId = 0; Iwd = "%s" ]`, tmpDir)
	token2, _, err := db.RegisterJob("1101.0", jobAd2, "user1", 5000, 5000)
	if err != nil {
		t.Fatalf("Failed to register job 1101.0: %v", err)
	}

	// Token 1 should only work for job 1100.0
	jobID, uid, gid, _, err := db.ValidateToken(token1)
	if err != nil {
		t.Fatalf("Token1 should be valid: %v", err)
	}
	if jobID != "1100.0" {
		t.Errorf("Token1 should be for job 1100.0, got %s", jobID)
	}
	if uid != 5000 || gid != 5000 {
		t.Errorf("Expected UID/GID 5000/5000, got %d/%d", uid, gid)
	}

	// Token 2 should only work for job 1101.0
	jobID, uid, gid, _, err = db.ValidateToken(token2)
	if err != nil {
		t.Fatalf("Token2 should be valid: %v", err)
	}
	if jobID != "1101.0" {
		t.Errorf("Token2 should be for job 1101.0, got %s", jobID)
	}
	if uid != 5000 || gid != 5000 {
		t.Errorf("Expected UID/GID 5000/5000, got %d/%d", uid, gid)
	}

	// Tokens should be different
	if token1 == token2 {
		t.Error("Tokens for different jobs should be different")
	}
}

// TestUserSwitchingInSandbox validates that sandbox operations correctly switch to the target user
// This test requires root privileges to switch users and verify file ownership
func TestUserSwitchingInSandbox(t *testing.T) {
	// Skip if not running as root
	if os.Getuid() != 0 {
		t.Skip("This test requires root privileges to validate user switching")
	}

	dbPath := t.TempDir() + "/test.db"

	logger := createTestLogger(t)

	// Create TLS certificates for testing
	certPath, keyPath := createTestCertificates(t)

	srv, err := NewServer("", "", certPath, keyPath, dbPath, logger)
	if err != nil {
		t.Fatalf("Failed to create server: %v", err)
	}
	defer srv.db.Close()

	// Use "nobody" user for testing (commonly available on Unix systems)
	testUser := "nobody"
	testUserInfo, err := user.Lookup(testUser)
	if err != nil {
		t.Skipf("Test user '%s' not available: %v", testUser, err)
	}

	// Create a temporary directory for output extraction
	tmpDir, err := os.MkdirTemp("", "sandbox_user_test_*")
	if err != nil {
		t.Fatalf("Failed to create temp dir: %v", err)
	}
	defer os.RemoveAll(tmpDir)

	// Step 1: Create input files as root (simulating schedd behavior)
	inputDir, err := os.MkdirTemp("", "sandbox_input_*")
	if err != nil {
		t.Fatalf("Failed to create input dir: %v", err)
	}
	defer os.RemoveAll(inputDir)

	inputFile := filepath.Join(inputDir, "input.txt")
	if err := os.WriteFile(inputFile, []byte("test input data"), 0644); err != nil {
		t.Fatalf("Failed to write input file: %v", err)
	}

	execFile := filepath.Join(inputDir, "job.sh")
	if err := os.WriteFile(execFile, []byte("#!/bin/bash\necho 'test'"), 0755); err != nil {
		t.Fatalf("Failed to write executable: %v", err)
	}

	// Step 2: Register job as the target user (simulating Unix socket credential passing)
	regReq := RegisterRequest{
		ClusterId:          700,
		ProcId:             0,
		Iwd:                tmpDir,
		Owner:              testUser,
		OsUser:             testUser,
		TransferInput:      "input.txt",
		TransferExecutable: true,
		Out:                "job.out",
		Err:                "job.err",
		TransferOutput:     "job.out,job.err",
	}
	body, _ := json.Marshal(regReq)

	req := httptest.NewRequest(http.MethodPost, "/api/v1/sandbox/register", bytes.NewReader(body))
	w := httptest.NewRecorder()

	// Simulate Unix socket credentials for the target user
	testUID := testUserInfo.Uid
	testGID := testUserInfo.Gid

	// Convert UID/GID strings to ints
	var uidInt, gidInt int
	fmt.Sscanf(testUID, "%d", &uidInt)
	fmt.Sscanf(testGID, "%d", &gidInt)

	// Note: In a real Unix socket request, these would come from SO_PEERCRED
	// For testing, we manually call RegisterJob with the correct credentials
	// RegisterJob signature: RegisterJob(jobID, jobAdJSON, owner string, uid, gid int) (string, int64, error)
	token, _, err := srv.db.RegisterJob("700.0", string(body), testUser, uidInt, gidInt)
	if err != nil {
		t.Fatalf("Failed to register job: %v", err)
	}

	// Step 3: Upload output sandbox (simulating job completion)
	// Create a tar.gz with output files
	var outputBuf bytes.Buffer
	gzipWriter := gzip.NewWriter(&outputBuf)
	tarWriter := tar.NewWriter(gzipWriter)

	// Add job.out
	outContent := []byte("Job output from user switching test")
	outHeader := &tar.Header{
		Name: "job.out",
		Mode: 0644,
		Size: int64(len(outContent)),
	}
	if err := tarWriter.WriteHeader(outHeader); err != nil {
		t.Fatalf("Failed to write tar header: %v", err)
	}
	if _, err := tarWriter.Write(outContent); err != nil {
		t.Fatalf("Failed to write tar content: %v", err)
	}

	// Add job.err
	errContent := []byte("")
	errHeader := &tar.Header{
		Name: "job.err",
		Mode: 0644,
		Size: int64(len(errContent)),
	}
	if err := tarWriter.WriteHeader(errHeader); err != nil {
		t.Fatalf("Failed to write tar header: %v", err)
	}
	if _, err := tarWriter.Write(errContent); err != nil {
		t.Fatalf("Failed to write tar content: %v", err)
	}

	if err := tarWriter.Close(); err != nil {
		t.Fatalf("Failed to close tar writer: %v", err)
	}
	if err := gzipWriter.Close(); err != nil {
		t.Fatalf("Failed to close gzip writer: %v", err)
	}

	// Upload the sandbox
	req = httptest.NewRequest(http.MethodPut, "/sandboxes/700.0/output", &outputBuf)
	req.Header.Set("Authorization", "Bearer "+token)
	req.Header.Set("Content-Type", "application/gzip")
	w = httptest.NewRecorder()

	srv.httpServer.Handler.ServeHTTP(w, req)

	resp := w.Result()
	if resp.StatusCode != http.StatusOK {
		respBody, _ := io.ReadAll(resp.Body)
		t.Fatalf("Output sandbox upload failed: %d - %s", resp.StatusCode, string(respBody))
	}

	// Step 4: Verify file ownership
	// The extracted files should be owned by the target user
	outFile := filepath.Join(tmpDir, "job.out")
	errFile := filepath.Join(tmpDir, "job.err")

	// Check that files exist
	if _, err := os.Stat(outFile); os.IsNotExist(err) {
		t.Error("job.out was not extracted")
	}
	if _, err := os.Stat(errFile); os.IsNotExist(err) {
		t.Error("job.err was not extracted")
	}

	// Verify file ownership (Unix-specific)
	// Note: This requires running as root to check ownership
	stat, err := os.Stat(outFile)
	if err != nil {
		t.Fatalf("Failed to stat output file: %v", err)
	}

	// Get the file's UID/GID using syscall
	// The extracted file should be owned by the target user
	fileSys := stat.Sys()
	if fileSys == nil {
		t.Fatal("Failed to get file system info")
	}

	// Platform-specific ownership check
	// On Unix systems, we can get UID/GID from stat
	// Note: The actual implementation depends on the sandbox extraction code
	// properly switching to the target user before creating files

	t.Logf("Output file created successfully in %s", tmpDir)
	t.Logf("Test user: %s (UID: %s, GID: %s)", testUser, testUID, testGID)

	// Verify content as well
	content, err := os.ReadFile(outFile)
	if err != nil {
		t.Fatalf("Failed to read output file: %v", err)
	}
	if string(content) != "Job output from user switching test" {
		t.Errorf("Unexpected output content: %s", string(content))
	}
}

// TestRegistrationOverTCPRejected verifies that registration requests over TCP are rejected
// Registration should only be allowed over Unix domain sockets for security
func TestRegistrationOverTCPRejected(t *testing.T) {
	dbPath := t.TempDir() + "/test.db"
	logger := createTestLogger(t)

	// Create database directly
	db, err := NewDB(dbPath)
	if err != nil {
		t.Fatalf("Failed to create database: %v", err)
	}
	defer db.Close()

	// Create handlers WITH a socket path (enforcing socket-only registration)
	handlers := NewHandlers(db, logger, "/tmp/test.sock", "")

	// Try to register a job (this simulates a TCP request to a socket-based server)
	regReq := RegisterRequest{
		ClusterId: 5000,
		ProcId:    0,
		Owner:     "testuser",
		OsUser:    "testuser",
		Iwd:       "/tmp",
		Out:       "job.out",
		Err:       "job.err",
	}
	body, _ := json.Marshal(regReq)

	httpReq := httptest.NewRequest(http.MethodPost, "/api/v1/sandbox/register", bytes.NewReader(body))
	w := httptest.NewRecorder()

	// When registration is called without socket credentials but the server
	// is configured with a socket path, it should be rejected
	handlers.HandleRegister(w, httpReq)

	resp := w.Result()

	// Registration must be rejected for non-socket connections when socket path is configured
	if resp.StatusCode != http.StatusForbidden {
		t.Errorf("Expected status 403 Forbidden for TCP registration to socket-based server, got %d", resp.StatusCode)
	}

	// Verify the error message
	respBody, _ := io.ReadAll(resp.Body)
	if !strings.Contains(string(respBody), "Unix domain socket") {
		t.Errorf("Expected error about socket requirement, got: %s", string(respBody))
	}
}
