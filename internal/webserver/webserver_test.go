package webserver

import (
	"bytes"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	condorconfig "github.com/bbockelm/golang-htcondor/config"
	htcondorlogging "github.com/bbockelm/golang-htcondor/logging"
)

func TestRegisterJob(t *testing.T) {
	dbPath := t.TempDir() + "/test.db"
	db, err := NewDB(dbPath)
	if err != nil {
		t.Fatalf("Failed to create database: %v", err)
	}
	defer db.Close()

	condorCfg, _ := condorconfig.New()
	logger, err := htcondorlogging.FromConfigWithDaemon("TEST", condorCfg)
	if err != nil || logger == nil {
		t.Skip("Logger initialization failed, skipping test")
	}
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

	token, _, err := db.RegisterJob("456", `{"ClusterId": 456}`, "testuser", 1000, 1000)
	if err != nil {
		t.Fatalf("Failed to register job: %v", err)
	}

	condorCfg, _ := condorconfig.New()
	logger, err := htcondorlogging.FromConfigWithDaemon("TEST", condorCfg)
	if err != nil || logger == nil {
		t.Skip("Logger initialization failed, skipping test")
	}
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

	condorCfg, _ := condorconfig.New()
	logger, err := htcondorlogging.FromConfigWithDaemon("TEST", condorCfg)
	if err != nil || logger == nil {
		t.Skip("Logger initialization failed, skipping test")
	}
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

	token, _, err := db.RegisterJob("789", `{"ClusterId": 789}`, "testuser", 1000, 1000)
	if err != nil {
		t.Fatalf("Failed to register job: %v", err)
	}

	condorCfg, _ := condorconfig.New()
	logger, err := htcondorlogging.FromConfigWithDaemon("TEST", condorCfg)
	if err != nil || logger == nil {
		t.Skip("Logger initialization failed, skipping test")
	}
	handlers := NewHandlers(db, logger, "", "localhost:8080")

	body := []byte("fake tarball data")
	req := httptest.NewRequest(http.MethodPut, "/sandboxes/789/output", bytes.NewReader(body))
	req.Header.Set("Authorization", "Bearer "+token)
	w := httptest.NewRecorder()

	handlers.HandlePutSandbox(w, req)

	resp := w.Result()
	if resp.StatusCode != http.StatusOK {
		t.Errorf("Expected status 200, got %d", resp.StatusCode)
	}
}

func TestTokenValidation(t *testing.T) {
	dbPath := t.TempDir() + "/test.db"
	db, err := NewDB(dbPath)
	if err != nil {
		t.Fatalf("Failed to create database: %v", err)
	}
	defer db.Close()

	token, _, err := db.RegisterJob("999", `{"ClusterId": 999}`, "testuser", 1000, 1000)
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
