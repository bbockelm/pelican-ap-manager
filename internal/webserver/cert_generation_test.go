package webserver

import (
	"os"
	"path/filepath"
	"testing"
)

// TestAutomaticCertificateGeneration verifies that certificates are automatically generated
// when they don't exist and the server is started
func TestAutomaticCertificateGeneration(t *testing.T) {
	tmpDir := t.TempDir()
	certPath := filepath.Join(tmpDir, "certs", "server.crt")
	keyPath := filepath.Join(tmpDir, "certs", "server.key")

	// Create a server with paths to non-existent certificates
	logger := createTestLogger(t)
	dbPath := filepath.Join(tmpDir, "test.db")

	srv, err := NewServer("localhost:8443", "", certPath, keyPath, dbPath, logger)
	if err != nil {
		t.Fatalf("Failed to create server: %v", err)
	}
	defer srv.db.Close()

	// Certificates should not exist yet
	if _, err := os.Stat(certPath); err == nil {
		t.Fatal("Certificates should not exist before ensureCertificates()")
	}

	// Call ensureCertificates to generate them
	if err := srv.ensureCertificates(); err != nil {
		t.Fatalf("Failed to ensure certificates: %v", err)
	}

	// Now certificates should exist
	if _, err := os.Stat(certPath); err != nil {
		t.Fatalf("Certificate file should exist after ensureCertificates(): %v", err)
	}

	if _, err := os.Stat(keyPath); err != nil {
		t.Fatalf("Key file should exist after ensureCertificates(): %v", err)
	}

	// Verify we can load the generated certificates
	if _, err := LoadCertificate(certPath, keyPath); err != nil {
		t.Fatalf("Failed to load generated certificate: %v", err)
	}

	// Calling ensureCertificates again should not fail and should not regenerate
	if err := srv.ensureCertificates(); err != nil {
		t.Fatalf("Failed on second ensureCertificates call: %v", err)
	}

	t.Logf("✓ Automatic certificate generation works correctly")
	t.Logf("✓ Certificates generated in: %s", filepath.Dir(certPath))
}

// TestCertificateDirectoryCreation verifies that the certificate directory is created
func TestCertificateDirectoryCreation(t *testing.T) {
	tmpDir := t.TempDir()
	certPath := filepath.Join(tmpDir, "deep", "nested", "path", "certs", "server.crt")
	keyPath := filepath.Join(tmpDir, "deep", "nested", "path", "certs", "server.key")

	logger := createTestLogger(t)
	dbPath := filepath.Join(tmpDir, "test.db")

	srv, err := NewServer("localhost:8443", "", certPath, keyPath, dbPath, logger)
	if err != nil {
		t.Fatalf("Failed to create server: %v", err)
	}
	defer srv.db.Close()

	// Directory should not exist
	certDir := filepath.Dir(certPath)
	if _, err := os.Stat(certDir); err == nil {
		t.Fatal("Certificate directory should not exist before ensureCertificates()")
	}

	// Call ensureCertificates to generate them and create directory
	if err := srv.ensureCertificates(); err != nil {
		t.Fatalf("Failed to ensure certificates: %v", err)
	}

	// Directory should now exist
	if _, err := os.Stat(certDir); err != nil {
		t.Fatalf("Certificate directory should exist after ensureCertificates(): %v", err)
	}

	// Certificates should exist
	if _, err := os.Stat(certPath); err != nil {
		t.Fatalf("Certificate file should exist: %v", err)
	}

	t.Logf("✓ Certificate directory created successfully at: %s", certDir)
}
