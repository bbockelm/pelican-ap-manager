package webserver

import (
	"crypto/x509"
	"encoding/pem"
	"os"
	"path/filepath"
	"testing"
)

// TestCertificateChainStructure verifies that the generated certificates form a proper chain
// as required by HTCondor with CA constraint and Server Auth constraints
func TestCertificateChainStructure(t *testing.T) {
	tmpDir := t.TempDir()
	certPath := filepath.Join(tmpDir, "test.crt")
	keyPath := filepath.Join(tmpDir, "test.key")

	// Generate certificate chain
	if err := GenerateTestCertificate(certPath, keyPath, "localhost"); err != nil {
		t.Fatalf("Failed to generate certificate: %v", err)
	}

	// Read and parse certificate file
	certData, err := os.ReadFile(certPath)
	if err != nil {
		t.Fatalf("Failed to read cert file: %v", err)
	}

	var certs []*x509.Certificate
	rest := certData
	for len(rest) > 0 {
		var block *pem.Block
		block, rest = pem.Decode(rest)
		if block == nil {
			break
		}

		if block.Type == "CERTIFICATE" {
			cert, err := x509.ParseCertificate(block.Bytes)
			if err != nil {
				t.Fatalf("Failed to parse certificate: %v", err)
			}
			certs = append(certs, cert)
		}
	}

	if len(certs) != 2 {
		t.Errorf("Expected 2 certificates (host + CA), got %d", len(certs))
	}

	// First cert should be the host certificate
	hostCert := certs[0]

	// Host certificate must NOT be a CA
	if hostCert.IsCA {
		t.Error("Host certificate should have IsCA=false")
	}

	// Host certificate must have Server Auth
	hasServerAuth := false
	for _, eku := range hostCert.ExtKeyUsage {
		if eku == x509.ExtKeyUsageServerAuth {
			hasServerAuth = true
			break
		}
	}
	if !hasServerAuth {
		t.Error("Host certificate must have ServerAuth ExtKeyUsage")
	}

	// Host certificate must have BasicConstraints set to false
	if !hostCert.BasicConstraintsValid {
		t.Error("Host certificate must have BasicConstraintsValid=true")
	}

	t.Logf("✓ Host Certificate: CN=%s, IsCA=%v, ServerAuth=true", hostCert.Subject.CommonName, hostCert.IsCA)

	// Second cert should be the CA certificate
	caCert := certs[1]

	// CA certificate MUST be a CA
	if !caCert.IsCA {
		t.Error("CA certificate should have IsCA=true")
	}

	// CA certificate must have BasicConstraints set to true
	if !caCert.BasicConstraintsValid {
		t.Error("CA certificate must have BasicConstraintsValid=true")
	}

	// Verify the host cert is signed by the CA
	if err := hostCert.CheckSignatureFrom(caCert); err != nil {
		t.Errorf("Host certificate signature verification failed: %v", err)
	}

	t.Logf("✓ CA Certificate: CN=%s, IsCA=%v", caCert.Subject.CommonName, caCert.IsCA)
	t.Logf("✓ Certificate chain is properly structured for HTCondor")
}
