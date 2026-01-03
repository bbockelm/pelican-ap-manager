package webserver

import (
	"crypto/rand"
	"crypto/rsa"
	"crypto/tls"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/pem"
	"fmt"
	"math/big"
	"net"
	"os"
	"time"
)

// GenerateTestCertificate generates a CA certificate and a host certificate signed by the CA
// This creates a proper certificate chain as required by HTCondor
// Returns caPaths (ca cert, ca key), hostPaths (host cert, host key), error
// The cert file contains the host cert followed by the CA cert for the chain
func GenerateTestCertificate(certPath, keyPath, hostname string) error {
	// Generate CA private key
	caPrivateKey, err := rsa.GenerateKey(rand.Reader, 2048)
	if err != nil {
		return fmt.Errorf("failed to generate CA private key: %w", err)
	}

	// Create CA certificate template with CA constraint set to true
	caSerialNumber, err := rand.Int(rand.Reader, new(big.Int).Lsh(big.NewInt(1), 128))
	if err != nil {
		return fmt.Errorf("failed to generate CA serial number: %w", err)
	}

	caTemplate := x509.Certificate{
		SerialNumber: caSerialNumber,
		Subject: pkix.Name{
			Organization: []string{"HTCondor Test CA"},
			CommonName:   "HTCondor Test CA",
		},
		NotBefore: time.Now(),
		NotAfter:  time.Now().Add(24 * time.Hour),

		KeyUsage: x509.KeyUsageCertSign | x509.KeyUsageCRLSign,
		// CA constraint MUST be true for CA certificate
		BasicConstraintsValid: true,
		IsCA:                  true,
	}

	// Create CA certificate (self-signed)
	caCertBytes, err := x509.CreateCertificate(rand.Reader, &caTemplate, &caTemplate, &caPrivateKey.PublicKey, caPrivateKey)
	if err != nil {
		return fmt.Errorf("failed to create CA certificate: %w", err)
	}

	// Generate host private key
	hostPrivateKey, err := rsa.GenerateKey(rand.Reader, 2048)
	if err != nil {
		return fmt.Errorf("failed to generate host private key: %w", err)
	}

	// Create host certificate template with CA constraint set to false
	hostSerialNumber, err := rand.Int(rand.Reader, new(big.Int).Lsh(big.NewInt(1), 128))
	if err != nil {
		return fmt.Errorf("failed to generate host serial number: %w", err)
	}

	hostTemplate := x509.Certificate{
		SerialNumber: hostSerialNumber,
		Subject: pkix.Name{
			Organization: []string{"HTCondor Test"},
			CommonName:   hostname,
		},
		NotBefore: time.Now(),
		NotAfter:  time.Now().Add(24 * time.Hour),

		KeyUsage: x509.KeyUsageKeyEncipherment | x509.KeyUsageDigitalSignature,
		// Server Auth must be set for host certificate
		ExtKeyUsage: []x509.ExtKeyUsage{x509.ExtKeyUsageServerAuth},
		// CA constraint MUST be false for host certificate
		BasicConstraintsValid: true,
		IsCA:                  false,

		// Add both hostname and localhost for testing
		DNSNames: []string{hostname, "localhost", "127.0.0.1"},
		IPAddresses: []net.IP{
			net.ParseIP("127.0.0.1"),
			net.ParseIP("::1"),
		},
	}

	// Create host certificate signed by CA
	hostCertBytes, err := x509.CreateCertificate(rand.Reader, &hostTemplate, &caTemplate, &hostPrivateKey.PublicKey, caPrivateKey)
	if err != nil {
		return fmt.Errorf("failed to create host certificate: %w", err)
	}

	// Write certificate chain to file (host cert first, then CA cert)
	certFile, err := os.Create(certPath)
	if err != nil {
		return fmt.Errorf("failed to create cert file: %w", err)
	}
	defer certFile.Close()

	// Write host certificate
	if err := pem.Encode(certFile, &pem.Block{
		Type:  "CERTIFICATE",
		Bytes: hostCertBytes,
	}); err != nil {
		return fmt.Errorf("failed to write host certificate: %w", err)
	}

	// Write CA certificate to complete the chain
	if err := pem.Encode(certFile, &pem.Block{
		Type:  "CERTIFICATE",
		Bytes: caCertBytes,
	}); err != nil {
		return fmt.Errorf("failed to write CA certificate: %w", err)
	}

	// Write host private key to file
	keyFile, err := os.Create(keyPath)
	if err != nil {
		return fmt.Errorf("failed to create key file: %w", err)
	}
	defer keyFile.Close()

	hostPrivateKeyBytes, err := x509.MarshalPKCS8PrivateKey(hostPrivateKey)
	if err != nil {
		return fmt.Errorf("failed to marshal host private key: %w", err)
	}

	if err := pem.Encode(keyFile, &pem.Block{
		Type:  "PRIVATE KEY",
		Bytes: hostPrivateKeyBytes,
	}); err != nil {
		return fmt.Errorf("failed to write host private key: %w", err)
	}

	return nil
}

// LoadCertificate loads a TLS certificate and key from files
func LoadCertificate(certPath, keyPath string) (tls.Certificate, error) {
	return tls.LoadX509KeyPair(certPath, keyPath)
}

// LoadCACertificate loads a CA certificate and returns the certificate pool
func LoadCACertificate(certPath string) (*x509.CertPool, error) {
	certData, err := os.ReadFile(certPath)
	if err != nil {
		return nil, fmt.Errorf("failed to read CA certificate: %w", err)
	}

	caCertPool := x509.NewCertPool()
	if !caCertPool.AppendCertsFromPEM(certData) {
		return nil, fmt.Errorf("failed to parse CA certificate")
	}

	return caCertPool, nil
}
