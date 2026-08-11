//go:build linux

package webserver

import (
	"crypto/tls"
	"net"
	"net/http"
	"syscall"

	htcondorlogging "github.com/bbockelm/golang-htcondor/logging"
)

func getSocketCredentials(r *http.Request, logger *htcondorlogging.Logger) (int, int) {
	// Try to get credentials from Unix socket
	conn, ok := r.Context().Value(connContextKey).(net.Conn)
	if !ok {
		logger.Warnf(htcondorlogging.DestinationGeneral, "Connection not found in context")
		return -1, -1
	}

	logger.Infof(htcondorlogging.DestinationGeneral, "Got connection from context, type=%T", conn)

	// If it's a TLS connection, unwrap it to get the underlying connection
	if tlsConn, ok := conn.(*tls.Conn); ok {
		logger.Infof(htcondorlogging.DestinationGeneral, "Connection is TLS, unwrapping...")
		conn = tlsConn.NetConn()
		logger.Infof(htcondorlogging.DestinationGeneral, "After unwrap, type=%T", conn)
	}

	unixConn, ok := conn.(*net.UnixConn)
	if !ok {
		logger.Warnf(htcondorlogging.DestinationGeneral, "Connection is not UnixConn, type=%T", conn)
		return -1, -1
	}

	logger.Infof(htcondorlogging.DestinationGeneral, "Got UnixConn, extracting credentials")

	file, err := unixConn.File()
	if err != nil {
		logger.Errorf(htcondorlogging.DestinationGeneral, "Failed to get file: %v", err)
		return -1, -1
	}
	defer file.Close()

	cred, err := syscall.GetsockoptUcred(int(file.Fd()), syscall.SOL_SOCKET, syscall.SO_PEERCRED)
	if err != nil {
		logger.Errorf(htcondorlogging.DestinationGeneral, "Failed to get credentials: %v", err)
		return -1, -1
	}

	logger.Infof(htcondorlogging.DestinationGeneral, "Got credentials: UID=%d, GID=%d", cred.Uid, cred.Gid)
	return int(cred.Uid), int(cred.Gid)
}
