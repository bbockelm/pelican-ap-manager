//go:build darwin

package webserver

import (
	"crypto/tls"
	"net"
	"net/http"

	htcondorlogging "github.com/bbockelm/golang-htcondor/logging"
	"golang.org/x/sys/unix"
)

// On macOS, use LOCAL_PEERCRED instead of SO_PEERCRED
func getSocketCredentials(r *http.Request, logger *htcondorlogging.Logger) (int, int) {
	// connContextKey is where Server.ConnContext stashes the accepted
	// connection. http.LocalAddrContextKey holds a net.Addr, not a net.Conn, so
	// reading it here always failed the type assertion -- which made every
	// sandbox registration over the socket look like a non-socket connection and
	// get a 403.
	conn, ok := r.Context().Value(connContextKey).(net.Conn)
	if !ok {
		return -1, -1
	}

	// If it's a TLS connection, unwrap it to get the underlying connection
	if tlsConn, ok := conn.(*tls.Conn); ok {
		conn = tlsConn.NetConn()
	}

	if unixConn, ok := conn.(*net.UnixConn); ok {
		file, err := unixConn.File()
		if err == nil {
			defer file.Close()

			// macOS uses LOCAL_PEERCRED with the Xucred structure, at level
			// SOL_LOCAL -- not SOL_SOCKET, which is where Linux keeps
			// SO_PEERCRED. Asking at the wrong level returns ENOPROTOOPT, so
			// every peer looked unauthenticated.
			cred, err := unix.GetsockoptXucred(int(file.Fd()), unix.SOL_LOCAL, unix.LOCAL_PEERCRED)
			if err == nil {
				if cred.Ngroups > 0 {
					return int(cred.Uid), int(cred.Groups[0])
				}
				// A peer with no supplementary groups still has a uid; the gid
				// is simply unknown.
				return int(cred.Uid), -1
			}
			logger.Debugf(htcondorlogging.DestinationGeneral, "LOCAL_PEERCRED failed on the sandbox socket: %v", err)
		}
	}
	return -1, -1
}
