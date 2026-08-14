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
	conn, ok := r.Context().Value(http.LocalAddrContextKey).(net.Conn)
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

			// macOS uses LOCAL_PEERCRED with Xucred structure
			cred, err := unix.GetsockoptXucred(int(file.Fd()), unix.SOL_SOCKET, unix.LOCAL_PEERCRED)
			if err == nil {
				if cred.Ngroups > 0 {
					return int(cred.Uid), int(cred.Groups[0])
				}
				return -1, -1
			}
		}
	}
	return -1, -1
}
