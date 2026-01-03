//go:build darwin
// +build darwin

package webserver

import (
	"net"
	"net/http"
)

// On macOS, use LOCAL_PEERCRED instead of SO_PEERCRED
func getSocketCredentials(r *http.Request) (int, int) {
	if conn, ok := r.Context().Value(http.LocalAddrContextKey).(net.Conn); ok {
		if unixConn, ok := conn.(*net.UnixConn); ok {
			file, err := unixConn.File()
			if err == nil {
				defer file.Close()

				// macOS uses LOCAL_PEERCRED with Xucred structure
				// This is more complex, so for now return -1, -1
				// A full implementation would use syscall.GetsockoptXucred
				_ = file
			}
		}
	}
	return -1, -1
}
