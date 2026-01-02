// +build linux

package webserver

import (
	"net"
	"net/http"
	"syscall"
)

func getSocketCredentials(r *http.Request) (int, int) {
	// Try to get credentials from Unix socket
	if conn, ok := r.Context().Value(http.LocalAddrContextKey).(net.Conn); ok {
		if unixConn, ok := conn.(*net.UnixConn); ok {
			file, err := unixConn.File()
			if err == nil {
				defer file.Close()

				cred, err := syscall.GetsockoptUcred(int(file.Fd()), syscall.SOL_SOCKET, syscall.SO_PEERCRED)
				if err == nil {
					return int(cred.Uid), int(cred.Gid)
				}
			}
		}
	}
	return -1, -1
}
