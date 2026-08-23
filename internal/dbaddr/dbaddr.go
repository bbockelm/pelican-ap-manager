// Package dbaddr resolves the configured htcondordb address into one that can
// actually be dialed.
//
// A literal address cannot be written into a static configuration when
// htcondordb runs under condor_master. Behind the shared port its command
// address carries a socket name derived from the daemon's pid
// (<subsys>_<pid>_<rand>), so the address changes every time htcondordb
// restarts. That is why htcondordb publishes an address file and why every
// client it ships -- the CLI, the Python driver -- resolves through
// locate.Daemon rather than taking a host and port.
//
// This is the same resolution for pelican-man, so the two agree on where the
// database is, and so a configuration written once keeps working.
package dbaddr

import (
	"fmt"
	"strings"

	htcondor "github.com/bbockelm/golang-htcondor"
	"github.com/bbockelm/golang-htcondor/config"
	"github.com/bbockelm/htcondordb/locate"
)

// Auto is the configured value that means "find the local htcondordb the way
// its own clients do": HTCONDORDB_ADDRESS_FILE or HTCONDORDB_HOST from the
// environment, else the address file the local daemon publishes.
const Auto = "auto"

// Resolve turns a configured address into a dialable one.
//
//	""            -> "", meaning the caller's feature is not configured
//	"auto"        -> whatever locate.Daemon finds
//	"/path/file"  -> the address published in that address file
//	anything else -> returned unchanged (a sinful string or host:port)
//
// Call this per connection attempt, not once at startup: resolving again is how
// a client picks up an htcondordb that has restarted under a new socket name.
func Resolve(value string, cfg *config.Config) (string, error) {
	value = strings.TrimSpace(value)
	switch {
	case value == "":
		return "", nil

	case strings.EqualFold(value, Auto):
		if cfg == nil {
			return "", fmt.Errorf("dbaddr: %q needs an HTCondor configuration to locate htcondordb", Auto)
		}
		addr, err := locate.Daemon(cfg)
		if err != nil {
			return "", fmt.Errorf("dbaddr: locating the local htcondordb: %w", err)
		}
		return addr, nil

	case strings.HasPrefix(value, "/"):
		addr, err := htcondor.ReadAddressFile(value)
		if err != nil {
			return "", fmt.Errorf("dbaddr: reading the htcondordb address from %s: %w", value, err)
		}
		return addr, nil

	default:
		return value, nil
	}
}

// IsConfigured reports whether a configured value asks for htcondordb at all,
// without trying to reach it. Callers use this to decide whether to build a
// database-backed component; Resolve then does the work that can fail.
func IsConfigured(value string) bool {
	return strings.TrimSpace(value) != ""
}
