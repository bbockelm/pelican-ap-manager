package webserver

import (
	"fmt"
	"os"
	"strings"

	condorconfig "github.com/bbockelm/golang-htcondor/config"
)

// Configuration macros for the web server. They keep the PELICAN_MANAGER_
// prefix they have always had, so an existing configuration works unchanged
// whether the server runs inside pelican_man or as the standalone pelican_web
// daemon.
const (
	macroListenAddress = "PELICAN_MANAGER_WEB_LISTEN_ADDRESS"
	macroSocketPath    = "PELICAN_REGISTRATION_SOCKET"
	macroTLSCert       = "PELICAN_MANAGER_WEB_TLS_CERT"
	macroTLSKey        = "PELICAN_MANAGER_WEB_TLS_KEY"
	macroDBPath        = "PELICAN_MANAGER_WEB_DB_PATH"
)

// LoadConfig derives the web server's settings from the HTCondor
// configuration. The caller supplies the Logger.
//
// The macros keep the PELICAN_MANAGER_ prefix they have always had even though
// the server is now its own daemon, so an existing configuration keeps working
// across the split.
func LoadConfig(condorCfg *condorconfig.Config) (*ServerConfig, error) {
	if condorCfg == nil {
		return nil, fmt.Errorf("webserver: nil HTCondor configuration")
	}

	spoolDir := macroValue(condorCfg, "SPOOL")
	if spoolDir == "" {
		spoolDir = "./data"
	}
	logDir := macroValue(condorCfg, "LOG")
	if logDir == "" {
		logDir = "./log"
	}

	cfg := &ServerConfig{
		SocketPath:     fmt.Sprintf("%s/pelican_manager.sock", spoolDir),
		TLSCert:        fmt.Sprintf("%s/pelican-certs/server.crt", spoolDir),
		TLSKey:         fmt.Sprintf("%s/pelican-certs/server.key", spoolDir),
		DBPath:         fmt.Sprintf("%s/pelican_web.db", spoolDir),
		HTCondorConfig: condorCfg,
	}

	if v := macroValue(condorCfg, macroListenAddress); v != "" {
		cfg.ListenAddress = v
	}
	if v := macroValue(condorCfg, macroSocketPath); v != "" {
		cfg.SocketPath = v
	}
	if v := macroValue(condorCfg, macroTLSCert); v != "" {
		cfg.TLSCert = v
	}
	if v := macroValue(condorCfg, macroTLSKey); v != "" {
		cfg.TLSKey = v
	}
	if v := macroValue(condorCfg, macroDBPath); v != "" {
		cfg.DBPath = v
	}

	// The schedd address feeds the golang-htcondor HTTP API handler mounted at
	// /api/. Configured explicitly, or discovered from the schedd's address
	// file the way condor tools do.
	if scheddHost := macroValue(condorCfg, "SCHEDD_HOST"); scheddHost != "" {
		scheddPort := macroValue(condorCfg, "SCHEDD_PORT")
		if scheddPort == "" {
			scheddPort = "9618"
		}
		cfg.ScheddAddr = fmt.Sprintf("%s:%s", scheddHost, scheddPort)
	} else if addr := readScheddAddressFile(logDir); addr != "" {
		cfg.ScheddAddr = addr
	}

	return cfg, nil
}

// Configured reports whether the server has somewhere to listen. With neither a
// socket path nor a listen address there is nothing to serve.
func (c *ServerConfig) Configured() bool {
	return c != nil && (c.ListenAddress != "" || c.SocketPath != "")
}

func macroValue(cfg *condorconfig.Config, name string) string {
	if v, ok := cfg.Get(name); ok {
		return v
	}
	return ""
}

// readScheddAddressFile reads the schedd's sinful address from
// LOG/.schedd_address, returning just the host:port.
func readScheddAddressFile(logDir string) string {
	if logDir == "" {
		return ""
	}

	data, err := os.ReadFile(fmt.Sprintf("%s/.schedd_address", logDir))
	if err != nil {
		return ""
	}

	for _, line := range strings.Split(string(data), "\n") {
		line = strings.TrimSpace(line)
		if line == "" || strings.Contains(line, "(null)") {
			continue
		}
		if strings.HasPrefix(line, "<") {
			if idx := strings.Index(line, "?"); idx > 0 {
				return line[1:idx]
			}
			if idx := strings.Index(line, ">"); idx > 0 {
				return line[1:idx]
			}
		}
	}

	return ""
}
