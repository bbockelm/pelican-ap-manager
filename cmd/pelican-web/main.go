// Command pelican-web serves the Pelican AP manager's HTTP surface: the
// sandbox registration/upload/download API used by the Pelican transfer plugin,
// and the golang-htcondor REST API mounted at /api/.
//
// This is the only place the HTTP surface runs: pelican-man serves none of it,
// which is what keeps the web stack (OAuth2/OIDC, OpenTelemetry, sqlite) out of
// that binary entirely. PELICAN_WEB must be in DAEMON_LIST for Pelican transfer
// plugins to be able to register sandboxes.
//
// Like pelican-man, the daemon lifecycle (configuration, HTCondor logging,
// privilege drop, condor_master readiness/keepalive, SIGHUP reconfigure,
// shared-port command socket) comes from the golang-htcondor daemon framework.
package main

import (
	"context"
	"flag"
	"fmt"
	"net"
	"os"
	"path/filepath"
	"strings"
	"sync"

	"github.com/bbockelm/cedar/commands"
	cedarserver "github.com/bbockelm/cedar/server"
	htcondor "github.com/bbockelm/golang-htcondor"
	condorconfig "github.com/bbockelm/golang-htcondor/config"
	condordaemon "github.com/bbockelm/golang-htcondor/daemon"
	"github.com/bbockelm/golang-htcondor/droppriv"
	htcondorlogging "github.com/bbockelm/golang-htcondor/logging"
	"github.com/bbockelm/pelican-ap-manager/internal/webserver"
)

// subsystem is the HTCondor subsystem name. It selects the per-daemon log knobs
// (PELICAN_WEB_LOG, MAX_PELICAN_WEB_LOG, PELICAN_WEB_DEBUG) and the
// PELICAN_WEB.<key> configuration scope, so the web server's logging and
// security can be tuned separately from pelican-man's.
const subsystem = "PELICAN_WEB"

// version is stamped at build time (see the Makefile).
var version = "dev"

func main() {
	if err := run(); err != nil {
		fmt.Fprintln(os.Stderr, "pelican-web:", err)
		os.Exit(1)
	}
}

func run() error {
	showVersion := flag.Bool("version", false, "print the version and exit")
	listen := flag.String("listen", "127.0.0.1:0", "command-socket bind address when not running under condor_master")
	// condor_master passes these to every daemon it starts; flag.Parse would
	// reject them otherwise. -local-name additionally scopes config lookups.
	localName := flag.String("local-name", "", "HTCondor subsystem local-name; passed by condor_master")
	_ = flag.String("sock", "", "HTCondor shared-port endpoint name; accepted for compatibility (fd inherited via CONDOR_INHERIT)")
	flag.Parse()

	if *showVersion {
		fmt.Println("pelican-web", version)
		return nil
	}

	condorCfg, err := condorconfig.NewWithOptions(condorconfig.ConfigOptions{
		Subsystem: subsystem,
		LocalName: *localName,
	})
	if err != nil {
		return fmt.Errorf("loading config: %w", err)
	}

	d, err := condordaemon.New(condordaemon.Options{
		Subsys:    subsystem,
		LocalName: *localName,
		Config:    condorCfg,
	})
	if err != nil {
		return err
	}
	log := d.Logger()

	// Rebuild the process-wide droppriv singleton after the drop, so the
	// sandbox handlers -- which read and write files as the job's owner -- see
	// the post-drop identity as their baseline.
	droppriv.ReloadDefaultManager()

	webCfg, err := webserver.LoadConfig(d.Config())
	if err != nil {
		return fmt.Errorf("web server config: %w", err)
	}
	webCfg.Logger = log
	if !webCfg.Configured() {
		// Standalone, this is fatal rather than a shrug: the operator started a
		// daemon whose only job is to serve, and it has nowhere to listen.
		return fmt.Errorf("no listen address configured: set PELICAN_REGISTRATION_SOCKET or PELICAN_MANAGER_WEB_LISTEN_ADDRESS")
	}

	srv, err := webserver.NewServerWithConfig(webCfg)
	if err != nil {
		return fmt.Errorf("web server initialization failed: %w", err)
	}

	// Command socket: the inherited shared-port endpoint under condor_master,
	// else a plain bind. Serving the standard DaemonCore commands on it is what
	// makes condor_ping / condor_reconfig -daemon / condor_off -daemon work
	// against this daemon. It is separate from the HTTP listener, which speaks
	// TLS to Pelican clients rather than CEDAR to condor tools.
	ln, err := d.Listener(func() (net.Listener, error) {
		return (&net.ListenConfig{}).Listen(context.Background(), "tcp", *listen)
	})
	if err != nil {
		return fmt.Errorf("command-socket listener: %w", err)
	}
	defer func() { _ = ln.Close() }()

	sec, err := htcondor.GetServerSecurityConfig(d.Config(), commands.DC_NOP, "DEFAULT")
	if err != nil {
		return fmt.Errorf("building security config: %w", err)
	}
	cmdSrv := cedarserver.New(sec)
	d.RegisterDefaultCommands(cmdSrv)

	if path := writeAddressFile(d, ln); path != "" {
		defer func() { _ = os.Remove(path) }()
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		if err := srv.Start(ctx); err != nil && err != context.Canceled {
			log.Errorf(htcondorlogging.DestinationGeneral, "web server error: %v", err)
		}
	}()

	log.Infof(htcondorlogging.DestinationGeneral,
		"pelican-web %s starting: command_socket=%s under_master=%v socket=%s listen=%q db=%s",
		version, ln.Addr().String(), d.UnderMaster(), webCfg.SocketPath, webCfg.ListenAddress, webCfg.DBPath)

	// Blocks until a termination signal, a command-server error, or DC_OFF. The
	// deferred cancel then stops the HTTP server.
	serveErr := d.Serve(ctx, ln, cmdSrv.Serve)
	cancel()
	wg.Wait()
	return serveErr
}

// writeAddressFile publishes the command address to PELICAN_WEB_ADDRESS_FILE
// (default $(LOG)/.pelican_web_address) as a sinful string, so condor tools can
// reach this daemon's command port. Returns the path written, or "".
func writeAddressFile(d *condordaemon.Daemon, ln net.Listener) string {
	cfg := d.Config()
	path, ok := cfg.Get("PELICAN_WEB_ADDRESS_FILE")
	if !ok || strings.TrimSpace(path) == "" {
		logDir, ok := cfg.Get("LOG")
		if !ok || logDir == "" {
			return ""
		}
		path = filepath.Join(logDir, ".pelican_web_address")
	}

	addr := ln.Addr().String()
	if sinful, ok := d.AdvertisedSinful(); ok {
		addr = sinful
	}
	if err := os.WriteFile(path, []byte("<"+addr+">\n"), 0o644); err != nil {
		d.Logger().Warnf(htcondorlogging.DestinationGeneral, "could not write address file %s: %v", path, err)
		return ""
	}
	return path
}
