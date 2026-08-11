// Command pelican_man is the Pelican AP manager daemon. It runs under
// condor_master as an HTCondor daemon (subsystem PELICAN_MANAGER): it polls the
// schedd/collector for transfer epochs, summarizes them per user/site, publishes
// the summaries to the collector, and drives schedd startup limits from the
// resulting control decisions.
//
// The daemon lifecycle -- configuration, HTCondor logging, privilege drop,
// condor_master readiness/keepalive, SIGHUP reconfigure, shared-port command
// socket -- is the golang-htcondor daemon framework's. This file only wires the
// pelican-specific services onto it.
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
	"time"

	"github.com/bbockelm/cedar/commands"
	cedarserver "github.com/bbockelm/cedar/server"
	htcondor "github.com/bbockelm/golang-htcondor"
	condorconfig "github.com/bbockelm/golang-htcondor/config"
	condordaemon "github.com/bbockelm/golang-htcondor/daemon"
	"github.com/bbockelm/golang-htcondor/droppriv"
	htcondorlogging "github.com/bbockelm/golang-htcondor/logging"
	"github.com/bbockelm/pelican-ap-manager/internal/condor"
	"github.com/bbockelm/pelican-ap-manager/internal/config"
	"github.com/bbockelm/pelican-ap-manager/internal/daemon"
	"github.com/bbockelm/pelican-ap-manager/internal/director"
	"github.com/bbockelm/pelican-ap-manager/internal/jobqueue"
	"github.com/bbockelm/pelican-ap-manager/internal/state"
	"github.com/bbockelm/pelican-ap-manager/internal/stats"
	"github.com/bbockelm/pelican-ap-manager/internal/store"
	"github.com/bbockelm/pelican-ap-manager/internal/webserver"
)

// subsystem is the HTCondor subsystem name. It selects the per-daemon log knobs
// (PELICAN_MANAGER_LOG, MAX_PELICAN_MANAGER_LOG, PELICAN_MANAGER_DEBUG) and the
// PELICAN_MANAGER.<key> configuration scope.
const subsystem = "PELICAN_MANAGER"

// version is stamped at build time (see the Makefile).
var version = "dev"

func main() {
	if err := run(); err != nil {
		fmt.Fprintln(os.Stderr, "pelican_man:", err)
		os.Exit(1)
	}
}

func run() error {
	showVersion := flag.Bool("version", false, "print the version and exit")
	oneshot := flag.Bool("oneshot", false, "run a single poll/advertise cycle and print findings")
	scheddFlag := flag.String("schedd", "", "override schedd name")
	collectorFlag := flag.String("collector", "", "override collector host")
	infoPath := flag.String("info-path", "", "write info ClassAds to the given file (default: SPOOL/pelican_info.json)")
	listen := flag.String("listen", "127.0.0.1:0", "command-socket bind address when not running under condor_master")
	// condor_master passes these to every daemon it starts; flag.Parse would
	// reject them otherwise. -local-name additionally scopes config lookups.
	localName := flag.String("local-name", "", "HTCondor subsystem local-name; passed by condor_master")
	_ = flag.String("sock", "", "HTCondor shared-port endpoint name; accepted for compatibility (fd inherited via CONDOR_INHERIT)")
	flag.Parse()

	if *showVersion {
		fmt.Println("pelican_man", version)
		return nil
	}

	condorCfg, err := condorconfig.NewWithOptions(condorconfig.ConfigOptions{
		Subsystem: subsystem,
		LocalName: *localName,
	})
	if err != nil {
		return fmt.Errorf("loading config: %w", err)
	}

	// Bootstrap: HTCondor logging, condor_master integration (DC_SET_READY +
	// DC_CHILDALIVE), and the privilege drop to the condor user when started as
	// root. The drop happens inside New, before the log file is opened, so every
	// file this process creates belongs to the dropped-to user.
	d, err := condordaemon.New(condordaemon.Options{
		Subsys:    subsystem,
		LocalName: *localName,
		Config:    condorCfg,
	})
	if err != nil {
		return err
	}
	log := d.Logger()

	// Rebuild the process-wide droppriv singleton now that the drop has happened,
	// so components that switch to a job owner (the sandbox HTTP handlers) see the
	// post-drop identity as their baseline.
	droppriv.ReloadDefaultManager()

	cfg, err := config.LoadFrom(d.Config())
	if err != nil {
		return fmt.Errorf("pelican config: %w", err)
	}
	cfg = cfg.WithOverrides(0, 0, 0, 0, 0, "", *infoPath, *collectorFlag, *scheddFlag, "", "")

	log.Infof(htcondorlogging.DestinationGeneral, "collector config after load: host=%s logpath=%s", cfg.CollectorHost, cfg.LogPath)

	// If collector host needs address file (dynamic port :0), poll for it
	if needsCollectorAddressFileResolution(cfg.CollectorHost, cfg.LogPath) {
		log.Infof(htcondorlogging.DestinationGeneral, "waiting for collector address file discovery... (logpath=%s)", cfg.LogPath)
		if resolvedAddr := pollForCollectorAddress(cfg.LogPath, 60*time.Second); resolvedAddr != "" {
			cfg.CollectorHost = resolvedAddr
			log.Infof(htcondorlogging.DestinationGeneral, "discovered collector address: %s", cfg.CollectorHost)
		} else {
			log.Warnf(htcondorlogging.DestinationGeneral, "collector address file not found after 60s, will retry at runtime")
		}
	}

	svc, err := buildService(cfg, log, *oneshot)
	if err != nil {
		return err
	}

	// Rate rules: the operator's static policy plus the control loop's own
	// conclusions, persisted so both survive a restart. Failing to open the
	// store is not fatal -- the daemon still observes and advertises -- but it
	// does mean no static policy is in force, which is worth an error-level
	// line rather than a warning.
	ruleStore, storeDesc, err := store.Open(store.Options{
		DBAddress: cfg.RuleDBAddress,
		DBTable:   cfg.RuleDBTable,
		FilePath:  cfg.RuleStorePath,
		Config:    d.Config(),
	})
	if err != nil {
		log.Errorf(htcondorlogging.DestinationGeneral, "rate rule store unavailable; no static rules will be applied: %v", err)
	} else {
		defer func() { _ = ruleStore.Close() }()
		log.Infof(htcondorlogging.DestinationGeneral, "rate rule store: %s", storeDesc)
		svc.SetRuleStore(ruleStore)
		syncStaticRules(context.Background(), ruleStore, cfg, log)
	}
	svc.SetEnforcement(cfg.EnforcementMode)
	log.Infof(htcondorlogging.DestinationGeneral, "rate limit enforcement mode: %s (%d static rule(s) declared)",
		cfg.EnforcementMode, len(cfg.StaticRules))

	// A -oneshot run is a diagnostic, not a daemon: no command socket, no master
	// signaling, no web server. Run the single cycle and exit.
	if *oneshot {
		return svc.Run(context.Background())
	}

	// Command socket: the inherited shared-port endpoint under condor_master,
	// else a plain bind. Serving the standard DaemonCore commands on it is what
	// makes condor_ping / condor_reconfig -daemon / condor_off -daemon work
	// against this daemon.
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

	if path := writeAddressFile(d, cfg, ln); path != "" {
		defer func() { _ = os.Remove(path) }()
	}

	// SIGHUP / DC_RECONFIG: re-derive the pelican configuration from the freshly
	// reloaded HTCondor config and push the runtime-adjustable knobs into the
	// service.
	d.OnReconfig(func(newCondorCfg *condorconfig.Config) {
		newCfg, rerr := config.LoadFrom(newCondorCfg)
		if rerr != nil {
			log.Errorf(htcondorlogging.DestinationGeneral, "config reload error: %v", rerr)
			return
		}
		newCfg = newCfg.WithOverrides(0, 0, 0, 0, 0, "", *infoPath, *collectorFlag, *scheddFlag, "", "")
		svc.ReloadConfig(newCfg)
		// Enforcement mode and the static rule set are both reconfigurable:
		// flipping to observing, or retiring a rule, should not need a restart.
		svc.SetEnforcement(newCfg.EnforcementMode)
		log.Infof(htcondorlogging.DestinationGeneral, "rate limit enforcement mode: %s (%d static rule(s) declared)",
			newCfg.EnforcementMode, len(newCfg.StaticRules))
		if ruleStore != nil {
			syncStaticRules(context.Background(), ruleStore, newCfg, log)
		}
	})

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	var wg sync.WaitGroup

	wg.Add(1)
	go func() {
		defer wg.Done()
		if err := svc.Run(ctx); err != nil && ctx.Err() == nil {
			log.Errorf(htcondorlogging.DestinationGeneral, "service terminated with error: %v", err)
		}
	}()

	if cfg.WebListenAddress != "" || cfg.WebSocketPath != "" {
		webSrv, err := webserver.NewServerWithConfig(&webserver.ServerConfig{
			ListenAddress:  cfg.WebListenAddress,
			SocketPath:     cfg.WebSocketPath,
			TLSCert:        cfg.WebTLSCert,
			TLSKey:         cfg.WebTLSKey,
			DBPath:         cfg.WebDBPath,
			Logger:         log,
			HTCondorConfig: d.Config(),
			ScheddAddr:     cfg.ScheddAddr,
		})
		if err != nil {
			return fmt.Errorf("web server initialization failed: %w", err)
		}
		wg.Add(1)
		go func() {
			defer wg.Done()
			if err := webSrv.Start(ctx); err != nil && err != context.Canceled {
				log.Errorf(htcondorlogging.DestinationGeneral, "web server error: %v", err)
			}
		}()
	} else {
		log.Infof(htcondorlogging.DestinationGeneral, "Web server not configured (set PELICAN_MANAGER_WEB_LISTEN_ADDRESS or PELICAN_REGISTRATION_SOCKET)")
	}

	log.Infof(htcondorlogging.DestinationGeneral, "pelican_man starting: command_socket=%s under_master=%v %s",
		ln.Addr().String(), d.UnderMaster(), cfg.EffectiveIntervals())

	// Blocks until a termination signal, a command-server error, or DC_OFF. The
	// deferred cancel then stops the service loop and the web server.
	serveErr := d.Serve(ctx, ln, cmdSrv.Serve)
	cancel()
	wg.Wait()
	return serveErr
}

// syncStaticRules pushes the configuration's rate rules into the store and
// retires the config-managed rules that are no longer declared.
//
// A store that cannot be written is logged and tolerated: the daemon keeps
// running, and any rules already installed in the schedd stay in force until
// they lapse. The operator sees why in the log rather than in a crash loop.
func syncStaticRules(ctx context.Context, rs store.RuleStore, cfg *config.Config, log *htcondorlogging.Logger) {
	syncCtx, cancel := context.WithTimeout(ctx, 30*time.Second)
	defer cancel()
	if err := store.SyncConfigRules(syncCtx, rs, cfg.StaticRules); err != nil {
		log.Errorf(htcondorlogging.DestinationGeneral, "syncing static rate rules: %v", err)
		return
	}
	for _, r := range cfg.StaticRules {
		log.Infof(htcondorlogging.DestinationGeneral, "static rate rule %s: user=%q site=%q %d jobs/%s%s",
			r.Name, r.User, r.Site, r.RateCount, r.Window(), noteSuffix(r.Note))
	}
}

func noteSuffix(note string) string {
	if note == "" {
		return ""
	}
	return " (" + note + ")"
}

// buildService assembles the polling/advertising service and the state it
// carries across restarts.
func buildService(cfg *config.Config, log *htcondorlogging.Logger, oneshot bool) (*daemon.Service, error) {
	log.Infof(htcondorlogging.DestinationGeneral, "loading state from %s", cfg.StatePath)
	st, err := state.Load(cfg.StatePath)
	if err != nil {
		return nil, fmt.Errorf("state load: %w", err)
	}

	condorClient, err := condor.NewClient(cfg.CollectorHost, cfg.ScheddName, cfg.SiteAttribute)
	if err != nil {
		return nil, fmt.Errorf("condor client init: %w", err)
	}

	tracker := stats.NewTracker(cfg.StatsWindow)
	if len(st.RecentTransfers) > 0 {
		preload := make(map[string][]stats.ProcessedTransfer, len(st.RecentTransfers))
		for user, entries := range st.RecentTransfers {
			for _, e := range entries {
				preload[user] = append(preload[user], stats.ProcessedTransfer{
					Epoch:            e.Epoch,
					User:             user,
					Endpoint:         e.Endpoint,
					Site:             e.Site,
					Source:           e.Source,
					Destination:      e.Destination,
					Direction:        state.Direction(e.Direction),
					FederationPrefix: e.FederationPrefix,
					Bytes:            e.Bytes,
					Duration:         time.Duration(e.DurationSeconds * float64(time.Second)),
					JobRuntime:       time.Duration(e.JobRuntimeSec * float64(time.Second)),
					Success:          e.Success,
					EndedAt:          e.EndedAt,
					Cached:           e.Cached,
					SandboxName:      e.SandboxName,
					SandboxSize:      e.SandboxSize,
					SandboxObject:    e.SandboxObject,
				})
			}
		}
		tracker.Load(preload)
	}

	jobMirror, err := jobqueue.NewMirror(cfg.JobQueueLogPath, condorClient, log)
	if err != nil {
		log.Infof(htcondorlogging.DestinationGeneral, "job mirror initialization failed: %v; falling back to schedd polling", err)
	}

	return daemon.NewService(condorClient, st, cfg.StatePath, cfg.PollInterval, cfg.AdvertiseInterval,
		cfg.EpochLookback, cfg.StatsWindow, tracker, jobMirror, cfg.JobMirrorPath,
		director.New(cfg.DirectorCacheTTL), log, cfg.InfoPath, cfg.ScheddName, cfg.SiteAttribute, oneshot), nil
}

// writeAddressFile publishes the command address to PELICAN_MANAGER_ADDRESS_FILE
// (default $(LOG)/.pelican_manager_address) as a sinful string, so condor tools
// and the master can reach this daemon's command port. Returns the path written,
// or "" when no address file is configured.
func writeAddressFile(d *condordaemon.Daemon, cfg *config.Config, ln net.Listener) string {
	path := cfg.AddressFilePath
	if strings.TrimSpace(path) == "" {
		return ""
	}
	if dir := filepath.Dir(path); dir != "" {
		if err := os.MkdirAll(dir, 0o755); err != nil {
			d.Logger().Warnf(htcondorlogging.DestinationGeneral, "could not create address file directory %s: %v", dir, err)
			return ""
		}
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

// needsCollectorAddressFileResolution checks if we need to poll for the collector address file.
func needsCollectorAddressFileResolution(collectorHost, logPath string) bool {
	if collectorHost == "" || logPath == "" {
		return false
	}
	// Check if it ends with :0 (dynamic port assignment)
	return strings.HasSuffix(collectorHost, ":0")
}

// pollForCollectorAddress polls the collector address file until it's available or timeout.
func pollForCollectorAddress(logPath string, timeout time.Duration) string {
	deadline := time.Now().Add(timeout)
	addrFile := filepath.Join(logPath, ".collector_address")

	for time.Now().Before(deadline) {
		// Check if file exists first
		if info, err := os.Stat(addrFile); err == nil && info.Size() > 0 {
			if addr := readCollectorAddressFromFile(addrFile); addr != "" {
				return addr
			}
		}
		time.Sleep(100 * time.Millisecond)
	}

	return ""
}

// readCollectorAddressFromFile reads and parses the collector address file.
func readCollectorAddressFromFile(addrFile string) string {
	data, err := os.ReadFile(addrFile)
	if err != nil {
		return ""
	}

	// Parse the address file - it may contain multiple lines, we want the sinful string
	for _, line := range strings.Split(string(data), "\n") {
		line = strings.TrimSpace(line)
		if line == "" || strings.Contains(line, "(null)") {
			continue
		}
		// Look for sinful string format: <IP:port...>
		if strings.HasPrefix(line, "<") {
			// Extract host:port from sinful string
			if idx := strings.Index(line, "?"); idx > 0 {
				// Remove the sinful wrapper and query params
				return line[1:idx]
			}
			if idx := strings.Index(line, ">"); idx > 0 {
				return line[1:idx]
			}
		}
	}

	return ""
}
