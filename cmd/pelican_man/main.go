package main

import (
	"context"
	"flag"
	"os"
	"os/signal"
	"path/filepath"
	"strings"
	"sync"
	"syscall"
	"time"

	"github.com/bbockelm/golang-htcondor/droppriv"
	htcondorlogging "github.com/bbockelm/golang-htcondor/logging"
	"github.com/bbockelm/pelican-ap-manager/internal/condor"
	"github.com/bbockelm/pelican-ap-manager/internal/config"
	"github.com/bbockelm/pelican-ap-manager/internal/daemon"
	"github.com/bbockelm/pelican-ap-manager/internal/director"
	"github.com/bbockelm/pelican-ap-manager/internal/jobqueue"
	"github.com/bbockelm/pelican-ap-manager/internal/state"
	"github.com/bbockelm/pelican-ap-manager/internal/stats"
	"github.com/bbockelm/pelican-ap-manager/internal/webserver"
)

func main() {
	oneshoot := flag.Bool("oneshot", false, "run a single poll/advertise cycle and print findings")
	schedd := flag.String("schedd", "", "override schedd name")
	collector := flag.String("collector", "", "override collector host")
	infoPath := flag.String("info-path", "", "write info ClassAds to the given file (default: SPOOL/pelican_info.json)")
	flag.Parse()

	cfg, err := config.Load()
	if err != nil {
		// Can't use logger yet, fall back to stderr
		os.Stderr.WriteString("config error: " + err.Error() + "\n")
		os.Exit(1)
	}

	// Initialize logging using HTCondor config
	logger, err := htcondorlogging.FromConfigWithDaemon("PELICAN_MANAGER", cfg.HTCondorConfig())
	if err != nil {
		os.Stderr.WriteString("logging setup error: " + err.Error() + "\n")
		os.Exit(1)
	}

	// Initialize droppriv manager if running as root
	if os.Geteuid() == 0 {
		logger.Infof(htcondorlogging.DestinationGeneral, "Running as root, enabling privilege drop mode")

		dropPrivConfig := droppriv.ConfigFromHTCondor(cfg.HTCondorConfig())
		dropPrivConfig.Enabled = true

		privMgr, err := droppriv.NewManager(dropPrivConfig)
		if err != nil {
			logger.Errorf(htcondorlogging.DestinationGeneral, "Failed to initialize privilege manager: %v", err)
			os.Exit(1)
		}

		if err := privMgr.Start(); err != nil {
			logger.Errorf(htcondorlogging.DestinationGeneral, "Failed to start privilege manager: %v", err)
			os.Exit(1)
		}

		// Store in atomic pointer so sandbox APIs can use it
		droppriv.ReloadDefaultManager()
		logger.Infof(htcondorlogging.DestinationGeneral, "Privilege manager initialized and started")
	}

	cfg = cfg.WithOverrides(0, 0, 0, 0, 0, "", *infoPath, *collector, *schedd, "", "")

	logger.Infof(htcondorlogging.DestinationGeneral, "collector config after load: host=%s logpath=%s", cfg.CollectorHost, cfg.LogPath)

	// If collector host needs address file (dynamic port :0), poll for it
	needsPolling := needsCollectorAddressFileResolution(cfg.CollectorHost, cfg.LogPath)
	logger.Infof(htcondorlogging.DestinationGeneral, "needs collector address polling: %v (host=%s)", needsPolling, cfg.CollectorHost)
	if needsPolling {
		logger.Infof(htcondorlogging.DestinationGeneral, "waiting for collector address file discovery... (logpath=%s)", cfg.LogPath)
		resolvedAddr := pollForCollectorAddress(cfg.LogPath, 60*time.Second, logger)
		if resolvedAddr != "" {
			cfg.CollectorHost = resolvedAddr
			logger.Infof(htcondorlogging.DestinationGeneral, "discovered collector address: %s", cfg.CollectorHost)
		} else {
			logger.Warnf(htcondorlogging.DestinationGeneral, "collector address file not found after 60s, will retry at runtime")
		}
	}

	logger.Infof(htcondorlogging.DestinationGeneral, "loading state from %s", cfg.StatePath)

	st, err := state.Load(cfg.StatePath)
	if err != nil {
		logger.Errorf(htcondorlogging.DestinationGeneral, "state load error: %v", err)
		os.Exit(1)
	}

	condorClient, err := condor.NewClient(cfg.CollectorHost, cfg.ScheddName, cfg.SiteAttribute)
	if err != nil {
		logger.Errorf(htcondorlogging.DestinationGeneral, "condor client init failed: %v", err)
		os.Exit(1)
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

	directorClient := director.New(cfg.DirectorCacheTTL)

	jobMirror, err := jobqueue.NewMirror(cfg.JobQueueLogPath, condorClient, logger)
	if err != nil {
		logger.Infof(htcondorlogging.DestinationGeneral, "job mirror initialization failed: %v; falling back to schedd polling", err)
	}

	svc := daemon.NewService(condorClient, st, cfg.StatePath, cfg.PollInterval, cfg.AdvertiseInterval, cfg.EpochLookback, cfg.StatsWindow, tracker, jobMirror, cfg.JobMirrorPath, directorClient, logger, cfg.InfoPath, cfg.ScheddName, cfg.SiteAttribute, *oneshoot)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	var wg sync.WaitGroup

	// Write address file for condor_master discovery
	if err := daemon.WriteAddressFile(cfg.AddressFilePath, cfg.WebSocketPath); err != nil {
		logger.Warnf(htcondorlogging.DestinationGeneral, "failed to write address file: %v", err)
	}

	// Initialize master communicator for heartbeats and ready signals
	masterComm := daemon.NewMasterCommunicator(cfg.MasterSockPath, logger)
	if err := masterComm.SendReady(); err != nil {
		logger.Warnf(htcondorlogging.DestinationGeneral, "failed to send ready signal: %v", err)
	}

	// Start periodic heartbeats to condor_master (every 5 minutes)
	heartbeatInterval := 5 * time.Minute
	_ = masterComm.StartHeartbeat(ctx, heartbeatInterval)

	// Start web server if configured
	if cfg.WebListenAddress != "" || cfg.WebSocketPath != "" {
		webSrv, err := webserver.NewServerWithConfig(&webserver.ServerConfig{
			ListenAddress:  cfg.WebListenAddress,
			SocketPath:     cfg.WebSocketPath,
			TLSCert:        cfg.WebTLSCert,
			TLSKey:         cfg.WebTLSKey,
			DBPath:         cfg.WebDBPath,
			Logger:         logger,
			HTCondorConfig: cfg.HTCondorConfig(),
			ScheddAddr:     cfg.ScheddAddr,
		})
		if err != nil {
			logger.Errorf(htcondorlogging.DestinationGeneral, "web server initialization failed: %v", err)
			os.Exit(1)
		}

		wg.Add(1)
		go func() {
			defer wg.Done()
			if err := webSrv.Start(ctx); err != nil && err != context.Canceled {
				logger.Errorf(htcondorlogging.DestinationGeneral, "web server error: %v", err)
			}
		}()
	} else {
		logger.Infof(htcondorlogging.DestinationGeneral, "Web server not configured (set PELICAN_MANAGER_WEB_LISTEN_ADDRESS or PELICAN_REGISTRATION_SOCKET)")
	}

	sigs := make(chan os.Signal, 1)
	signal.Notify(sigs, syscall.SIGINT, syscall.SIGTERM, syscall.SIGHUP)
	go func() {
		for {
			sig := <-sigs
			switch sig {
			case syscall.SIGHUP:
				logger.Infof(htcondorlogging.DestinationGeneral, "SIGHUP received; reloading configuration")
				newCfg, err := config.Load()
				if err != nil {
					logger.Errorf(htcondorlogging.DestinationGeneral, "config reload error: %v", err)
					continue
				}
				// Apply any CLI overrides
				newCfg = newCfg.WithOverrides(0, 0, 0, 0, 0, "", *infoPath, *collector, *schedd, "", "")
				svc.ReloadConfig(newCfg)
			case syscall.SIGINT, syscall.SIGTERM:
				logger.Infof(htcondorlogging.DestinationGeneral, "signal received; shutting down")
				cancel()
				return
			}
		}
	}()

	wg.Add(1)
	go func() {
		defer wg.Done()
		if err := svc.Run(ctx); err != nil {
			logger.Errorf(htcondorlogging.DestinationGeneral, "service terminated with error: %v", err)
		}
	}()

	wg.Wait()
}

// needsCollectorAddressFileResolution checks if we need to poll for the collector address file.
func needsCollectorAddressFileResolution(collectorHost, logPath string) bool {
	if collectorHost == "" || logPath == "" {
		return false
	}
	// Check if it ends with :0 (dynamic port assignment)
	if len(collectorHost) > 2 && collectorHost[len(collectorHost)-2:] == ":0" {
		return true
	}
	return false
}

// pollForCollectorAddress polls the collector address file until it's available or timeout.
func pollForCollectorAddress(logPath string, timeout time.Duration, logger *htcondorlogging.Logger) string {
	deadline := time.Now().Add(timeout)
	addrFile := filepath.Join(logPath, ".collector_address")

	for time.Now().Before(deadline) {
		// Check if file exists first
		if info, err := os.Stat(addrFile); err == nil && info.Size() > 0 {
			addr := readCollectorAddressFromFile(addrFile)
			if addr != "" {
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
