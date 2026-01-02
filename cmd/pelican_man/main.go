package main

import (
	"context"
	"flag"
	"os"
	"os/signal"
	"syscall"
	"time"

	htcondorlogging "github.com/bbockelm/golang-htcondor/logging"
	"github.com/bbockelm/pelican-ap-manager/internal/condor"
	"github.com/bbockelm/pelican-ap-manager/internal/config"
	"github.com/bbockelm/pelican-ap-manager/internal/daemon"
	"github.com/bbockelm/pelican-ap-manager/internal/director"
	"github.com/bbockelm/pelican-ap-manager/internal/jobqueue"
	"github.com/bbockelm/pelican-ap-manager/internal/state"
	"github.com/bbockelm/pelican-ap-manager/internal/stats"
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
		_, _ = os.Stderr.WriteString("config error: " + err.Error() + "\n")
		os.Exit(1)
	}

	// Initialize logging using HTCondor config
	logger, err := htcondorlogging.FromConfigWithDaemon("PELICAN_MANAGER", cfg.HTCondorConfig())
	if err != nil {
		_, _ = os.Stderr.WriteString("logging setup error: " + err.Error() + "\n")
		os.Exit(1)
	}

	cfg = cfg.WithOverrides(0, 0, 0, 0, 0, "", *infoPath, *collector, *schedd, "", "")

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

	if err := svc.Run(ctx); err != nil {
		logger.Errorf(htcondorlogging.DestinationGeneral, "service terminated with error: %v", err)
		os.Exit(1)
	}
}
