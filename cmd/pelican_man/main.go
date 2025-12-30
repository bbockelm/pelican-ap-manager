package main

import (
	"context"
	"flag"
	"log"
	"os"
	"os/signal"
	"syscall"
	"time"

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
	advertiseDryRun := flag.String("advertise-dry-run", "", "write advertised ClassAds to the given file instead of sending")
	flag.Parse()

	logger := log.New(os.Stdout, "pelican_man ", log.LstdFlags|log.Lmsgprefix)

	cfg, err := config.Load()
	if err != nil {
		logger.Fatalf("config error: %v", err)
	}

	cfg = cfg.WithOverrides(0, 0, 0, 0, 0, "", *collector, *schedd, "", "")

	logger.Printf("loading state from %s", cfg.StatePath)

	st, err := state.Load(cfg.StatePath)
	if err != nil {
		logger.Fatalf("state load error: %v", err)
	}

	condorClient, err := condor.NewClient(cfg.CollectorHost, cfg.ScheddName, cfg.SiteAttribute)
	if err != nil {
		logger.Fatalf("condor client init failed: %v", err)
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
		logger.Printf("job mirror initialization failed: %v; falling back to schedd polling", err)
	}

	svc := daemon.NewService(condorClient, st, cfg.StatePath, cfg.PollInterval, cfg.AdvertiseInterval, cfg.EpochLookback, cfg.StatsWindow, tracker, jobMirror, cfg.JobMirrorPath, directorClient, logger, *advertiseDryRun, cfg.ScheddName, cfg.SiteAttribute, *oneshoot)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	sigs := make(chan os.Signal, 1)
	signal.Notify(sigs, syscall.SIGINT, syscall.SIGTERM)
	go func() {
		<-sigs
		logger.Println("signal received; shutting down")
		cancel()
	}()

	if err := svc.Run(ctx); err != nil {
		logger.Fatalf("service terminated with error: %v", err)
	}
}
