package daemon

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"log"
	"net/url"
	"os"
	"path"
	"path/filepath"
	"sort"
	"strings"
	"time"

	"github.com/bbockelm/pelican-ap-manager/internal/condor"
	"github.com/bbockelm/pelican-ap-manager/internal/control"
	"github.com/bbockelm/pelican-ap-manager/internal/director"
	"github.com/bbockelm/pelican-ap-manager/internal/jobqueue"
	"github.com/bbockelm/pelican-ap-manager/internal/state"
	"github.com/bbockelm/pelican-ap-manager/internal/stats"
)

const (
	jobEpochRetention      = 24 * time.Hour
	transferEpochRetention = 24 * time.Hour
)

// Service coordinates polling HTCondor state, maintaining aggregates, and advertising to the collector.
type Service struct {
	condor            condor.CondorClient
	state             *state.State
	statePath         string
	pollInterval      time.Duration
	advertiseInterval time.Duration
	advertiseDryRun   string
	scheddName        string
	epochLookback     time.Duration
	tracker           *stats.Tracker
	jobs              *jobqueue.Mirror
	jobMirrorPath     string
	director          *director.Client
	statsWindow       time.Duration
	logger            *log.Logger
	oneshoot          bool
	startTime         time.Time
	lastJobEpoch      state.EpochID
	controlCfg        control.Config
}

// NewService wires up dependencies for the daemon.
func NewService(client condor.CondorClient, st *state.State, statePath string, poll, advertise, epochLookback, statsWindow time.Duration, tracker *stats.Tracker, jobMirror *jobqueue.Mirror, jobMirrorPath string, directorClient *director.Client, logger *log.Logger, advertiseDryRun, scheddName string, oneshot bool) *Service {
	if statsWindow <= 0 {
		statsWindow = time.Hour
	}
	if poll <= 0 {
		poll = 30 * time.Second
	}
	if advertise <= 0 {
		advertise = time.Minute
	}
	if epochLookback <= 0 {
		epochLookback = 24 * time.Hour
	}
	return &Service{
		condor:            client,
		state:             st,
		statePath:         statePath,
		pollInterval:      poll,
		advertiseInterval: advertise,
		advertiseDryRun:   advertiseDryRun,
		scheddName:        scheddName,
		epochLookback:     epochLookback,
		tracker:           tracker,
		jobs:              jobMirror,
		jobMirrorPath:     jobMirrorPath,
		director:          directorClient,
		statsWindow:       statsWindow,
		logger:            logger,
		oneshoot:          oneshot,
		startTime:         time.Now(),
		controlCfg:        control.DefaultConfig(),
	}
}

// Run starts the main polling and advertisement loops until the context is canceled.
func (s *Service) Run(ctx context.Context) error {
	if s.oneshoot {
		s.logger.Printf("pelican_man oneshot: poll=%s advertise=%s stats_window=%s", s.pollInterval, s.advertiseInterval, s.statsWindow)
		return s.runOnce(ctx)
	}
	s.logger.Printf("pelican_man starting: poll=%s advertise=%s stats_window=%s", s.pollInterval, s.advertiseInterval, s.statsWindow)

	pollTicker := time.NewTicker(s.pollInterval)
	advTicker := time.NewTicker(s.advertiseInterval)
	defer pollTicker.Stop()
	defer advTicker.Stop()

	for {
		select {
		case <-ctx.Done():
			s.logger.Println("pelican_man shutting down")
			return nil
		case <-pollTicker.C:
			s.pollOnce(ctx)
		case <-advTicker.C:
			s.advertiseOnce()
		}
	}
}

// runOnce executes a single poll/summary cycle in oneshot mode.
func (s *Service) runOnce(ctx context.Context) error {
	count := s.pollOnce(ctx)
	s.printFindings(count)

	s.saveState()

	if s.advertiseDryRun != "" {
		s.advertiseOnce()
	}
	return nil
}

// pollOnce fetches recent transfers and updates persisted state.
func (s *Service) pollOnce(ctx context.Context) int {
	if s.jobs != nil {
		if err := s.jobs.Sync(ctx); err != nil {
			s.logger.Printf("job mirror sync error: %v", err)
		} else {
			s.persistJobMirror()
		}
	}

	lastEpoch, _ := s.state.Snapshot()
	cutoff := time.Now().Add(-s.statsWindow)
	records, newestEpoch, err := s.condor.FetchTransferEpochs(lastEpoch, cutoff)
	if err != nil {
		s.logger.Printf("poll error: %v", err)
		return 0
	}

	jobRecords, newestJobEpoch, err := s.condor.FetchJobEpochs(s.lastJobEpoch, cutoff)
	if err != nil {
		s.logger.Printf("job epoch poll error: %v", err)
	}

	for _, rec := range records {
		if rec.User == "" {
			if u, ok := s.state.LookupUserForEpoch(rec.EpochID); ok {
				rec.User = u
			}
		}
		dir := state.Direction(rec.Direction)
		if dir != state.DirectionUpload && dir != state.DirectionDownload {
			dir = state.DirectionUpload
		}

		pt := s.buildProcessedTransfers(rec)

		key := state.SummaryKey{User: rec.User, Endpoint: rec.Endpoint, Site: rec.Site, Direction: dir}
		bucketKey := state.KeyString(key)
		var epochRefs []state.TransferEpochRef
		for _, tr := range pt {
			s.state.Update(key, tr.Success, tr.Bytes, tr.Duration, tr.EndedAt, tr.FederationPrefix)
			epochRefs = append(epochRefs, state.TransferEpochRef{
				Epoch:         tr.Epoch,
				EndedAt:       tr.EndedAt,
				DurationSec:   tr.Duration.Seconds(),
				User:          tr.User,
				JobRuntimeSec: tr.JobRuntime.Seconds(),
				Source:        tr.Source,
				Destination:   tr.Destination,
			})
		}
		if len(epochRefs) > 0 {
			s.state.AppendEpochBuckets(transferEpochRetention, bucketKey, epochRefs)
		}

		if s.tracker != nil && len(pt) > 0 {
			s.tracker.Add(rec.User, pt)
			s.state.AppendRecent(s.statsWindow, rec.User, s.toHistoryEntries(pt))
		}
	}

	if len(jobRecords) > 0 {
		byUser := make(map[string][]stats.ProcessedTransfer)
		for _, jr := range jobRecords {
			s.state.AppendJobEpoch(jobEpochRetention, state.JobEpochSample{
				Epoch:      jr.EpochID,
				User:       jr.User,
				Site:       jr.Site,
				RuntimeSec: jr.Runtime.Seconds(),
				EndedAt:    jr.EndedAt,
			})

			if bucket, _, ok := s.state.LookupBucketForEpoch(jr.EpochID); ok && jr.Runtime > 0 {
				s.state.AppendBucketRuntime(jobEpochRetention, bucket, state.BucketRuntimeSample{
					Epoch:      jr.EpochID,
					RuntimeSec: jr.Runtime.Seconds(),
					EndedAt:    jr.EndedAt,
				})
			}

			if s.tracker == nil {
				continue
			}

			if !jr.Success || jr.Runtime <= 0 {
				continue
			}
			byUser[jr.User] = append(byUser[jr.User], stats.ProcessedTransfer{
				Epoch:      jr.EpochID,
				User:       jr.User,
				Site:       jr.Site,
				Bytes:      0,
				Duration:   jr.Runtime,
				JobRuntime: jr.Runtime,
				Success:    true,
				EndedAt:    jr.EndedAt,
			})
		}
		for user, trs := range byUser {
			s.tracker.Add(user, trs)
			s.state.AppendRecent(s.statsWindow, user, s.toHistoryEntries(trs))
		}
	}

	if newestEpoch.After(lastEpoch) {
		s.state.SetLastEpoch(newestEpoch)
	}
	if newestJobEpoch.After(s.lastJobEpoch) {
		s.lastJobEpoch = newestJobEpoch
	}

	if !s.oneshoot {
		s.saveState()
	}

	return len(records)
}

func (s *Service) saveState() {
	if s.tracker != nil {
		grouped := make(map[string][]stats.ProcessedTransfer)
		for _, tr := range s.tracker.AllTransfers() {
			grouped[tr.User] = append(grouped[tr.User], tr)
		}
		if len(grouped) > 0 {
			s.state.ResetRecent()
			for user, trs := range grouped {
				s.state.AppendRecent(s.statsWindow, user, s.toHistoryEntries(trs))
			}
		}
	}

	s.logger.Printf("saving state to %s", s.statePath)
	if err := s.state.Save(s.statePath); err != nil {
		s.logger.Printf("state save error: %v", err)
	}
}

// persistJobMirror writes a JSON snapshot of the current job mirror for external inspection.
func (s *Service) persistJobMirror() {
	if s.jobMirrorPath == "" || s.jobs == nil {
		return
	}

	snapshot := s.jobs.Snapshot()

	type jobEntry struct {
		ClusterID           int64  `json:"cluster_id"`
		ProcID              int64  `json:"proc_id"`
		Owner               string `json:"owner"`
		JobStatus           int64  `json:"job_status"`
		GlobalJobID         string `json:"global_job_id"`
		Cmd                 string `json:"cmd,omitempty"`
		Args                string `json:"args,omitempty"`
		Iwd                 string `json:"iwd,omitempty"`
		TransferInput       string `json:"transfer_input,omitempty"`
		TransferOutput      string `json:"transfer_output,omitempty"`
		TransferInputRemaps string `json:"transfer_input_remaps,omitempty"`
		EnteredCurrentState int64  `json:"entered_current_state"`
	}

	entries := make([]jobEntry, 0, len(snapshot))
	for key, job := range snapshot {
		entries = append(entries, jobEntry{
			ClusterID:           key.ClusterID,
			ProcID:              key.ProcID,
			Owner:               job.Owner,
			JobStatus:           job.JobStatus,
			GlobalJobID:         job.GlobalJobID,
			Cmd:                 job.Cmd,
			Args:                job.Args,
			Iwd:                 job.Iwd,
			TransferInput:       job.TransferInput,
			TransferOutput:      job.TransferOutput,
			TransferInputRemaps: job.TransferInputRemaps,
			EnteredCurrentState: job.EnteredCurrentState,
		})
	}

	sort.Slice(entries, func(i, j int) bool {
		if entries[i].ClusterID == entries[j].ClusterID {
			return entries[i].ProcID < entries[j].ProcID
		}
		return entries[i].ClusterID < entries[j].ClusterID
	})

	payload := struct {
		Updated time.Time  `json:"updated"`
		Jobs    []jobEntry `json:"jobs"`
	}{
		Updated: time.Now(),
		Jobs:    entries,
	}

	data, err := json.MarshalIndent(payload, "", "  ")
	if err != nil {
		s.logger.Printf("job mirror marshal error: %v", err)
		return
	}

	if err := os.MkdirAll(filepath.Dir(s.jobMirrorPath), 0o755); err != nil {
		s.logger.Printf("job mirror mkdir error: %v", err)
		return
	}

	tmpPath := s.jobMirrorPath + ".tmp"
	if err := os.WriteFile(tmpPath, data, 0o644); err != nil {
		s.logger.Printf("job mirror write error: %v", err)
		return
	}

	if err := os.Rename(tmpPath, s.jobMirrorPath); err != nil {
		_ = os.Remove(tmpPath)
		s.logger.Printf("job mirror rename error: %v", err)
	}
}

// advertiseOnce prepares ClassAds from the aggregated summaries and sends them to the collector.
func (s *Service) advertiseOnce() {
	if s.oneshoot && s.advertiseDryRun == "" {
		return
	}

	s.updatePairControllers()

	ads := s.buildSummaryAds()
	ads = append(ads, s.buildPairAds()...)
	if len(ads) == 0 {
		return
	}

	if s.advertiseDryRun != "" {
		if err := s.writeDryRunAds(ads); err != nil {
			s.logger.Printf("advertise dry-run error: %v", err)
		}
		return
	}

	if err := s.condor.AdvertiseClassAds(ads); err != nil {
		s.logger.Printf("advertise error: %v", err)
	}
}

// updatePairControllers steps the per-pair AIMD controller and persists state.
func (s *Service) updatePairControllers() {
	if s.tracker == nil || s.state == nil {
		return
	}

	pairs := s.gatherPairs()
	if len(pairs) == 0 {
		return
	}

	now := time.Now()
	controller := control.NewPairController(s.controlCfg)

	for pair := range pairs {
		metrics := pairMetrics(s.controlCfg, s.state, s.tracker, pair.Source, pair.Destination)
		prev := s.state.PairState(pair.Source, pair.Destination)
		next := controller.Step(now, prev, metrics)
		s.state.SetPairState(pair.Source, pair.Destination, next)
	}
}

// gatherPairs returns the union of pairs seen in the tracker window and any persisted pair states.
func (s *Service) gatherPairs() map[control.PairKey]struct{} {
	pairs := make(map[control.PairKey]struct{})

	if s.tracker != nil {
		for _, tr := range s.tracker.AllTransfers() {
			if tr.Source == "" || tr.Destination == "" {
				continue
			}
			pairs[control.PairKey{Source: tr.Source, Destination: tr.Destination}] = struct{}{}
		}
	}

	if s.state != nil {
		for _, key := range s.state.PairKeys() {
			if key.Source == "" || key.Destination == "" {
				continue
			}
			pairs[key] = struct{}{}
		}
	}

	return pairs
}

// buildSummaryAds converts the in-memory aggregates into ClassAds for advertising or inspection.
func (s *Service) buildSummaryAds() []map[string]any {
	_, buckets := s.state.Snapshot()
	ads := make([]map[string]any, 0, len(buckets))

	windowStart := time.Now().Add(-s.statsWindow)
	daemonStart := s.startTime

	windowMetrics := map[summaryWindowKey]outcomeMetrics{}
	if s.tracker != nil {
		windowMetrics = aggregateWindowMetrics(s.tracker.AllTransfers())
	}

	windowRates := map[summaryWindowKey]rateStats{}
	if s.tracker != nil {
		windowRates = aggregateWindowRates(s.tracker.AllTransfers())
	}

	for rawKey, stats := range buckets {
		key := state.DecodeKey(rawKey)
		summaryID := shortSummaryID(rawKey)

		federations := stats.Federations
		if len(federations) == 0 {
			federations = map[string]state.FederationStats{"": {
				Successes:          stats.Successes,
				Failures:           stats.Failures,
				SuccessBytes:       stats.SuccessBytes,
				FailureBytes:       stats.FailureBytes,
				SuccessDurationSec: stats.SuccessDurationSec,
				FailureDurationSec: stats.FailureDurationSec,
			}}
		}

		for prefix, fedStats := range federations {
			success := fedStats.Successes
			failure := fedStats.Failures
			total := success + failure
			rate := 0.0
			if total > 0 {
				rate = float64(success) / float64(total)
			}

			wk := summaryWindowKey{key: key, prefix: prefix}
			wm := windowMetrics[wk]
			windowRate := 0.0
			if wm.success+wm.failure > 0 {
				windowRate = float64(wm.success) / float64(wm.success+wm.failure)
			}

			rateStats := windowRates[wk]
			stageSamples, stagePct := s.state.BucketStageInPercent(jobEpochRetention, rawKey)

			// Compute control loop band classifications
			errorRate := 1.0 - windowRate
			errorBand := control.ClassifyBand(errorRate, s.controlCfg.ErrorGreenThreshold, s.controlCfg.ErrorYellowThreshold)
			costBand := control.ClassifyBand(stagePct/100.0, s.controlCfg.CostGreenThresholdPercent/100.0, s.controlCfg.CostYellowThresholdPercent/100.0)

			name := s.summaryAdName(key, summaryID, prefix)
			summaryVal := rawKey
			if prefix != "" {
				summaryVal = fmt.Sprintf("%s|federation=%s", rawKey, prefix)
			}
			ad := map[string]any{
				"Name":               name,
				"MyType":             "PelicanSummary",
				"Summary":            summaryVal,
				"SummaryID":          summaryID,
				"ScheddName":         s.scheddName,
				"User":               key.User,
				"Endpoint":           key.Endpoint,
				"Site":               key.Site,
				"Direction":          string(key.Direction),
				"Updated":            stats.LastUpdated.Unix(),
				"StatsWindowSeconds": int64(s.statsWindow.Seconds()),
				"StatsWindowStart":   windowStart.Unix(),
				"DaemonStart":        daemonStart.Unix(),
				// Window-scoped counters
				"WindowSuccessCount":          wm.success,
				"WindowFailureCount":          wm.failure,
				"WindowTotalCount":            wm.success + wm.failure,
				"WindowSuccessBytes":          wm.successBytes,
				"WindowFailureBytes":          wm.failureBytes,
				"WindowTotalBytes":            wm.successBytes + wm.failureBytes,
				"WindowSuccessDurationSec":    wm.successDurationSec,
				"WindowFailureDurationSec":    wm.failureDurationSec,
				"WindowTotalDurationSec":      wm.successDurationSec + wm.failureDurationSec,
				"WindowSuccessRate":           windowRate,
				"WindowRateAvgBytesPerSec":    rateStats.avg,
				"WindowRateMedianBytesPerSec": rateStats.median,
				"WindowRateP10BytesPerSec":    rateStats.p10,
				"WindowRateP90BytesPerSec":    rateStats.p90,
				"WindowRateSamples":           rateStats.count,
				"StageInPercent":              stagePct,
				"StageInSamples":              stageSamples,
				"ControlErrorBand":            errorBand.String(),
				"ControlCostBand":             costBand.String(),
				// Cumulative totals since daemon start
				"TotalSuccessCount":       success,
				"TotalFailureCount":       failure,
				"TotalCount":              total,
				"TotalSuccessBytes":       stats.SuccessBytes,
				"TotalFailureBytes":       stats.FailureBytes,
				"TotalBytes":              stats.SuccessBytes + stats.FailureBytes,
				"TotalSuccessDurationSec": stats.SuccessDurationSec,
				"TotalFailureDurationSec": stats.FailureDurationSec,
				"TotalDurationSec":        stats.SuccessDurationSec + stats.FailureDurationSec,
				"TotalSuccessRate":        rate,
			}

			if prefix != "" {
				if key.Direction == state.DirectionDownload {
					ad["FederationSourcePrefix"] = prefix
				} else {
					ad["FederationDestinationPrefix"] = prefix
				}
			}

			ads = append(ads, ad)
		}
	}

	if s.tracker != nil {
		details := s.tracker.SandboxDetails()
		for _, sb := range details {
			files := strings.Join(sb.Paths, ",")

			ads = append(ads, map[string]any{
				"Name":               s.sandboxAdName(sb.Name),
				"MyType":             "PelicanSandbox",
				"ScheddName":         s.scheddName,
				"SandboxName":        sb.Name,
				"SandboxSize":        sb.SizeBytes,
				"ObjectCount":        len(sb.Paths),
				"SandboxFiles":       files,
				"WindowSuccessCount": sb.Successes,
				"WindowFailureCount": sb.Failures,
				"WindowTotalCount":   sb.Successes + sb.Failures,
				"TotalSuccessCount":  sb.Successes,
				"TotalFailureCount":  sb.Failures,
				"TotalCount":         sb.Successes + sb.Failures,
				"StatsWindowSeconds": int64(s.statsWindow.Seconds()),
				"StatsWindowStart":   windowStart.Unix(),
				"DaemonStart":        daemonStart.Unix(),
			})
		}
	}

	return ads
}

// buildPairAds emits capacity ads for each observed (source,destination) pair.
func (s *Service) buildPairAds() []map[string]any {
	pairs := s.gatherPairs()
	if len(pairs) == 0 {
		return nil
	}

	ads := make([]map[string]any, 0, len(pairs))
	windowSeconds := int64(s.statsWindow.Seconds())
	now := time.Now()

	for pair := range pairs {
		metrics := pairMetrics(s.controlCfg, s.state, s.tracker, pair.Source, pair.Destination)
		stageSamples, stagePct := 0, 0.0
		if s.state != nil {
			stageSamples, stagePct = s.state.PairStageInPercent(jobEpochRetention, pair.Source, pair.Destination)
		}

		state := s.state.PairState(pair.Source, pair.Destination)
		updated := state.LastUpdated
		if updated.IsZero() {
			updated = now
		}

		ads = append(ads, map[string]any{
			"Name":               s.pairAdName(pair.Source, pair.Destination),
			"MyType":             "PelicanPair",
			"ScheddName":         s.scheddName,
			"Source":             pair.Source,
			"Destination":        pair.Destination,
			"Updated":            updated.Unix(),
			"CapacityGBPerMin":   state.CapacityGBPerMin,
			"ErrorRate":          metrics.ErrorRate,
			"StageInPercent":     stagePct,
			"StageInSamples":     stageSamples,
			"JobCostGB":          metrics.JobCostGB,
			"StatsWindowSeconds": windowSeconds,
		})
	}

	return ads
}

// outcomeMetrics captures aggregate results for a summary key over the stats window.
type outcomeMetrics struct {
	success            int
	failure            int
	successBytes       int64
	failureBytes       int64
	successDurationSec float64
	failureDurationSec float64
}

type rateStats struct {
	avg    float64
	median float64
	p10    float64
	p90    float64
	count  int
}

type summaryWindowKey struct {
	key    state.SummaryKey
	prefix string
}

func addOutcome(metrics map[summaryWindowKey]outcomeMetrics, key summaryWindowKey, tr stats.ProcessedTransfer) {
	m := metrics[key]
	if tr.Success {
		m.success++
		m.successBytes += tr.Bytes
		m.successDurationSec += tr.Duration.Seconds()
	} else {
		m.failure++
		m.failureBytes += tr.Bytes
		m.failureDurationSec += tr.Duration.Seconds()
	}
	metrics[key] = m
}

// aggregateWindowMetrics groups transfers by summary key and federation prefix for window reporting.
func aggregateWindowMetrics(transfers []stats.ProcessedTransfer) map[summaryWindowKey]outcomeMetrics {
	metrics := make(map[summaryWindowKey]outcomeMetrics)
	for _, tr := range transfers {
		key := state.SummaryKey{User: tr.User, Endpoint: tr.Endpoint, Site: tr.Site, Direction: tr.Direction}
		baseKey := summaryWindowKey{key: key}
		addOutcome(metrics, baseKey, tr)
		if tr.FederationPrefix != "" {
			fedKey := summaryWindowKey{key: key, prefix: tr.FederationPrefix}
			addOutcome(metrics, fedKey, tr)
		}
	}
	return metrics
}

func aggregateWindowRates(transfers []stats.ProcessedTransfer) map[summaryWindowKey]rateStats {
	byKey := make(map[summaryWindowKey][]float64)
	for _, tr := range transfers {
		if !tr.Success {
			continue
		}
		if tr.Duration <= 0 || tr.Bytes <= 0 {
			continue
		}
		rate := float64(tr.Bytes) / tr.Duration.Seconds()
		key := summaryWindowKey{key: state.SummaryKey{User: tr.User, Endpoint: tr.Endpoint, Site: tr.Site, Direction: tr.Direction}}
		byKey[key] = append(byKey[key], rate)
		if tr.FederationPrefix != "" {
			fkey := summaryWindowKey{key: key.key, prefix: tr.FederationPrefix}
			byKey[fkey] = append(byKey[fkey], rate)
		}
	}

	out := make(map[summaryWindowKey]rateStats, len(byKey))
	for k, vals := range byKey {
		if len(vals) == 0 {
			continue
		}
		sort.Float64s(vals)
		sum := 0.0
		for _, v := range vals {
			sum += v
		}
		out[k] = rateStats{
			avg:    sum / float64(len(vals)),
			median: quantile(vals, 0.5),
			p10:    quantile(vals, 0.10),
			p90:    quantile(vals, 0.90),
			count:  len(vals),
		}
	}
	return out
}

// quantile returns the value at the given quantile in a sorted slice (0<=q<=1).
func quantile(sorted []float64, q float64) float64 {
	if len(sorted) == 0 {
		return 0
	}
	if q <= 0 {
		return sorted[0]
	}
	if q >= 1 {
		return sorted[len(sorted)-1]
	}
	idx := q * float64(len(sorted)-1)
	i := int(idx)
	frac := idx - float64(i)
	if i+1 >= len(sorted) {
		return sorted[i]
	}
	return sorted[i] + frac*(sorted[i+1]-sorted[i])
}

// federationPrefix builds a federation-aware prefix including scheme and namespace (e.g., osdf:///ospool/ap40).
func federationPrefix(rawURL string) string {
	u, err := url.Parse(rawURL)
	if err != nil {
		return ""
	}

	pathPrefix := stats.GuessPrefix(rawURL)
	if pathPrefix == "" {
		pathPrefix = u.Path
	}
	if pathPrefix != "" && !strings.HasPrefix(pathPrefix, "/") {
		pathPrefix = "/" + pathPrefix
	}

	if u.Scheme == "" {
		return pathPrefix
	}

	host := u.Host
	return fmt.Sprintf("%s://%s%s", u.Scheme, host, pathPrefix)
}

// normalizeSandboxObject mirrors the normalization used for sandbox hashing and preserves scheme/host for federation disambiguation.
func normalizeSandboxObject(rawURL string) string {
	u, err := url.Parse(rawURL)
	if err != nil {
		return ""
	}
	u.RawQuery = ""
	u.Fragment = ""
	cleanPath := path.Clean("/" + strings.TrimPrefix(u.Path, "/"))

	if u.Scheme != "" && u.Host != "" {
		return fmt.Sprintf("%s://%s%s", u.Scheme, u.Host, cleanPath)
	}
	if u.Host != "" {
		return u.Host + cleanPath
	}
	return cleanPath
}

// summarizeVirtual picks a representative virtual source or destination for a bucket based on processed transfers.
func summarizeVirtual(dir state.Direction, transfers []stats.ProcessedTransfer) (string, string) {
	if len(transfers) == 0 {
		return "", ""
	}

	first := transfers[0]
	switch dir {
	case state.DirectionDownload:
		return first.Source, ""
	case state.DirectionUpload:
		return "", first.Destination
	default:
		return first.Source, first.Destination
	}
}

// writeDryRunAds serializes the ads to a file instead of advertising them to the collector.
func (s *Service) writeDryRunAds(ads []map[string]any) error {
	data, err := json.MarshalIndent(ads, "", "  ")
	if err != nil {
		return fmt.Errorf("marshal ads: %w", err)
	}

	if dir := filepath.Dir(s.advertiseDryRun); dir != "." && dir != "" {
		if err := os.MkdirAll(dir, 0o755); err != nil {
			return fmt.Errorf("ensure dry-run directory: %w", err)
		}
	}

	if err := os.WriteFile(s.advertiseDryRun, data, 0o644); err != nil {
		return fmt.Errorf("write ads: %w", err)
	}

	s.logger.Printf("advertise dry-run wrote %d ads to %s", len(ads), s.advertiseDryRun)
	return nil
}

func (s *Service) summaryAdName(key state.SummaryKey, id, federationPrefix string) string {
	schedd := sanitizeLabel(s.scheddName)
	site := sanitizeLabel(key.Site)
	endpoint := sanitizeLabel(key.Endpoint)
	user := sanitizeLabel(key.User)
	dir := sanitizeLabel(string(key.Direction))
	name := fmt.Sprintf("%s_%s_%s_%s_%s", schedd, site, endpoint, user, dir)
	if federationPrefix != "" {
		fedLabel := sanitizeLabel(federationPrefix)
		hash := shortSummaryID(federationPrefix)
		name = fmt.Sprintf("%s_fed_%s_%s", name, fedLabel, hash)
	}
	if id != "" {
		name = fmt.Sprintf("%s_%s", name, id)
	}
	return name
}

// shortSummaryID hashes the raw summary key to a short identifier that can be embedded in the ad name.
func shortSummaryID(rawKey string) string {
	sum := sha256.Sum256([]byte(rawKey))
	return hex.EncodeToString(sum[:4])
}

// sandboxAdName emits a predictable name for the sandbox ad.
func (s *Service) sandboxAdName(sandbox string) string {
	schedd := sanitizeLabel(s.scheddName)
	if schedd == "" {
		schedd = "unknown"
	}
	return fmt.Sprintf("%s_sandbox_%s", schedd, sanitizeLabel(sandbox))
}

// pairAdName returns a stable ad name for a (source,destination) pair.
func (s *Service) pairAdName(source, destination string) string {
	schedd := sanitizeLabel(s.scheddName)
	if schedd == "" {
		schedd = "unknown"
	}
	return fmt.Sprintf("%s_pair_%s_%s", schedd, sanitizeLabel(source), sanitizeLabel(destination))
}

// sanitizeLabel ensures ClassAd names avoid problematic characters and remain reasonably short.
func sanitizeLabel(part string) string {
	part = strings.TrimSpace(part)
	if part == "" {
		return "unknown"
	}

	part = strings.Map(func(r rune) rune {
		switch {
		case r >= 'a' && r <= 'z':
			return r
		case r >= 'A' && r <= 'Z':
			return r
		case r >= '0' && r <= '9':
			return r
		case r == '-' || r == '_':
			return r
		default:
			return '_'
		}
	}, part)

	if len(part) > 40 {
		part = part[:40]
	}

	return part
}

// printFindings renders a concise summary for oneshot mode.
func (s *Service) printFindings(processed int) {
	lastEpoch, buckets := s.state.Snapshot()
	recent := s.state.SnapshotRecent()

	s.logger.Printf("oneshot summary: processed=%d buckets=%d users_with_recent=%d last_epoch=%s", processed, len(buckets), len(recent), lastEpoch)
	for rawKey, stats := range buckets {
		s.logger.Printf("bucket %s success=%d failure=%d updated=%s", rawKey, stats.Successes, stats.Failures, stats.LastUpdated.Format(time.RFC3339))
	}

	if s.tracker != nil {
		tops := s.tracker.TopSandboxes(10)
		if len(tops) > 0 {
			s.logger.Printf("top sandboxes (count,size):")
			for _, sb := range tops {
				s.logger.Printf("  %s count=%d size=%d", sb.Name, sb.Count, sb.SizeBytes)
			}
		}
	}

	for user, entries := range recent {
		if len(entries) == 0 {
			continue
		}
		limit := len(entries)
		if limit > 5 {
			limit = 5
		}
		s.logger.Printf("recent transfers user=%s showing=%d/%d", user, limit, len(entries))
		for i := 0; i < limit; i++ {
			e := entries[len(entries)-limit+i]
			dir := e.Direction
			if dir == "" {
				dir = "unknown"
			}
			s.logger.Printf("  dir=%s %s -> %s bytes=%d success=%t cached=%t ended=%s sandbox=%s size=%d", dir, e.Source, e.Destination, e.Bytes, e.Success, e.Cached, e.EndedAt.Format(time.RFC3339), e.SandboxName, e.SandboxSize)
		}
	}
}

// buildProcessedTransfers converts a transfer record into per-file processed transfers with source/dest inference.
func (s *Service) buildProcessedTransfers(rec condor.TransferRecord) []stats.ProcessedTransfer {
	if len(rec.Files) == 0 {
		return nil
	}

	var out []stats.ProcessedTransfer
	for _, f := range rec.Files {
		resolveNamespace := func() string {
			if s.director != nil {
				if virt, err := s.director.ResolveVirtualSource(f.URL); err == nil && virt != "" {
					s.logger.Printf("director: resolved namespace for url=%s namespace=%s", f.URL, virt)
					return virt
				} else if err != nil {
					s.logger.Printf("director: resolve failed url=%s err=%v", f.URL, err)
				}
			}
			if prefix := stats.GuessPrefix(f.URL); prefix != "" {
				return prefix
			}
			return f.Endpoint
		}

		fedPrefix := federationPrefix(f.URL)
		source := ""
		destination := ""

		switch state.Direction(rec.Direction) {
		case state.DirectionDownload:
			destination = rec.Site
			if f.Cached {
				source = f.Endpoint
			} else {
				source = resolveNamespace()
			}
		case state.DirectionUpload:
			source = rec.Site
			destination = resolveNamespace()
		default:
			source = resolveNamespace()
			destination = rec.Site
		}

		if source == "" {
			source = f.Endpoint
		}
		if destination == "" {
			destination = f.Endpoint
		}

		duration := f.End.Sub(f.Start)
		if f.DurationSec > 0 {
			duration = time.Duration(f.DurationSec * float64(time.Second))
		} else if duration < 0 {
			duration = 0
		}

		normObj := normalizeSandboxObject(f.URL)

		out = append(out, stats.ProcessedTransfer{
			Epoch:            rec.EpochID,
			User:             rec.User,
			Endpoint:         rec.Endpoint,
			Site:             rec.Site,
			Source:           source,
			Destination:      destination,
			Direction:        state.Direction(rec.Direction),
			FederationPrefix: fedPrefix,
			SandboxObject:    normObj,
			Bytes:            f.Bytes,
			Duration:         duration,
			JobRuntime:       rec.JobRuntime,
			Success:          f.Success && rec.Success,
			LastAttempt:      f.Success || rec.Success,
			EndedAt:          f.End,
			Cached:           f.Cached,
			SandboxName:      rec.SandboxName,
			SandboxSize:      rec.SandboxSize,
		})
	}
	return out
}

func (s *Service) toHistoryEntries(transfers []stats.ProcessedTransfer) []state.TransferHistoryEntry {
	out := make([]state.TransferHistoryEntry, 0, len(transfers))
	for _, tr := range transfers {
		out = append(out, state.TransferHistoryEntry{
			Epoch:            tr.Epoch,
			User:             tr.User,
			Endpoint:         tr.Endpoint,
			Site:             tr.Site,
			Source:           tr.Source,
			Destination:      tr.Destination,
			Direction:        string(tr.Direction),
			Bytes:            tr.Bytes,
			DurationSeconds:  tr.Duration.Seconds(),
			Success:          tr.Success,
			LastAttempt:      tr.LastAttempt,
			EndedAt:          tr.EndedAt,
			Cached:           tr.Cached,
			SandboxName:      tr.SandboxName,
			SandboxSize:      tr.SandboxSize,
			FederationPrefix: tr.FederationPrefix,
			SandboxObject:    tr.SandboxObject,
			JobRuntimeSec:    tr.JobRuntime.Seconds(),
		})
	}
	return out
}
