package condor

import (
	"context"
	"crypto/sha256"
	"encoding/json"
	"fmt"
	"log"
	"net/url"
	"path"
	"sort"
	"strings"
	"time"

	"github.com/PelicanPlatform/classad/classad"
	htcondor "github.com/bbockelm/golang-htcondor"
	"github.com/bbockelm/pelican-ap-manager/internal/state"
)

// NewClient creates a Condor-backed client using the golang-htcondor bindings.
func NewClient(collectorAddress, scheddName, siteAttr string) (CondorClient, error) {
	return &htcClient{collector: htcondor.NewCollector(collectorAddress), scheddName: scheddName, siteAttr: siteAttr}, nil
}

// htcClient is a thin wrapper that satisfies the CondorClient interface.
type htcClient struct {
	collector  *htcondor.Collector
	scheddName string
	siteAttr   string
}

func (c *htcClient) FetchTransferEpochs(sinceEpoch state.EpochID, cutoff time.Time) ([]TransferRecord, state.EpochID, error) {
	sinceExpr := sinceExpr(cutoff, sinceEpoch)
	log.Printf("condor: fetch transfer epochs start since=%v cutoff=%v expr=%q", sinceEpoch, cutoff, sinceExpr)
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	location, err := c.collector.LocateDaemon(ctx, "Schedd", c.scheddName)
	if err != nil {
		return nil, sinceEpoch, fmt.Errorf("locate schedd: %w", err)
	}

	schedd := htcondor.NewSchedd(location.Name, location.Address)

	opts := &htcondor.HistoryQueryOptions{
		Source:    htcondor.HistorySourceTransfer,
		Backwards: true,
		Limit:     5000,
		ScanLimit: 10000,
		Since:     sinceExpr,
		Projection: []string{
			"ClusterId",
			"ProcId",
			"Owner",
			"User",
			"GlobalJobId",
			"RunInstanceID",
			"EpochWriteDate",
			"TransferType",
			"TransferProtocol",
			"TransferEndpoint",
			"TransferFileBytes",
			"TransferTotalBytes",
			"TransferStartTime",
			"TransferEndTime",
			"TransferOutcome",
			"TransferSuccess",
			"TransferHost",
			"Machine",
			"RemoteHost",
			"JobStartDate",
			"EnteredCurrentStatus",
			"InputPluginInvocations",
			"InputPluginResultList",
			"OutputPluginResultList",
			"OutputPluginInvocations",
			"MachineAttrGLIDEIN_Site0",
			"MachineAttrGLIDEIN_ResourceName0",
		},
	}

	stream, err := schedd.QueryHistoryStream(ctx, "true", opts, nil)
	if err != nil {
		return nil, sinceEpoch, fmt.Errorf("query transfer history: %w", err)
	}

	var (
		records   []TransferRecord
		newestRun = sinceEpoch
		count     int
	)

	for res := range stream {
		if res.Err != nil {
			return records, newestRun, fmt.Errorf("stream transfer history: %w", res.Err)
		}
		count++
		if count%1000 == 0 {
			log.Printf("transfer history progress: %d ads", count)
		}

		recs, runID := c.convertTransferAd(res.Ad)
		if runID.After(newestRun) {
			newestRun = runID
		}
		records = append(records, recs...)
	}

	if count > 0 && count%1000 != 0 {
		log.Printf("transfer history progress: %d ads", count)
	}

	log.Printf("condor: fetch transfer epochs done count=%d newest=%v", count, newestRun)
	return records, newestRun, nil
}

func (c *htcClient) FetchJobEpochs(sinceEpoch state.EpochID, cutoff time.Time) ([]JobEpochRecord, state.EpochID, error) {
	sinceExpr := sinceExpr(cutoff, sinceEpoch)
	log.Printf("condor: fetch job epochs start since=%v cutoff=%v expr=%q", sinceEpoch, cutoff, sinceExpr)
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	location, err := c.collector.LocateDaemon(ctx, "Schedd", c.scheddName)
	if err != nil {
		return nil, sinceEpoch, fmt.Errorf("locate schedd: %w", err)
	}

	schedd := htcondor.NewSchedd(location.Name, location.Address)

	projection := []string{
		"ClusterId",
		"ProcId",
		"Owner",
		"User",
		"GlobalJobId",
		"RunInstanceID",
		"EpochWriteDate",
		"JobStartDate",
		"JobCurrentStartDate",
		"EnteredCurrentStatus",
		"ActivationDuration",
		"ActivationExecutionDuration",
		"RemoteWallClockTime",
		"CommittedSlotTime",
		"CommittedSuspensionTime",
		"JobStatus",
		"Machine",
		"RemoteHost",
		"MachineAttrGLIDEIN_Site0",
		"MachineAttrGLIDEIN_ResourceName0",
	}
	if c.siteAttr != "" {
		projection = append(projection, c.siteAttr)
	}

	opts := &htcondor.HistoryQueryOptions{
		Backwards:  true,
		Limit:      5000,
		ScanLimit:  10000,
		Since:      sinceExpr,
		Projection: projection,
	}

	stream, err := schedd.QueryHistoryStream(ctx, "true", opts, nil)
	if err != nil {
		return nil, sinceEpoch, fmt.Errorf("query job history: %w", err)
	}

	var (
		records   []JobEpochRecord
		newestRun = sinceEpoch
		count     int
	)

	for res := range stream {
		if res.Err != nil {
			return records, newestRun, fmt.Errorf("stream job history: %w", res.Err)
		}
		count++
		if count%1000 == 0 {
			log.Printf("job history progress: %d ads", count)
		}

		rec, runID := c.convertJobEpochAd(res.Ad)
		if runID.After(newestRun) {
			newestRun = runID
		}
		if rec != nil {
			records = append(records, *rec)
		}
	}

	if count > 0 && count%1000 != 0 {
		log.Printf("job history progress: %d ads", count)
	}

	log.Printf("condor: fetch job epochs done count=%d newest=%v", count, newestRun)
	return records, newestRun, nil
}

// sinceExpr builds a stop condition for history iteration; iteration halts once the
// expression evaluates true. We stop when records fall before the time cutoff or
// at/below the last processed epoch tuple.
func sinceExpr(cutoff time.Time, since state.EpochID) string {
	var parts []string
	if !cutoff.IsZero() {
		parts = append(parts, fmt.Sprintf("(EpochWriteDate < %d)", cutoff.Unix()))
	}
	if !since.IsZero() {
		parts = append(parts, epochStopExpr(since))
	}
	return strings.Join(parts, " || ")
}

// epochStopExpr is true when the current ad is at or before the last processed epoch.
func epochStopExpr(since state.EpochID) string {
	return fmt.Sprintf("(ClusterId == %d && ProcId == %d && RunInstanceID == %d)", since.ClusterID, since.ProcID, since.RunInstanceID)
}

func epochFromAd(ad *classad.ClassAd) state.EpochID {
	cluster, _ := ad.EvaluateAttrInt("ClusterId")
	proc, _ := ad.EvaluateAttrInt("ProcId")
	run, _ := ad.EvaluateAttrInt("RunInstanceID")
	// RunInstanceID is bounded by condor; avoid substituting EpochWriteDate so the
	// persisted epoch tuple reflects the real RunInstanceID (or zero when absent).
	return state.EpochID{ClusterID: cluster, ProcID: proc, RunInstanceID: run}
}

func (c *htcClient) AdvertiseClassAds(payload []map[string]any) error {
	ctx := context.Background()
	for _, adMap := range payload {
		ad := classad.New()
		for k, v := range adMap {
			if err := ad.Set(k, v); err != nil {
				return fmt.Errorf("set classad attribute %s: %w", k, err)
			}
		}
		// Use MyType to determine the correct command; default matches condor generic ads.
		if err := c.collector.Advertise(ctx, ad, &htcondor.AdvertiseOptions{}); err != nil {
			return fmt.Errorf("advertise classad: %w", err)
		}
	}
	return nil
}

// QueryJobs returns job ads matching the provided constraint and projection.
// Used when the job queue log is unavailable.
func (c *htcClient) QueryJobs(ctx context.Context, constraint string, projection []string) ([]*classad.ClassAd, error) {
	ctx, cancel := context.WithTimeout(ctx, 30*time.Second)
	defer cancel()

	location, err := c.collector.LocateDaemon(ctx, "Schedd", c.scheddName)
	if err != nil {
		return nil, fmt.Errorf("locate schedd: %w", err)
	}

	schedd := htcondor.NewSchedd(location.Name, location.Address)
	ads, _, err := schedd.QueryWithOptions(ctx, constraint, &htcondor.QueryOptions{Projection: projection})
	if err != nil {
		return nil, fmt.Errorf("query jobs: %w", err)
	}
	return ads, nil
}

func (c *htcClient) convertTransferAd(ad *classad.ClassAd) ([]TransferRecord, state.EpochID) {
	runID := epochFromAd(ad)
	user := firstString(ad, "Owner", "User")
	endpoint := firstString(ad, "TransferEndpoint", "TransferHost", "TransferProtocol")
	if endpoint == "" {
		endpoint = firstString(ad, "RemoteHost", "Machine")
	}
	site := firstString(ad, c.siteAttr, "MachineAttrGLIDEIN_Site0", "MachineAttrGLIDEIN_ResourceName0")
	if c.siteAttr != "" {
		site = firstString(ad, c.siteAttr, "MachineAttrGLIDEIN_Site0", "MachineAttrGLIDEIN_ResourceName0")
	}
	if site == "" {
		site = firstString(ad, "Machine")
	}

	typeStr := strings.ToUpper(firstString(ad, "TransferType", "TransferClass", "EpochAdType"))
	if typeStr == "" {
		typeStr = transferTypeFromAd(ad)
	}
	direction := state.DirectionUpload
	switch {
	case typeStr == "INPUT" || typeStr == "DOWNLOAD" || strings.Contains(typeStr, "DOWNLOAD"):
		direction = state.DirectionDownload
	case typeStr == "OUTPUT" || typeStr == "UPLOAD" || strings.Contains(typeStr, "UPLOAD"):
		direction = state.DirectionUpload
	}

	success := parseSuccess(ad)
	endTS, _ := ad.EvaluateAttrInt("TransferEndTime")
	endedAt := time.Now()
	if endTS > 0 {
		endedAt = time.Unix(endTS, 0)
	}

	files := extractFiles(ad, endpoint, endedAt)
	if user == "" {
		user = userFromFiles(files)
	}
	if user == "" {
		user = userFromGlobalJobID(firstString(ad, "GlobalJobId"))
	}
	if endpoint == "" && len(files) > 0 {
		endpoint = files[0].LastEndpoint
	}
	sandboxName, sandboxSize := computeSandbox(files)

	jobStart, _ := ad.EvaluateAttrInt("JobStartDate")
	jobRuntime := time.Duration(0)
	if jobStart > 0 && endTS > jobStart {
		jobRuntime = time.Duration(endTS-jobStart) * time.Second
	}

	// Compute wall-clock transfer duration: min(all start times) to max(all end times)
	wallClock := time.Duration(0)
	if len(files) > 0 {
		var minStart, maxEnd time.Time
		sumDuration := 0.0
		for i, f := range files {
			if i == 0 || f.Start.Before(minStart) {
				minStart = f.Start
			}
			if i == 0 || f.End.After(maxEnd) {
				maxEnd = f.End
			}
			sumDuration += f.DurationSec
		}
		wallClock = maxEnd.Sub(minStart)
		// If wall-clock is 0 due to second-level granularity, use sum of individual transfer times
		if wallClock == 0 && sumDuration > 0 {
			wallClock = time.Duration(sumDuration * float64(time.Second))
		}
	}

	var records []TransferRecord
	for _, f := range files {
		fileDir := direction
		if dir, ok := directionFromString(f.Direction); ok {
			fileDir = dir
		} else if dir, ok := directionFromURL(f.URL); ok {
			fileDir = dir
		}

		recUser := user
		if recUser == "" {
			recUser = userFromFiles([]TransferFile{f})
		}

		records = append(records, TransferRecord{
			EpochID:           runID,
			User:              recUser,
			Endpoint:          f.LastEndpoint,
			Site:              site,
			Direction:         string(fileDir),
			Success:           f.Success,
			EndedAt:           endedAt,
			JobRuntime:        jobRuntime,
			Files:             []TransferFile{f},
			SandboxName:       sandboxName,
			SandboxSize:       f.TotalBytes,
			WallClockDuration: wallClock,
		})
	}

	if len(records) == 0 {
		records = append(records, TransferRecord{
			EpochID:           runID,
			User:              user,
			Endpoint:          endpoint,
			Site:              site,
			Direction:         string(direction),
			Success:           success,
			EndedAt:           endedAt,
			JobRuntime:        jobRuntime,
			Files:             files,
			SandboxName:       sandboxName,
			SandboxSize:       sandboxSize,
			WallClockDuration: wallClock,
		})
	}

	return records, runID
}

func (c *htcClient) convertJobEpochAd(ad *classad.ClassAd) (*JobEpochRecord, state.EpochID) {
	runID := epochFromAd(ad)
	status, _ := ad.EvaluateAttrInt("JobStatus")
	success := status == 4

	user := firstString(ad, "Owner", "User")
	if user == "" {
		user = userFromGlobalJobID(firstString(ad, "GlobalJobId"))
	}

	site := firstString(ad, c.siteAttr, "MachineAttrGLIDEIN_Site0", "MachineAttrGLIDEIN_ResourceName0", "RemoteHost", "Machine")

	endTS := firstInt64(ad, "EnteredCurrentStatus", "EpochWriteDate")
	endedAt := time.Now()
	if endTS > 0 {
		endedAt = time.Unix(endTS, 0)
	}

	// ActivationDuration: total time including setup/teardown/stagein/stageout
	// ActivationExecutionDuration: execution time only (may be 0 or missing if setup failed)
	runtimeSec := numericAttr(ad, "ActivationDuration")
	if runtimeSec <= 0 {
		// Fallback to RemoteWallClockTime (cumulative across runs) if ActivationDuration missing
		runtimeSec = numericAttr(ad, "RemoteWallClockTime", "CommittedSlotTime")
	}
	if runtimeSec <= 0 {
		startTS := firstInt64(ad, "JobCurrentStartDate", "JobStartDate")
		if startTS > 0 && endTS > startTS {
			runtimeSec = float64(endTS - startTS)
		}
	}

	executionDurationSec := numericAttr(ad, "ActivationExecutionDuration")

	if !success || runtimeSec <= 0 || user == "" {
		return nil, runID
	}

	return &JobEpochRecord{
		EpochID:           runID,
		User:              user,
		Site:              site,
		Runtime:           time.Duration(runtimeSec * float64(time.Second)),
		ExecutionDuration: time.Duration(executionDurationSec * float64(time.Second)),
		EndedAt:           endedAt,
		Success:           success,
	}, runID
}

func mergeTransferRecords(primary, secondary []TransferRecord) []TransferRecord {
	out := make(map[string]TransferRecord)
	add := func(rec TransferRecord) {
		key := transferRecordKey(rec)
		if existing, ok := out[key]; ok {
			if len(rec.Files) > len(existing.Files) {
				out[key] = rec
			}
			return
		}
		out[key] = rec
	}

	for _, rec := range primary {
		add(rec)
	}
	for _, rec := range secondary {
		add(rec)
	}

	merged := make([]TransferRecord, 0, len(out))
	for _, rec := range out {
		merged = append(merged, rec)
	}
	sort.Slice(merged, func(i, j int) bool {
		if merged[i].EpochID.ClusterID != merged[j].EpochID.ClusterID {
			return merged[i].EpochID.ClusterID < merged[j].EpochID.ClusterID
		}
		if merged[i].EpochID.ProcID != merged[j].EpochID.ProcID {
			return merged[i].EpochID.ProcID < merged[j].EpochID.ProcID
		}
		if merged[i].EpochID.RunInstanceID != merged[j].EpochID.RunInstanceID {
			return merged[i].EpochID.RunInstanceID < merged[j].EpochID.RunInstanceID
		}
		return merged[i].Direction < merged[j].Direction
	})

	return merged
}

func transferRecordKey(rec TransferRecord) string {
	url := rec.Endpoint
	if len(rec.Files) > 0 {
		url = rec.Files[0].URL
	}

	return fmt.Sprintf("%d-%d-%d-%s-%t-%s", rec.EpochID.ClusterID, rec.EpochID.ProcID, rec.EpochID.RunInstanceID, rec.Direction, rec.Success, url)
}

func firstString(ad *classad.ClassAd, names ...string) string {
	for _, n := range names {
		if v, ok := ad.EvaluateAttrString(n); ok {
			if v != "" {
				return v
			}
		}
	}
	return ""
}

func firstInt64(ad *classad.ClassAd, names ...string) int64 {
	for _, n := range names {
		if v, ok := ad.EvaluateAttrInt(n); ok {
			if v > 0 {
				return v
			}
		}
	}
	return 0
}

func parseSuccess(ad *classad.ClassAd) bool {
	if b, ok := ad.EvaluateAttrBool("TransferSuccess"); ok {
		return b
	}
	if outcome, ok := ad.EvaluateAttrString("TransferOutcome"); ok {
		return strings.EqualFold(outcome, "success") || strings.EqualFold(outcome, "ok")
	}
	return true
}

func numericAttr(ad *classad.ClassAd, names ...string) float64 {
	for _, n := range names {
		if v, ok := ad.EvaluateAttrInt(n); ok {
			if v > 0 {
				return float64(v)
			}
		}
	}

	raw, err := ad.MarshalJSON()
	if err != nil {
		return 0
	}

	var payload map[string]any
	if err := json.Unmarshal(raw, &payload); err != nil {
		return 0
	}

	for _, n := range names {
		if v, ok := payload[n]; ok {
			switch val := v.(type) {
			case float64:
				if val > 0 {
					return val
				}
			case json.Number:
				if f, err := val.Float64(); err == nil && f > 0 {
					return f
				}
			case int64:
				if val > 0 {
					return float64(val)
				}
			case int:
				if val > 0 {
					return float64(val)
				}
			}
		}
	}

	return 0
}

type pluginResult struct {
	TransferUrl        string                 `json:"TransferUrl"`
	TransferType       string                 `json:"TransferType"`
	TransferProtocol   string                 `json:"TransferProtocol"`
	TransferFileBytes  int64                  `json:"TransferFileBytes"`
	TransferTotalBytes int64                  `json:"TransferTotalBytes"`
	TransferStartTime  int64                  `json:"TransferStartTime"`
	TransferEndTime    int64                  `json:"TransferEndTime"`
	TransferSuccess    bool                   `json:"TransferSuccess"`
	DeveloperData      map[string]interface{} `json:"DeveloperData"`
}

func extractFiles(ad *classad.ClassAd, defaultEndpoint string, fallbackEnd time.Time) []TransferFile {
	var files []TransferFile

	if raw, err := ad.MarshalJSON(); err == nil {
		var payload map[string]any
		if err := json.Unmarshal(raw, &payload); err == nil {
			lists := [][]any{}
			if list, ok := payload["InputPluginResultList"].([]any); ok {
				lists = append(lists, list)
			}
			if list, ok := payload["OutputPluginResultList"].([]any); ok {
				lists = append(lists, list)
			}

			for _, list := range lists {
				for _, entry := range list {
					buf, _ := json.Marshal(entry)
					var pr pluginResult
					if err := json.Unmarshal(buf, &pr); err != nil {
						continue
					}

					end := fallbackEnd
					start := fallbackEnd
					durationSec := 0.0
					if pr.TransferEndTime > 0 {
						end = time.Unix(pr.TransferEndTime, 0)
					}
					if pr.TransferStartTime > 0 {
						start = time.Unix(pr.TransferStartTime, 0)
					}

					var attempts []TransferAttempt
					endpoint := defaultEndpoint
					cached := false
					totalAttemptBytes := int64(0)
					for i := 0; ; i++ {
						epKey := fmt.Sprintf("Endpoint%d", i)
						ageKey := fmt.Sprintf("DataAge%d", i)
						timeKey := fmt.Sprintf("TransferTime%d", i)
						bytesKey := fmt.Sprintf("TransferFileBytes%d", i)
						epVal, hasEp := pr.DeveloperData[epKey]
						ageVal, hasAge := pr.DeveloperData[ageKey]
						timeVal, hasTime := pr.DeveloperData[timeKey]
						bytesVal, hasBytes := pr.DeveloperData[bytesKey]
						if !hasEp && !hasAge {
							// No indexed data at all; leave defaults.
							break
						}
						attemptEp := endpoint
						if s, ok := epVal.(string); ok && s != "" {
							attemptEp = s
						}
						attemptCached := cached
						switch v := ageVal.(type) {
						case float64:
							attemptCached = v > 0
						case int64:
							attemptCached = v > 0
						case json.Number:
							if iv, err := v.Int64(); err == nil {
								attemptCached = iv > 0
							}
						}

						attemptDuration := 0.0
						if hasTime {
							switch v := timeVal.(type) {
							case float64:
								attemptDuration = v
								durationSec += v
							case json.Number:
								if fv, err := v.Float64(); err == nil {
									attemptDuration = fv
									durationSec += fv
								}
							}
						}

						attemptBytes := int64(0)
						if hasBytes {
							switch v := bytesVal.(type) {
							case float64:
								attemptBytes = int64(v)
								totalAttemptBytes += int64(v)
							case int64:
								attemptBytes = v
								totalAttemptBytes += v
							case json.Number:
								if iv, err := v.Int64(); err == nil {
									attemptBytes = iv
									totalAttemptBytes += iv
								}
							}
						}

						attempts = append(attempts, TransferAttempt{
							Endpoint:    attemptEp,
							Cached:      attemptCached,
							Bytes:       attemptBytes,
							DurationSec: attemptDuration,
						})
						endpoint = attemptEp
						cached = attemptCached
					}

					if len(attempts) == 0 {
						// No indexed attempt data; create single attempt with file-level totals
						attempts = append(attempts, TransferAttempt{
							Endpoint:    endpoint,
							Cached:      cached,
							Bytes:       pr.TransferFileBytes,
							DurationSec: durationSec,
						})
					}

					// Fallback to wall-clock duration if no precise timing was provided.
					if durationSec <= 0 {
						durationSec = end.Sub(start).Seconds()
					}

					dirStr := strings.TrimSpace(pr.TransferType)
					if dirStr == "" {
						if dir, ok := directionFromURL(pr.TransferUrl); ok {
							dirStr = string(dir)
						}
					}

					// Use total bytes from all attempts if available, otherwise use reported totals
					totalBytes := pr.TransferTotalBytes
					if totalAttemptBytes > 0 && totalAttemptBytes > totalBytes {
						totalBytes = totalAttemptBytes
					}

					files = append(files, TransferFile{
						URL:          pr.TransferUrl,
						LastEndpoint: endpoint,
						Bytes:        pr.TransferFileBytes,
						TotalBytes:   totalBytes,
						DurationSec:  durationSec,
						Start:        start,
						End:          end,
						Cached:       cached,
						Success:      pr.TransferSuccess,
						Attempts:     attempts,
						Direction:    dirStr,
					})
				}
			}
		}
	}

	return files
}

func directionFromString(raw string) (state.Direction, bool) {
	switch strings.ToUpper(strings.TrimSpace(raw)) {
	case "DOWNLOAD", "INPUT":
		return state.DirectionDownload, true
	case "UPLOAD", "OUTPUT":
		return state.DirectionUpload, true
	}

	return "", false
}

func directionFromURL(raw string) (state.Direction, bool) {
	u, err := url.Parse(raw)
	if err != nil {
		return "", false
	}

	path := strings.ToLower(u.Path)
	switch {
	case strings.Contains(path, "/input/"):
		return state.DirectionDownload, true
	case strings.Contains(path, "/output/"):
		return state.DirectionUpload, true
	case strings.Contains(path, "out_"):
		return state.DirectionUpload, true
	case strings.Contains(path, "payload"):
		return state.DirectionDownload, true
	}

	return "", false
}

// transferTypeFromAd walks the embedded plugin results to recover a per-file TransferType.
func transferTypeFromAd(ad *classad.ClassAd) string {
	raw, err := ad.MarshalJSON()
	if err != nil {
		return ""
	}

	var payload map[string]any
	if err := json.Unmarshal(raw, &payload); err != nil {
		return ""
	}

	lists := [][]any{}
	if list, ok := payload["InputPluginResultList"].([]any); ok {
		lists = append(lists, list)
	}
	if list, ok := payload["OutputPluginResultList"].([]any); ok {
		lists = append(lists, list)
	}

	for _, list := range lists {
		for _, entry := range list {
			if m, ok := entry.(map[string]any); ok {
				if tt, ok := m["TransferType"].(string); ok {
					if tt = strings.TrimSpace(tt); tt != "" {
						return strings.ToUpper(tt)
					}
				}
			}
		}
	}

	return ""
}

func computeSandbox(files []TransferFile) (string, int64) {
	if len(files) == 0 {
		return "", 0
	}

	paths := make([]string, 0, len(files))
	var size int64
	for _, f := range files {
		// Only count bytes from successful transfers - failed transfers don't contribute to sandbox size
		if f.Success {
			if f.TotalBytes > 0 {
				size += f.TotalBytes
			} else {
				size += f.Bytes
			}
		}

		u, err := url.Parse(f.URL)
		if err != nil {
			continue
		}
		u.RawQuery = ""
		u.Fragment = ""
		u.Path = path.Clean("/" + strings.TrimPrefix(u.Path, "/"))
		paths = append(paths, u.Host+u.Path)
	}

	if len(paths) == 0 {
		return "", size
	}

	sort.Strings(paths)
	h := sha256.New()
	for _, p := range paths {
		h.Write([]byte(p))
		h.Write([]byte{0})
	}

	return fmt.Sprintf("sandbox-%x", h.Sum(nil)[:12]), size
}

func userFromFiles(files []TransferFile) string {
	for _, f := range files {
		u, err := url.Parse(f.URL)
		if err != nil {
			continue
		}
		parts := strings.Split(strings.Trim(u.Path, "/"), "/")
		// Expect /ospool/<group>/data/<user>/...
		for i, p := range parts {
			if p == "data" && i+1 < len(parts) {
				return parts[i+1]
			}
		}
	}
	return ""
}

func userFromGlobalJobID(gjid string) string {
	// Format often resembles schedd#cluster.proc#owner@domain
	if gjid == "" {
		return ""
	}
	parts := strings.Split(gjid, "#")
	if len(parts) < 3 {
		return ""
	}
	owner := parts[len(parts)-1]
	if idx := strings.Index(owner, "@"); idx > 0 {
		owner = owner[:idx]
	}
	return owner
}
