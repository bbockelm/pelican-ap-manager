//go:build condor

package main

import (
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"log"
	"os"
	"path/filepath"
	"strings"
	"time"

	"github.com/PelicanPlatform/classad/classad"
	htcondor "github.com/bbockelm/golang-htcondor"
	condorconfig "github.com/bbockelm/golang-htcondor/config"
)

// collect transfers from a specific collector/schedd and write a small sample set.
func main() {
	defs := loadDefaults()

	collector := flag.String("collector", defs.collector, "collector host (PELICAN_MANAGER_COLLECTOR_HOST)")
	scheddName := flag.String("schedd", defs.schedd, "schedd name (PELICAN_MANAGER_SCHEDD_NAME)")
	limit := flag.Int("limit", 10, "number of samples to retain")
	output := flag.String("output", "internal/condor/testdata/sample_transfer_ads.json", "output path for samples")
	jobLimit := flag.Int("job_limit", 10, "number of job epoch samples to retain")
	jobOutput := flag.String("job_output", "internal/condor/testdata/sample_job_epochs.json", "output path for job epoch samples")
	siteAttr := flag.String("site_attr", defs.siteAttr, "site attribute name (PELICAN_MANAGER_SITE_ATTRIBUTE)")
	filter := flag.String("filter", "", "comma-separated substrings to require in transfer fields (case-insensitive)")
	flag.Parse()

	logger := log.New(os.Stdout, "collect_transfers ", log.LstdFlags|log.Lmsgprefix)

	ctx, cancel := context.WithTimeout(context.Background(), 45*time.Second)
	defer cancel()

	col := htcondor.NewCollector(*collector)
	logger.Printf("locating schedd %q via collector %s", *scheddName, *collector)

	loc, err := col.LocateDaemon(ctx, "Schedd", *scheddName)
	if err != nil {
		logger.Fatalf("locate schedd: %v", err)
	}

	schedd := htcondor.NewSchedd(loc.Name, loc.Address)

	opts := &htcondor.HistoryQueryOptions{
		Source:        htcondor.HistorySourceTransfer,
		Backwards:     true, // newest first
		Limit:         max(*limit*5, 50),
		ScanLimit:     0,
		TransferTypes: []htcondor.TransferType{htcondor.TransferTypeInput, htcondor.TransferTypeOutput, htcondor.TransferTypeCheckpoint},
		Projection:    []string{"*"},
	}

	filters := splitFilters(*filter)
	transferSamples := make([]map[string]any, 0, *limit)

	stream, err := schedd.QueryHistoryStream(ctx, "true", opts, nil)
	if err != nil {
		logger.Fatalf("query transfer history: %v", err)
	}

	var count int
	for res := range stream {
		if res.Err != nil {
			logger.Fatalf("stream transfer history: %v", res.Err)
		}
		count++
		if count%1000 == 0 {
			logger.Printf("transfer history progress: %d ads", count)
		}

		if len(filters) > 0 && !matchesAny(res.Ad, filters) {
			continue
		}
		transferSamples = append(transferSamples, selectFields(res.Ad, *siteAttr))
		if len(transferSamples) >= *limit {
			break
		}
	}
	if count > 0 && count%1000 != 0 {
		logger.Printf("transfer history progress: %d ads", count)
	}

	if err := writeSamples(*output, transferSamples); err != nil {
		logger.Fatalf("write transfer samples: %v", err)
	}
	logger.Printf("wrote %d transfer samples to %s", len(transferSamples), *output)

	jobSamples, err := collectJobEpochs(ctx, schedd, filters, *jobLimit, *siteAttr, *jobOutput)
	if err != nil {
		logger.Fatalf("query job epochs: %v", err)
	}
	if err := writeSamples(*jobOutput, jobSamples); err != nil {
		logger.Fatalf("write job epoch samples: %v", err)
	}
	logger.Printf("wrote %d job epoch samples to %s", len(jobSamples), *jobOutput)
}

func selectFields(ad *classad.ClassAd, siteAttr string) map[string]any {
	// Preserve the full ad, including lists and nested data, via JSON marshal/unmarshal.
	raw := make(map[string]any)
	if b, err := ad.MarshalJSON(); err == nil {
		_ = json.Unmarshal(b, &raw)
	}
	// ensure site attribute is present if casing differs
	if _, ok := raw[siteAttr]; !ok {
		for k, v := range raw {
			if strings.EqualFold(k, siteAttr) {
				raw[siteAttr] = v
				break
			}
		}
	}
	return raw
}

func matchesAny(ad *classad.ClassAd, filters []string) bool {
	if len(filters) == 0 {
		return true
	}

	for _, attr := range ad.GetAttributes() {
		lname := strings.ToLower(attr)
		if v, ok := ad.EvaluateAttrString(attr); ok {
			lv := strings.ToLower(v)
			for _, needle := range filters {
				if strings.Contains(lv, needle) || strings.Contains(lname, needle) {
					return true
				}
			}
			continue
		}
	}

	return false
}

type defaults struct {
	collector string
	schedd    string
	siteAttr  string
}

func loadDefaults() defaults {
	cfg, err := condorconfig.New()
	if err != nil {
		return defaults{collector: "localhost:9618", schedd: "", siteAttr: "Site"}
	}

	d := defaults{collector: "localhost:9618", schedd: "", siteAttr: "Site"}
	if v, ok := cfg.Get("PELICAN_MANAGER_COLLECTOR_HOST"); ok && v != "" {
		d.collector = v
	} else if v, ok := cfg.Get("COLLECTOR_HOST"); ok && v != "" { // legacy
		d.collector = v
	}
	if v, ok := cfg.Get("PELICAN_MANAGER_SCHEDD_NAME"); ok && v != "" {
		d.schedd = v
	} else if v, ok := cfg.Get("SCHEDD_NAME"); ok && v != "" { // legacy
		d.schedd = v
	}
	if v, ok := cfg.Get("PELICAN_MANAGER_SITE_ATTRIBUTE"); ok && v != "" {
		d.siteAttr = v
	} else if v, ok := cfg.Get("PEL_SITE_ATTRIBUTE"); ok && v != "" { // legacy
		d.siteAttr = v
	}
	return d
}

func splitFilters(raw string) []string {
	if raw == "" {
		return nil
	}
	parts := strings.Split(raw, ",")
	out := make([]string, 0, len(parts))
	for _, p := range parts {
		p = strings.TrimSpace(strings.ToLower(p))
		if p != "" {
			out = append(out, p)
		}
	}
	return out
}

func min(a, b int) int {
	if a < b {
		return a
	}
	return b
}

func max(a, b int) int {
	if a > b {
		return a
	}
	return b
}

func collectJobEpochs(ctx context.Context, schedd *htcondor.Schedd, filters []string, limit int, siteAttr, outputPath string) ([]map[string]any, error) {
	opts := &htcondor.HistoryQueryOptions{
		Backwards: true, // newest first
		Limit:     max(limit*5, 50),
		ScanLimit: 0,
		Projection: []string{
			"*",
		},
	}
	if last := loadLastEpochWriteDate(outputPath); last > 0 {
		opts.Since = fmt.Sprintf("EpochWriteDate <= %d", last)
	}

	stream, err := schedd.QueryHistoryStream(ctx, "true", opts, nil)
	if err != nil {
		return nil, err
	}

	var (
		samples []map[string]any
		count   int
	)
	for res := range stream {
		if res.Err != nil {
			return samples, res.Err
		}
		count++
		if count%1000 == 0 {
			log.Printf("job epoch history progress: %d ads", count)
		}

		if len(filters) > 0 && !matchesAny(res.Ad, filters) {
			continue
		}
		samples = append(samples, selectFields(res.Ad, siteAttr))
		if len(samples) >= limit {
			break
		}
	}
	if count > 0 && count%1000 != 0 {
		log.Printf("job epoch history progress: %d ads", count)
	}

	return samples, nil
}

func loadLastEpochWriteDate(path string) int64 {
	data, err := os.ReadFile(path)
	if err != nil {
		return 0
	}
	var records []map[string]any
	if err := json.Unmarshal(data, &records); err != nil {
		return 0
	}
	var maxTS int64
	for _, rec := range records {
		if v, ok := rec["EpochWriteDate"]; ok {
			switch ts := v.(type) {
			case float64:
				if int64(ts) > maxTS {
					maxTS = int64(ts)
				}
			case int64:
				if ts > maxTS {
					maxTS = ts
				}
			}
		}
	}
	return maxTS
}

func writeSamples(path string, samples []map[string]any) error {
	if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
		return fmt.Errorf("mkdir output dir: %w", err)
	}

	f, err := os.Create(path)
	if err != nil {
		return fmt.Errorf("create output: %w", err)
	}
	defer f.Close()

	enc := json.NewEncoder(f)
	enc.SetIndent("", "  ")
	if err := enc.Encode(samples); err != nil {
		return fmt.Errorf("encode output: %w", err)
	}

	return nil
}
