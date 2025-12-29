package integration

import (
	"context"
	"encoding/json"
	"log"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/bbockelm/pelican-ap-manager/internal/daemon"
	"github.com/bbockelm/pelican-ap-manager/internal/state"
	"github.com/bbockelm/pelican-ap-manager/internal/stats"
)

// TestSummarization verifies the summarization logic with historical epoch_history data:
// - Loads epoch_history from test data (without starting HTCondor)
// - Runs a single iteration of job & transfer statistics summarization
// - Outputs JSON PelicanSummary ads to disk
// - Verifies the summaries contain reasonable aggregations
func TestSummarization(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	// Create temporary directory for test state and output
	testDir, err := os.MkdirTemp("", "pelican-summarization-test-*")
	if err != nil {
		t.Fatalf("failed to create temp dir: %v", err)
	}
	defer func() {
		if err := os.RemoveAll(testDir); err != nil {
			t.Logf("failed to clean up temp dir: %v", err)
		}
	}()

	spoolDir := filepath.Join(testDir, "spool")
	if err := os.MkdirAll(spoolDir, 0755); err != nil {
		t.Fatalf("failed to create spool dir: %v", err)
	}

	// Seed epoch_history from sanitized test data
	t.Logf("seeding epoch_history from test data")
	cwd, err := os.Getwd()
	if err != nil {
		t.Fatalf("failed to get cwd: %v", err)
	}
	moduleRoot := filepath.Dir(cwd)
	seedEpochHistory(t, moduleRoot, spoolDir)

	// Verify epoch_history was created and has content
	epochHistoryPath := filepath.Join(spoolDir, "epoch_history")
	stat, err := os.Stat(epochHistoryPath)
	if err != nil {
		t.Fatalf("epoch_history not created: %v", err)
	}
	if stat.Size() == 0 {
		t.Fatal("epoch_history is empty")
	}
	t.Logf("epoch_history seeded: %d bytes", stat.Size())

	// Initialize state
	statePath := filepath.Join(testDir, "state.json")
	st := state.New()

	// Create tracker for windowed statistics
	statsWindow := 24 * time.Hour
	tracker := stats.NewTracker(statsWindow)

	// Create mock condor client that reads from test JSON files
	logger := log.New(os.Stdout, "[test-summarization] ", log.LstdFlags)
	jobEpochsPath := filepath.Join(moduleRoot, "internal", "condor", "testdata", "job_epochs_from_transfers.sanitized.json")
	transfersPath := filepath.Join(moduleRoot, "internal", "condor", "testdata", "transfers.sanitized.json")
	condorClient := newMockCondorClient(jobEpochsPath, transfersPath)

	// Output file for dry-run ads
	adsOutputPath := filepath.Join(testDir, "summary_ads.json")

	// Create service in oneshot mode with dry-run advertising
	service := daemon.NewService(
		condorClient,
		st,
		statePath,
		30*time.Second,       // poll interval (unused in oneshot)
		1*time.Minute,        // advertise interval (unused in oneshot)
		24*time.Hour,         // epoch lookback
		statsWindow,          // stats window
		tracker,              // tracker
		nil,                  // job mirror (not needed for this test)
		"",                   // job mirror path
		nil,                  // director client (not needed)
		logger,               // logger
		adsOutputPath,        // dry-run ads output
		"test-schedd",        // schedd name
		"MATCH_EXP_JOB_Site", // site attribute
		true,                 // oneshot mode
	)

	// Run single iteration
	t.Logf("running single summarization iteration")
	if err := service.Run(ctx); err != nil {
		t.Fatalf("service.Run failed: %v", err)
	}

	// Verify ads were written
	adsData, err := os.ReadFile(adsOutputPath)
	if err != nil {
		t.Fatalf("failed to read ads output: %v", err)
	}

	var ads []map[string]any
	if err := json.Unmarshal(adsData, &ads); err != nil {
		t.Fatalf("failed to unmarshal ads: %v", err)
	}

	if len(ads) == 0 {
		t.Fatal("no ads generated")
	}

	t.Logf("generated %d ads", len(ads))

	// Verify ad structure and content
	var summaryAds []map[string]any
	var sandboxAds []map[string]any
	var pairAds []map[string]any

	for _, ad := range ads {
		myType, ok := ad["MyType"].(string)
		if !ok {
			t.Errorf("ad missing MyType: %+v", ad)
			continue
		}

		switch myType {
		case "PelicanSummary":
			summaryAds = append(summaryAds, ad)
		case "PelicanSandbox":
			sandboxAds = append(sandboxAds, ad)
		case "PelicanPair":
			pairAds = append(pairAds, ad)
		case "PelicanLimit":
			// PelicanLimit ads are expected, just count them
		default:
			t.Errorf("unexpected MyType: %s", myType)
		}
	}

	t.Logf("found %d PelicanSummary ads, %d PelicanSandbox ads, %d PelicanPair ads",
		len(summaryAds), len(sandboxAds), len(pairAds))

	if len(summaryAds) == 0 {
		t.Fatal("no PelicanSummary ads generated")
	}

	// Verify each summary ad has required fields
	for i, ad := range summaryAds {
		requiredFields := []string{
			"Name", "MyType", "Summary", "SummaryID", "ScheddName",
			"User", "Endpoint", "Site", "Direction",
			"WindowAttemptSuccessCount", "WindowAttemptFailureCount", "WindowAttemptTotalCount",
			"WindowAttemptSuccessBytes", "WindowAttemptFailureBytes", "WindowAttemptTotalBytes",
			"TotalAttemptSuccessCount", "TotalAttemptFailureCount", "TotalAttemptCount",
			"TotalAttemptSuccessBytes", "TotalAttemptFailureBytes", "TotalAttemptBytes",
			"StatsWindowSeconds", "WindowAttemptSuccessRate", "TotalAttemptSuccessRate",
		}

		for _, field := range requiredFields {
			if _, ok := ad[field]; !ok {
				t.Errorf("summary ad %d missing field %s", i, field)
			}
		}

		// Verify schedd name
		if schedd, ok := ad["ScheddName"].(string); !ok || schedd != "test-schedd" {
			t.Errorf("summary ad %d has wrong ScheddName: %v (expected 'test-schedd')", i, ad["ScheddName"])
		}

		// Verify direction is valid
		if dir, ok := ad["Direction"].(string); ok {
			if dir != "download" && dir != "upload" {
				t.Errorf("summary ad %d has invalid Direction: %s", i, dir)
			}
		} else {
			t.Errorf("summary ad %d Direction is not a string", i)
		}

		// Verify counts are non-negative
		if total, ok := ad["WindowAttemptTotalCount"].(float64); ok {
			if total < 0 {
				t.Errorf("summary ad %d has negative WindowAttemptTotalCount: %v", i, total)
			}
		}

		if totalBytes, ok := ad["WindowAttemptTotalBytes"].(float64); ok {
			if totalBytes < 0 {
				t.Errorf("summary ad %d has negative WindowAttemptTotalBytes: %v", i, totalBytes)
			}
		}

		// Verify success rate is between 0 and 1
		if rate, ok := ad["WindowAttemptSuccessRate"].(float64); ok {
			if rate < 0 || rate > 1 {
				t.Errorf("summary ad %d has invalid WindowAttemptSuccessRate: %v (expected 0-1)", i, rate)
			}
		}

		// Log sample ad details for inspection
		if i == 0 {
			t.Logf("sample summary ad: User=%v Endpoint=%v Site=%v Direction=%v",
				ad["User"], ad["Endpoint"], ad["Site"], ad["Direction"])
			t.Logf("  WindowAttemptSuccessCount=%v WindowAttemptFailureCount=%v WindowAttemptTotalBytes=%v",
				ad["WindowAttemptSuccessCount"], ad["WindowAttemptFailureCount"], ad["WindowAttemptTotalBytes"])
			t.Logf("  TotalAttemptSuccessCount=%v TotalAttemptFailureCount=%v TotalAttemptBytes=%v",
				ad["TotalAttemptSuccessCount"], ad["TotalAttemptFailureCount"], ad["TotalAttemptBytes"])
		}
	}

	// Verify sandbox ads if present
	for i, ad := range sandboxAds {
		requiredFields := []string{
			"Name", "MyType", "ScheddName", "SandboxName",
			"SandboxSize", "ObjectCount", "EpochSuccessCount",
			"EpochFailureCount", "EpochTotalCount",
		}

		for _, field := range requiredFields {
			if _, ok := ad[field]; !ok {
				t.Errorf("sandbox ad %d missing field %s", i, field)
			}
		}

		if i == 0 {
			t.Logf("sample sandbox ad: SandboxName=%v Size=%v Objects=%v",
				ad["SandboxName"], ad["SandboxSize"], ad["ObjectCount"])
		}
	}

	// Verify pair ads if present
	for i, ad := range pairAds {
		requiredFields := []string{
			"Name", "MyType", "ScheddName", "Source", "Destination",
			"CapacityGBPerMin", "ErrorRate", "StageInPercent",
		}

		for _, field := range requiredFields {
			if _, ok := ad[field]; !ok {
				t.Errorf("pair ad %d missing field %s", i, field)
			}
		}

		if i == 0 {
			t.Logf("sample pair ad: Source=%v Destination=%v CapacityGBPerMin=%v",
				ad["Source"], ad["Destination"], ad["CapacityGBPerMin"])
		}
	}

	// Verify state was updated
	lastEpoch, buckets := st.Snapshot()
	if lastEpoch.IsZero() {
		t.Error("state lastEpoch is zero after processing")
	}
	if len(buckets) == 0 {
		t.Error("no buckets in state after processing")
	}

	t.Logf("state snapshot: lastEpoch=%s buckets=%d", lastEpoch, len(buckets))

	// Verify state file was written
	if _, err := os.Stat(statePath); err != nil {
		t.Errorf("state file not written: %v", err)
	}

	t.Log("summarization test passed")
}
