//go:build integration

package integration

import (
	"context"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"testing"
	"time"

	htcondor "github.com/bbockelm/golang-htcondor"
	condorconfig "github.com/bbockelm/golang-htcondor/config"
	"github.com/bbockelm/pelican-ap-manager/internal/condor"
	"github.com/bbockelm/pelican-ap-manager/internal/state"
)

func TestFakeTransferPluginIntegration(t *testing.T) {
	if _, err := exec.LookPath("condor_master"); err != nil {
		t.Skip("condor_master not found; skipping integration test")
	}

	keepTemp := os.Getenv("KEEP_INTEGRATION_TEMP") == "1"
	rootDir := t.TempDir()
	projectRoot := moduleRoot(t)
	if keepTemp {
		// When debugging test artifacts, keep the temp directory instead of letting Go clean it up.
		var err error
		rootDir, err = os.MkdirTemp("", "TestFakeTransferPluginIntegration")
		if err != nil {
			t.Fatalf("create preserved temp dir: %v", err)
		}
		// avoid the automatic t.TempDir cleanup; caller can remove this directory manually.
		if err := os.Setenv("TEST_PRESERVED_DIR", rootDir); err == nil {
			t.Logf("KEEP_INTEGRATION_TEMP set; preserved dir exported via TEST_PRESERVED_DIR=%s", rootDir)
		}
	}
	t.Logf("fake transfer plugin root dir: %s", rootDir)
	socketDir, err := os.MkdirTemp("/tmp", "pelsp_")
	if err != nil {
		t.Fatalf("socket dir: %v", err)
	}
	if !keepTemp {
		t.Cleanup(func() { _ = os.RemoveAll(socketDir) })
	}

	statePath := filepath.Join(rootDir, "pelican_state.json")
	mirrorPath := filepath.Join(rootDir, "job_mirror.json")
	configPath := filepath.Join(rootDir, "condor_config")

	if err := writeMiniCondorConfig(configPath, rootDir, socketDir, statePath, mirrorPath, t); err != nil {
		t.Fatalf("write condor config: %v", err)
	}
	if err := os.Setenv("CONDOR_CONFIG", configPath); err != nil {
		t.Fatalf("set CONDOR_CONFIG: %v", err)
	}

	seedEpochHistory(t, projectRoot, filepath.Join(rootDir, "spool"))

	pluginPath, err := buildFakeTransferPlugin(t, rootDir)
	if err != nil {
		t.Fatalf("build fake transfer plugin: %v", err)
	}

	if err := appendConfigOverride(configPath, "FILETRANSFER_PLUGINS", pluginPath); err != nil {
		t.Fatalf("config filetransfer plugins: %v", err)
	}

	// Increase starter logging to see plugin initialization details.
	if err := appendConfigOverride(configPath, "STARTER_DEBUG", "D_FULLDEBUG"); err != nil {
		t.Fatalf("increase starter debug: %v", err)
	}

	if err := appendConfigOverride(configPath, "SHADOW_DEBUG", "D_FULLDEBUG"); err != nil {
		t.Fatalf("increase shadow debug: %v", err)
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	condorCmd, err := startCondorMaster(ctx, configPath, rootDir)
	if err != nil {
		t.Fatalf("start condor_master: %v", err)
	}
	t.Cleanup(func() { stopCondorMaster(condorCmd, t) })

	if err := waitForCondor(rootDir, 60*time.Second, t); err != nil {
		printHTCondorLogs(rootDir, t)
		t.Fatalf("condor readiness: %v", err)
	}

	collectorAddr, err := discoverCollectorAddress(rootDir, 10*time.Second)
	if err != nil {
		printHTCondorLogs(rootDir, t)
		t.Fatalf("collector address discovery: %v", err)
	}
	collectorHostPort := stripHostPort(collectorAddr)

	scheddAddr, err := getScheddAddress(rootDir, 10*time.Second)
	if err != nil {
		printHTCondorLogs(rootDir, t)
		t.Fatalf("schedd address: %v", err)
	}

	scheddName := "integration_schedd"
	if cfg, err := condorconfig.New(); err == nil {
		if v, ok := cfg.Get("PELICAN_MANAGER_SCHEDD_NAME"); ok && v != "" {
			scheddName = v
		} else if v, ok := cfg.Get("SCHEDD_NAME"); ok && v != "" {
			scheddName = v
		}
	}

	cases := []struct {
		name          string
		inputStatus   int
		outputStatus  int
		expectJobStat int
	}{
		{name: "success", inputStatus: 0, outputStatus: 0, expectJobStat: 4},
		{name: "input_fail", inputStatus: 5, outputStatus: 0, expectJobStat: 5},
		{name: "output_fail", inputStatus: 0, outputStatus: 7, expectJobStat: 5},
	}

	var clusterIDs []int64
	for _, tc := range cases {
		clusterID := submitPluginJob(ctx, t, rootDir, scheddAddr, tc.name, tc.inputStatus, tc.outputStatus)
		t.Logf("submitted %s cluster %d", tc.name, clusterID)
		clusterIDs = append(clusterIDs, clusterID)
		waitForJobToReachStatus(ctx, t, rootDir, scheddAddr, clusterID, tc.expectJobStat, 45*time.Second, tc.name)
	}

	client, err := condor.NewClient(collectorHostPort, scheddName, "")
	if err != nil {
		t.Fatalf("condor client: %v", err)
	}

	clusterSet := make(map[int64]struct{}, len(clusterIDs))
	for _, id := range clusterIDs {
		clusterSet[id] = struct{}{}
	}

	deadline := time.Now().Add(15 * time.Second)
	var lastTransfers []condor.TransferRecord
	for {
		transfers, _, err := client.FetchTransferEpochs(state.EpochID{}, time.Time{})
		if err != nil {
			t.Fatalf("fetch transfer history: %v", err)
		}
		lastTransfers = transfers

		needed := map[string]bool{
			"download_success": false,
			"download_failure": false,
			"upload_success":   false,
			"upload_failure":   false,
		}

		for _, tr := range transfers {
			if _, ok := clusterSet[tr.EpochID.ClusterID]; !ok {
				continue
			}

			switch tr.Direction {
			case string(state.DirectionDownload):
				if tr.Success {
					needed["download_success"] = true
				} else {
					needed["download_failure"] = true
				}
			case string(state.DirectionUpload):
				if tr.Success {
					needed["upload_success"] = true
				} else {
					needed["upload_failure"] = true
				}
			}
		}

		allSeen := true
		missing := make([]string, 0)
		for key, seen := range needed {
			if !seen {
				allSeen = false
				missing = append(missing, key)
			}
		}
		if allSeen {
			break
		}

		if time.Now().After(deadline) {
			for _, tr := range lastTransfers {
				t.Logf("transfer ad: cluster=%d direction=%s success=%t endpoint=%s files=%d", tr.EpochID.ClusterID, tr.Direction, tr.Success, tr.Endpoint, len(tr.Files))
			}
			t.Fatalf("missing transfer outcomes after wait: %v", missing)
		}

		time.Sleep(500 * time.Millisecond)
	}
}

func buildFakeTransferPlugin(t *testing.T, workDir string) (string, error) {
	pluginPath := filepath.Join(workDir, "fake_transfer_plugin")

	cwd, err := os.Getwd()
	if err != nil {
		return "", fmt.Errorf("getwd: %w", err)
	}
	moduleRoot := filepath.Dir(cwd)

	cmd := exec.Command("go", "build", "-o", pluginPath, "./tools/fake_transfer_plugin")
	cmd.Dir = moduleRoot
	cmd.Env = os.Environ()
	output, err := cmd.CombinedOutput()
	if err != nil {
		return "", fmt.Errorf("go build fake plugin: %v (%s)", err, string(output))
	}

	return pluginPath, nil
}

func submitPluginJob(ctx context.Context, t *testing.T, workDir, scheddAddr, name string, inputStatus, outputStatus int) int64 {
	scriptPath := filepath.Join(workDir, fmt.Sprintf("job_%s.sh", name))
	if err := os.WriteFile(scriptPath, []byte(fmt.Sprintf("#!/bin/sh\nset -e\ncat payload.txt >/dev/null 2>&1 || true\necho %s > out_%s.txt\n", name, name)), 0o755); err != nil {
		t.Fatalf("write job script: %v", err)
	}

	inputResult := filepath.Join(workDir, fmt.Sprintf("input_result_%s.txt", name))
	outputResult := filepath.Join(workDir, fmt.Sprintf("output_result_%s.txt", name))

	remotePayload := fmt.Sprintf("fake://test/input/%s/payload.txt", name)
	remoteOutput := fmt.Sprintf("fake://test/output/%s/out_%s.txt", name, name)

	submit := fmt.Sprintf(
		"executable = %s\n"+
			"output = stdout_%s.txt\n"+
			"error = stderr_%s.txt\n"+
			"log = log_%s.txt\n"+
			"initialdir = %s\n"+
			"transfer_input_files = %s\n"+
			"transfer_output_files = out_%s.txt\n"+
			"transfer_output_remaps = out_%s.txt=%s\n"+
			"should_transfer_files = YES\n"+
			"when_to_transfer_output = ON_EXIT\n"+
			"leave_in_queue = (CompletionDate == 0) || ((time() - CompletionDate) < 60)\n"+
			"+FakeInputResultFile = \"%s\"\n"+
			"+FakeInputResultStatus = %d\n"+
			"+FakeOutputResultFile = \"%s\"\n"+
			"+FakeOutputResultStatus = %d\n"+
			"queue\n",
		scriptPath,
		name,
		name,
		name,
		workDir,
		remotePayload,
		name,
		name,
		remoteOutput,
		inputResult,
		inputStatus,
		outputResult,
		outputStatus,
	)

	schedd := htcondor.NewSchedd("integration_schedd", scheddAddr)
	submitCtx, cancel := context.WithTimeout(ctx, 30*time.Second)
	defer cancel()

	clusterStr, err := schedd.Submit(submitCtx, submit)
	if err != nil {
		t.Fatalf("submit job %s: %v", name, err)
	}

	var clusterID int64
	if _, err := fmt.Sscanf(clusterStr, "%d", &clusterID); err != nil {
		t.Fatalf("parse cluster id: %v", err)
	}

	editCtx, editCancel := context.WithTimeout(ctx, 10*time.Second)
	defer editCancel()
	if err := schedd.EditJob(editCtx, int(clusterID), 0, map[string]string{
		"FakeInputResultFile":    fmt.Sprintf("\"%s\"", inputResult),
		"FakeInputResultStatus":  fmt.Sprintf("%d", inputStatus),
		"FakeOutputResultFile":   fmt.Sprintf("\"%s\"", outputResult),
		"FakeOutputResultStatus": fmt.Sprintf("%d", outputStatus),
	}, nil); err != nil {
		t.Fatalf("attach fake plugin attrs to job %s: %v", name, err)
	}
	return clusterID
}

func waitForJobToReachStatus(ctx context.Context, t *testing.T, rootDir, scheddAddr string, clusterID int64, desiredStatus int, timeout time.Duration, jobName string) {
	deadline := time.Now().Add(timeout)
	schedd := htcondor.NewSchedd("integration_schedd", scheddAddr)
	lastStatus := -1
	lastHold := ""

	for time.Now().Before(deadline) {
		queryCtx, cancel := context.WithTimeout(ctx, 10*time.Second)
		ads, _, err := schedd.QueryWithOptions(queryCtx, fmt.Sprintf("ClusterId == %d", clusterID), &htcondor.QueryOptions{Projection: []string{"JobStatus", "HoldReason"}})
		cancel()
		if err != nil {
			time.Sleep(time.Second)
			continue
		}
		if len(ads) == 0 {
			if histStatus, histHold, found := lookupJobHistory(ctx, scheddAddr, clusterID); found {
				lastStatus = histStatus
				lastHold = histHold
				if histStatus == desiredStatus {
					return
				}
			}
			time.Sleep(time.Second)
			continue
		}
		if js, ok := ads[0].EvaluateAttrInt("JobStatus"); ok {
			lastStatus = int(js)
			if desiredStatus == 5 && int(js) == 4 {
				printHTCondorLogs(rootDir, t)
				dumpJobAd(t, scheddAddr, clusterID)
				t.Fatalf("job %d (%s) completed (JobStatus=4) while waiting for hold (JobStatus=5); aborting", clusterID, jobName)
			}
			if int(js) == desiredStatus {
				return
			}
			if int(js) == 5 && desiredStatus != 5 {
				hold := ""
				if hr, ok := ads[0].EvaluateAttrString("HoldReason"); ok {
					hold = hr
				}
				lastHold = hold
				printHTCondorLogs(rootDir, t)
				t.Fatalf("job %d entered hold (JobStatus=5) before reaching status %d; HoldReason=%s", clusterID, desiredStatus, hold)
			}
		}
		time.Sleep(time.Second)
	}

	printHTCondorLogs(rootDir, t)
	dumpJobAd(t, scheddAddr, clusterID)
	if lastStatus == -1 {
		t.Fatalf("job %d did not reach status %d (no ads returned)", clusterID, desiredStatus)
	}
	if lastHold != "" {
		t.Fatalf("job %d did not reach status %d (last status=%d, hold=%s)", clusterID, desiredStatus, lastStatus, lastHold)
	}
	t.Fatalf("job %d did not reach status %d (last status=%d)", clusterID, desiredStatus, lastStatus)
}

// lookupJobHistory checks if the job already landed in history and returns its status for debugging.
func lookupJobHistory(ctx context.Context, scheddAddr string, clusterID int64) (int, string, bool) {
	schedd := htcondor.NewSchedd("integration_schedd", scheddAddr)
	queryCtx, cancel := context.WithTimeout(ctx, 10*time.Second)
	defer cancel()
	stream, err := schedd.QueryHistoryStream(queryCtx, fmt.Sprintf("ClusterId == %d", clusterID), &htcondor.HistoryQueryOptions{
		Backwards:  true,
		Limit:      1,
		ScanLimit:  100,
		Projection: []string{"JobStatus", "HoldReason"},
	}, nil)
	if err != nil {
		return -1, "", false
	}
	for res := range stream {
		if res.Err != nil {
			return -1, "", false
		}
		js, _ := res.Ad.EvaluateAttrInt("JobStatus")
		hold, _ := res.Ad.EvaluateAttrString("HoldReason")
		return int(js), hold, true
	}
	return -1, "", false
}

func dumpJobAd(t *testing.T, scheddAddr string, clusterID int64) {
	schedd := htcondor.NewSchedd("integration_schedd", scheddAddr)
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	ads, _, err := schedd.QueryWithOptions(ctx, fmt.Sprintf("ClusterId == %d", clusterID), &htcondor.QueryOptions{})
	if err != nil {
		t.Logf("job ad dump for %d failed: %v", clusterID, err)
		return
	}
	if len(ads) == 0 {
		t.Logf("job ad dump for %d: no ads returned", clusterID)
		return
	}

	for _, ad := range ads {
		t.Logf("job ad for cluster %d:\n%s", clusterID, ad.String())
	}
}
