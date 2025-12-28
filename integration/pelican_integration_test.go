//go:build integration
// +build integration

package integration

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"regexp"
	"strings"
	"testing"
	"time"

	htcondor "github.com/bbockelm/golang-htcondor"
	condorconfig "github.com/bbockelm/golang-htcondor/config"
)

// TestPelicanIntegration boots a minimal local HTCondor, runs pelican_man, submits
// a trivial job, waits for completion, and verifies the pelican job mirror sees it.
func TestPelicanIntegration(t *testing.T) {
	if _, err := exec.LookPath("condor_master"); err != nil {
		t.Skip("condor_master not found; skipping integration test")
	}
	projectRoot := moduleRoot(t)
	rootDir := t.TempDir()
	socketDir, err := os.MkdirTemp("/tmp", "pelsp_")
	if err != nil {
		t.Fatalf("socket dir: %v", err)
	}
	t.Cleanup(func() { _ = os.RemoveAll(socketDir) })
	statePath := filepath.Join(rootDir, "pelican_state.json")
	mirrorPath := filepath.Join(rootDir, "job_mirror.json")
	configPath := filepath.Join(rootDir, "condor_config")

	if err := writeMiniCondorConfig(configPath, rootDir, socketDir, statePath, mirrorPath, t); err != nil {
		t.Fatalf("write condor config: %v", err)
	}
	t.Setenv("_CONDOR_CONFIG", configPath)
	t.Setenv("CONDOR_CONFIG", configPath)

	seedEpochHistory(t, projectRoot, filepath.Join(rootDir, "spool"))

	// Sanity-check that the config surfaces key macros that pelican_man depends on.
	if cfg, err := condorconfig.New(); err == nil {
		if v, ok := cfg.Get("PELICAN_MANAGER_JOB_MIRROR_PATH"); ok {
			t.Logf("condor macro PELICAN_MANAGER_JOB_MIRROR_PATH=%s", v)
		}
		if v, ok := cfg.Get("JOB_QUEUE_LOG"); ok {
			t.Logf("condor macro JOB_QUEUE_LOG=%s", v)
		}
	} else {
		t.Fatalf("condor config load after overrides: %v", err)
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

	// Refresh config after setting daemon name to ensure parent reads with the right context.
	if cfg, err := condorconfig.New(); err == nil {
		if v, ok := cfg.Get("LOG"); ok && v != "" {
			t.Logf("condor LOG set to %s", v)
		}
	} else {
		t.Fatalf("condor config reload: %v", err)
	}

	collectorAddr, err := discoverCollectorAddress(rootDir, 10*time.Second)
	if err != nil {
		printHTCondorLogs(rootDir, t)
		t.Fatalf("collector address discovery: %v", err)
	}
	collectorHostPort := stripHostPort(collectorAddr)
	if collectorHostPort == "" {
		printHTCondorLogs(rootDir, t)
		t.Fatalf("collector host:port parse failed from %q", collectorAddr)
	}

	// Persist the discovered collector host:port back into the config so downstream
	// consumers that rely on HTCondor macros (including pelican_man itself) see a
	// concrete port instead of the initial 0 placeholder.
	if err := appendConfigOverride(configPath, "PELICAN_MANAGER_COLLECTOR_HOST", collectorHostPort); err != nil {
		t.Fatalf("write collector override: %v", err)
	}
	if err := appendConfigOverride(configPath, "COLLECTOR_HOST", collectorHostPort); err != nil {
		t.Fatalf("write collector host override: %v", err)
	}

	scheddAddr, err := getScheddAddress(rootDir, 10*time.Second)
	if err != nil {
		t.Fatalf("get schedd address: %v", err)
	}
	if scheddAddr == "" {
		t.Fatalf("schedd address empty")
	}

	pelicanPath, err := buildPelicanBinary(t, rootDir)
	if err != nil {
		t.Fatalf("build pelican: %v", err)
	}

	pelicanCtx, pelicanCancel := context.WithCancel(ctx)
	pelicanCmd := exec.CommandContext(pelicanCtx, pelicanPath)
	pelicanCmd.Env = append(os.Environ(),
		"CONDOR_CONFIG="+configPath,
		"PELICAN_MANAGER_COLLECTOR_HOST="+collectorHostPort,
		"_CONDOR_COLLECTOR_HOST="+collectorHostPort,
		"_CONDOR_DAEMON_NAME=PELICAN_MANAGER",
	)
	pelicanCmd.Stdout = os.Stdout
	pelicanCmd.Stderr = os.Stderr
	if err := pelicanCmd.Start(); err != nil {
		pelicanCancel()
		t.Fatalf("start pelican: %v", err)
	}
	t.Cleanup(func() {
		pelicanCancel()
		_ = pelicanCmd.Wait()
	})

	clusterID, err := submitSleepJob(ctx, rootDir, collectorAddr, scheddAddr)
	if err != nil {
		printHTCondorLogs(rootDir, t)
		t.Fatalf("submit job: %v", err)
	}

	if err := waitForJobCompletion(ctx, scheddAddr, collectorAddr, clusterID); err != nil {
		printHTCondorLogs(rootDir, t)
		t.Fatalf("job completion: %v", err)
	}

	if err := waitForMirrorStatus(mirrorPath, clusterID, 0, 4, 30*time.Second); err != nil {
		printHTCondorLogs(rootDir, t)
		t.Fatalf("mirror status: %v", err)
	}
}

func writeMiniCondorConfig(configFile, localDir, socketDir, statePath, mirrorPath string, t *testing.T) error {
	sbinLine, libexecLine := detectCondorPaths(t)
	if err := os.MkdirAll(filepath.Join(localDir, "log"), 0o755); err != nil {
		return err
	}
	if err := os.MkdirAll(filepath.Join(localDir, "spool"), 0o755); err != nil {
		return err
	}
	if err := os.MkdirAll(filepath.Join(localDir, "execute"), 0o755); err != nil {
		return err
	}

	config := fmt.Sprintf(`# Mini HTCondor config for pelican_man integration
LOCAL_DIR = %s
LOG = $(LOCAL_DIR)/log
SPOOL = $(LOCAL_DIR)/spool
EXECUTE = $(LOCAL_DIR)/execute
RUN = $(LOCAL_DIR)/run
LOCK = $(LOCAL_DIR)/lock
DAEMON_LIST = MASTER, COLLECTOR, SHARED_PORT, NEGOTIATOR, SCHEDD, STARTD
SCHEDD_NAME = integration_schedd
PELICAN_MANAGER_SCHEDD_NAME = integration_schedd@$(FULL_HOSTNAME)
SCHEDD_INTERVAL = 5
UPDATE_INTERVAL = 5
CONDOR_HOST = 127.0.0.1
NETWORK_INTERFACE = 127.0.0.1
BIND_ALL_INTERFACES = False
USE_SHARED_PORT = True
DAEMON_SOCKET_DIR = %s
COLLECTOR_HOST = 127.0.0.1:0
COLLECTOR_ADDRESS_FILE = $(LOG)/.collector_address
SCHEDD_ADDRESS_FILE = $(LOG)/.schedd_address
ENABLE_FILE_TRANSFER = TRUE
SHOULD_TRANSFER_FILES = YES
WHEN_TO_TRANSFER_OUTPUT = ON_EXIT
JOB_QUEUE_LOG = $(SPOOL)/job_queue.log
START = TRUE
SUSPEND = FALSE
PREEMPT = FALSE
KILL = FALSE
KEEP_JOB_ON_EXIT = True
ALLOW_WRITE = *
ALLOW_READ = *
ALLOW_ADMINISTRATOR = *
ALLOW_NEGOTIATOR = *
ALLOW_OWNER = *
ALLOW_CLIENT = *
SEC_DEFAULT_AUTHENTICATION = OPTIONAL
SEC_DEFAULT_ENCRYPTION = OPTIONAL
SEC_DEFAULT_INTEGRITY = OPTIONAL
PELICAN_MANAGER_STATS_WINDOW = 5m
PELICAN_MANAGER_ADVERTISE_INTERVAL = 5m
PELICAN_MANAGER_POLL_INTERVAL = 1s
PELICAN_MANAGER_JOB_MIRROR_PATH = %s
PELICAN_MANAGER_STATE_PATH = %s
PELICAN_MANAGER_DEBUG = cedar:warn
HISTORY = $(SPOOL)/history
MAX_HISTORY_LOG = 10000000
MAX_HISTORY_ROTATIONS = 1
ENABLE_HISTORY_ROTATION = TRUE
EPOCH_HISTORY = $(SPOOL)/epoch_history
MAX_EPOCH_HISTORY = 10000000
MAX_EPOCH_HISTORY_ROTATIONS = 1
TRANSFER_HISTORY = $(LOG)/transfer_history
MAX_TRANSFER_HISTORY = 10000000
MAX_TRANSFER_HISTORY_ROTATIONS = 1
NEGOTIATOR_INTERVAL = 5
NEGOTIATOR_CYCLE_DELAY = 1
%s%s
`, localDir, socketDir, mirrorPath, statePath, sbinLine, libexecLine)

	return os.WriteFile(configFile, []byte(config), 0o644)
}

func appendConfigOverride(configFile, macro, value string) error {
	f, err := os.OpenFile(configFile, os.O_APPEND|os.O_WRONLY, 0)
	if err != nil {
		return err
	}
	defer f.Close()
	if _, err := fmt.Fprintf(f, "\n%s = %s\n", macro, value); err != nil {
		return err
	}
	return nil
}

// detectCondorPaths mirrors the shared port discovery logic to set SBIN/LIBEXEC for the mini condor.
func detectCondorPaths(t *testing.T) (string, string) {
	// SBIN derived from condor_master if present.
	sbinLine := ""
	if masterPath, err := exec.LookPath("condor_master"); err == nil {
		sbinDir := filepath.Dir(masterPath)
		binDir := filepath.Join(filepath.Dir(sbinDir), "bin")
		if _, err := os.Stat(filepath.Join(binDir, "condor_history")); err == nil {
			sbinLine = fmt.Sprintf("SBIN = %s\nBIN = %s\n", sbinDir, binDir)
			t.Logf("detected condor_master at %s; SBIN=%s BIN=%s", masterPath, sbinDir, binDir)
		} else {
			sbinLine = fmt.Sprintf("SBIN = %s\n", sbinDir)
			t.Logf("detected condor_master at %s; SBIN=%s", masterPath, sbinDir)
		}
	}

	// LIBEXEC from condor_shared_port; fall back to derived libexec next to SBIN or /usr/libexec/condor.
	libexecLine := ""
	var libexecDir string
	if sharedPortPath, err := exec.LookPath("condor_shared_port"); err == nil {
		libexecDir = filepath.Dir(sharedPortPath)
		t.Logf("detected condor_shared_port at %s; LIBEXEC=%s", sharedPortPath, libexecDir)
	} else {
		if masterPath, err2 := exec.LookPath("condor_master"); err2 == nil {
			sbinDir := filepath.Dir(masterPath)
			derived := filepath.Join(filepath.Dir(sbinDir), "libexec")
			if _, err := os.Stat(filepath.Join(derived, "condor_shared_port")); err == nil {
				libexecDir = derived
				t.Logf("derived LIBEXEC=%s from condor_master location", libexecDir)
			}
		}
		if libexecDir == "" {
			std := "/usr/libexec/condor"
			if _, err := os.Stat(filepath.Join(std, "condor_shared_port")); err == nil {
				libexecDir = std
				t.Logf("using standard LIBEXEC=%s", libexecDir)
			}
		}
	}

	if libexecDir != "" {
		libexecLine = fmt.Sprintf("LIBEXEC = %s\n", libexecDir)
	}

	return sbinLine, libexecLine
}

func startCondorMaster(ctx context.Context, configFile, localDir string) (*exec.Cmd, error) {
	cmd := exec.CommandContext(ctx, "condor_master", "-f")
	cmd.Env = append(os.Environ(), "CONDOR_CONFIG="+configFile, "_CONDOR_LOCAL_DIR="+localDir)
	cmd.Stdout = os.Stdout
	cmd.Stderr = os.Stderr
	if err := cmd.Start(); err != nil {
		return nil, err
	}
	return cmd, nil
}

func stopCondorMaster(cmd *exec.Cmd, t *testing.T) {
	if cmd == nil || cmd.Process == nil {
		return
	}
	_ = cmd.Process.Signal(os.Interrupt)
	select {
	case <-time.After(10 * time.Second):
		_ = cmd.Process.Kill()
	case <-waitChan(cmd):
	}
}

func waitChan(cmd *exec.Cmd) <-chan struct{} {
	done := make(chan struct{})
	go func() {
		_ = cmd.Wait()
		close(done)
	}()
	return done
}

func waitForCondor(localDir string, timeout time.Duration, t *testing.T) error {
	deadline := time.Now().Add(timeout)
	collectorAddressFile := filepath.Join(localDir, "log", ".collector_address")
	scheddAddressFile := filepath.Join(localDir, "log", ".schedd_address")
	for time.Now().Before(deadline) {
		if readyFile(collectorAddressFile) && readyFile(scheddAddressFile) {
			return nil
		}
		time.Sleep(500 * time.Millisecond)
	}
	return fmt.Errorf("condor daemons did not become ready")
}

func readyFile(path string) bool {
	data, err := os.ReadFile(path)
	if err != nil {
		return false
	}
	content := strings.TrimSpace(string(data))
	return content != "" && !strings.Contains(content, "(null)")
}

func discoverCollectorAddress(localDir string, timeout time.Duration) (string, error) {
	deadline := time.Now().Add(timeout)
	collectorAddressFile := filepath.Join(localDir, "log", ".collector_address")
	sharedPortAd := filepath.Join(localDir, "log", "shared_port_ad")

	for time.Now().Before(deadline) {
		// Prefer explicit collector address if present and non-null.
		if data, err := os.ReadFile(collectorAddressFile); err == nil {
			if addr := parseAddressFile(string(data)); addr != "" && !strings.Contains(addr, "(null)") {
				return addr, nil
			}
		}

		// Fallback: parse shared_port_ad for the daemon socket contact string.
		if data, err := os.ReadFile(sharedPortAd); err == nil {
			if addr := parseSharedPortAddress(string(data)); addr != "" {
				return addr, nil
			}
		}

		time.Sleep(500 * time.Millisecond)
	}

	return "", fmt.Errorf("timeout discovering collector shared port address")
}

func parseSharedPortAddress(ad string) string {
	// Look for ClassAd-style MyAddress = "<...>"
	re := regexp.MustCompile(`MyAddress\s*=\s*\"([^\"]+)\"`)
	if match := re.FindStringSubmatch(ad); len(match) == 2 {
		return match[1]
	}

	// Fallback: grab the first sinful string-like token.
	re2 := regexp.MustCompile(`<[^>]+>`)
	if match := re2.FindString(ad); match != "" {
		return match
	}
	return ""
}

func getScheddAddress(localDir string, timeout time.Duration) (string, error) {
	deadline := time.Now().Add(timeout)
	scheddAddressFile := filepath.Join(localDir, "log", ".schedd_address")
	for time.Now().Before(deadline) {
		if data, err := os.ReadFile(scheddAddressFile); err == nil {
			if addr := parseAddressFile(string(data)); addr != "" && !strings.Contains(addr, "(null)") {
				return addr, nil
			}
		}
		time.Sleep(500 * time.Millisecond)
	}
	return "", fmt.Errorf("timeout waiting for schedd address")
}

func parseAddressFile(content string) string {
	for _, line := range strings.Split(content, "\n") {
		trim := strings.TrimSpace(line)
		if trim == "" || strings.Contains(trim, "(null)") {
			continue
		}
		if strings.HasPrefix(trim, "<") {
			return trim
		}
	}
	return ""
}

// stripHostPort pulls host:port out of a sinful string.
func stripHostPort(sinful string) string {
	if sinful == "" {
		return ""
	}
	trim := strings.TrimSpace(sinful)
	trim = strings.TrimPrefix(trim, "<")
	if idx := strings.Index(trim, ">"); idx >= 0 {
		trim = trim[:idx]
	}
	if idx := strings.Index(trim, "?"); idx >= 0 {
		trim = trim[:idx]
	}
	return trim
}

func buildPelicanBinary(t *testing.T, workDir string) (string, error) {
	binPath := filepath.Join(workDir, "pelican_man")
	cwd, err := os.Getwd()
	if err != nil {
		return "", fmt.Errorf("getwd: %w", err)
	}
	moduleRoot := filepath.Dir(cwd)
	cmd := exec.Command("go", "build", "-o", binPath, "./cmd/pelican_man")
	cmd.Env = os.Environ()
	cmd.Dir = moduleRoot
	out, err := cmd.CombinedOutput()
	if err != nil {
		return "", fmt.Errorf("go build: %v (%s)", err, string(out))
	}
	return binPath, nil
}

func submitSleepJob(ctx context.Context, workDir, collectorAddr, scheddAddr string) (int64, error) {
	cfg, err := condorconfig.New()
	if err != nil {
		return 0, fmt.Errorf("load condor config: %w", err)
	}

	scheddName := "integration_schedd"
	if v, ok := cfg.Get("PELICAN_MANAGER_SCHEDD_NAME"); ok && v != "" {
		scheddName = v
	} else if v, ok := cfg.Get("SCHEDD_NAME"); ok && v != "" {
		scheddName = v
	}

	if collectorAddr == "" {
		if v, ok := cfg.Get("PELICAN_MANAGER_COLLECTOR_HOST"); ok && v != "" {
			collectorAddr = v
		} else if v, ok := cfg.Get("COLLECTOR_HOST"); ok && v != "" {
			collectorAddr = v
		}
	}
	if collectorAddr == "" {
		return 0, fmt.Errorf("collector address not available")
	}

	scriptPath := filepath.Join(workDir, "job.sh")
	script := "#!/bin/sh\nset -e\necho pelican > output.txt\n"
	if err := os.WriteFile(scriptPath, []byte(script), 0o755); err != nil {
		return 0, fmt.Errorf("write job script: %w", err)
	}

	submit := fmt.Sprintf("executable = job.sh\n"+
		"output = job.out\n"+
		"error = job.err\n"+
		"log = job.log\n"+
		"initialdir = %s\n"+
		"should_transfer_files = YES\n"+
		"when_to_transfer_output = ON_EXIT\n"+
		"leave_in_queue = True\n"+
		"queue\n", workDir)

	if scheddAddr == "" {
		col := htcondor.NewCollector(collectorAddr)
		loc, err := col.LocateDaemon(ctx, "Schedd", scheddName)
		if err != nil {
			return 0, fmt.Errorf("locate schedd %q: %w", scheddName, err)
		}
		scheddAddr = loc.Address
	}

	schedd := htcondor.NewSchedd(scheddName, scheddAddr)
	submitCtx, cancel := context.WithTimeout(ctx, 30*time.Second)
	defer cancel()

	clusterStr, err := schedd.Submit(submitCtx, submit)
	if err != nil {
		return 0, fmt.Errorf("submit job via API: %w", err)
	}

	var clusterID int64
	if _, err := fmt.Sscanf(clusterStr, "%d", &clusterID); err != nil {
		return 0, fmt.Errorf("parse cluster id %q: %w", clusterStr, err)
	}
	return clusterID, nil
}

func waitForJobCompletion(ctx context.Context, scheddAddr, collectorAddr string, clusterID int64) error {
	cfg, err := condorconfig.New()
	if err != nil {
		return fmt.Errorf("load condor config: %w", err)
	}

	scheddName := "integration_schedd"
	if v, ok := cfg.Get("PELICAN_MANAGER_SCHEDD_NAME"); ok && v != "" {
		scheddName = v
	} else if v, ok := cfg.Get("SCHEDD_NAME"); ok && v != "" {
		scheddName = v
	}

	if scheddAddr == "" {
		if collectorAddr == "" {
			if v, ok := cfg.Get("PELICAN_MANAGER_COLLECTOR_HOST"); ok && v != "" {
				collectorAddr = v
			} else if v, ok := cfg.Get("COLLECTOR_HOST"); ok && v != "" {
				collectorAddr = v
			}
		}
		if collectorAddr == "" {
			return fmt.Errorf("collector address not available for wait")
		}
		col := htcondor.NewCollector(collectorAddr)
		loc, err := col.LocateDaemon(ctx, "Schedd", scheddName)
		if err != nil {
			return fmt.Errorf("locate schedd %q: %w", scheddName, err)
		}
		scheddAddr = loc.Address
	}

	schedd := htcondor.NewSchedd(scheddName, scheddAddr)
	constraint := fmt.Sprintf("ClusterId == %d && ProcId == 0", clusterID)
	deadline := time.Now().Add(90 * time.Second)

	for time.Now().Before(deadline) {
		queryCtx, cancel := context.WithTimeout(ctx, 5*time.Second)
		ads, err := schedd.Query(queryCtx, constraint, []string{"JobStatus", "ClusterId", "ProcId"})
		cancel()
		if err == nil {
			if len(ads) == 0 {
				return fmt.Errorf("job %d disappeared from schedd queue", clusterID)
			}
			if st, ok := ads[0].EvaluateAttrInt("JobStatus"); ok {
				if st == 4 {
					return nil
				}
			}
		}
		time.Sleep(1 * time.Second)
	}

	return fmt.Errorf("job %d did not reach completed state in time", clusterID)
}

type jobMirrorSnapshot struct {
	Updated time.Time      `json:"updated"`
	Jobs    []jobMirrorJob `json:"jobs"`
}

type jobMirrorJob struct {
	ClusterID int64  `json:"cluster_id"`
	ProcID    int64  `json:"proc_id"`
	Owner     string `json:"owner"`
	JobStatus int64  `json:"job_status"`
}

func waitForMirrorStatus(mirrorPath string, clusterID, procID, wantedStatus int64, timeout time.Duration) error {
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		data, err := os.ReadFile(mirrorPath)
		if err == nil {
			var snap jobMirrorSnapshot
			if err := json.Unmarshal(data, &snap); err == nil {
				for _, job := range snap.Jobs {
					if job.ClusterID == clusterID && job.ProcID == procID {
						if job.JobStatus == wantedStatus {
							return nil
						}
					}
				}
			}
		}
		time.Sleep(1 * time.Second)
	}
	return fmt.Errorf("mirror file %s did not report status %d for %d.%d", mirrorPath, wantedStatus, clusterID, procID)
}

func printHTCondorLogs(localDir string, t *testing.T) {
	logDir := filepath.Join(localDir, "log")
	files, err := os.ReadDir(logDir)
	if err != nil {
		t.Logf("failed to read log dir: %v", err)
		return
	}
	for _, file := range files {
		if file.IsDir() {
			continue
		}
		path := filepath.Join(logDir, file.Name())
		data, err := os.ReadFile(path)
		if err != nil {
			t.Logf("-- %s (error: %v)", file.Name(), err)
			continue
		}
		lines := strings.Split(string(data), "\n")

		limit := 20
		if strings.Contains(file.Name(), "StarterLog") || strings.Contains(file.Name(), "ShadowLog") || strings.Contains(file.Name(), "ScheddLog") {
			limit = 200
		}
		if len(lines) > limit {
			lines = lines[len(lines)-limit:]
		}
		t.Logf("-- %s (last %d lines)", file.Name(), len(lines))
		for _, line := range lines {
			if strings.TrimSpace(line) != "" {
				t.Log(line)
			}
		}
	}
}
