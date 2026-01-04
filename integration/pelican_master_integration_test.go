//go:build integration

package integration

import (
	"archive/tar"
	"bytes"
	"compress/gzip"
	"context"
	"crypto/tls"
	"encoding/json"
	"fmt"
	"io"
	"net"
	"net/http"
	"os"
	"os/exec"
	"os/user"
	"path/filepath"
	"strconv"
	"strings"
	"syscall"
	"testing"
	"time"

	htcondor "github.com/bbockelm/golang-htcondor"
	condorconfig "github.com/bbockelm/golang-htcondor/config"
	"github.com/bbockelm/pelican-ap-manager/internal/webserver"
)

// TestPelicanManagedDaemon ensures pelican_man runs under condor_master, advertises,
// serves sandboxes over HTTP, and respects privilege expectations for root vs unprivileged runs.
func TestPelicanManagedDaemon(t *testing.T) {
	if _, err := exec.LookPath("condor_master"); err != nil {
		t.Skip("condor_master not found; skipping integration test")
	}

	cases := []struct {
		name        string
		requireRoot bool
	}{
		{name: "Unprivileged", requireRoot: false},
		{name: "RootManaged", requireRoot: true},
	}

	for _, tc := range cases {
		tc := tc
		t.Run(tc.name, func(t *testing.T) {
			if tc.requireRoot && os.Geteuid() != 0 {
				t.Skip("requires root to exercise condor-managed privileges")
			}
			if !tc.requireRoot && os.Geteuid() == 0 {
				t.Skip("skipping unprivileged variant when running as root")
			}
			runManagedDaemonScenario(t, tc.requireRoot)
		})
	}
}

func runManagedDaemonScenario(t *testing.T, rootMode bool) {
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

	// Prepare initial overrides; more will be added after discovering collector and building pelican
	initialOverrides := map[string]string{}

	if err := writeMiniCondorConfig(configPath, rootDir, socketDir, statePath, mirrorPath, t, initialOverrides); err != nil {
		t.Fatalf("write condor config: %v", err)
	}
	t.Setenv("CONDOR_CONFIG", configPath)

	seedEpochHistory(t, projectRoot, filepath.Join(rootDir, "spool"))

	var condorUID, condorGID int
	if rootMode {
		uid, gid, ok := lookupCondorUser(t)
		if !ok {
			if os.Getenv("PEL_REQUIRE_CONDOR_USER") == "1" {
				t.Fatalf("condor user not present; set up condor user or unset PEL_REQUIRE_CONDOR_USER")
			}
			t.Skip("condor user not present; skipping root-managed variant")
		}
		condorUID, condorGID = uid, gid
		// Allow condor-owned daemons to write into the mini-condor directories.
		for _, path := range []string{rootDir, filepath.Join(rootDir, "log"), filepath.Join(rootDir, "spool"), filepath.Join(rootDir, "execute"), filepath.Join(rootDir, "run"), filepath.Join(rootDir, "lock")} {
			if err := os.MkdirAll(path, 0o755); err != nil {
				t.Fatalf("mkdir %s: %v", path, err)
			}
			chownRecursive(t, path, condorUID, condorGID)
		}
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
	if collectorHostPort == "" {
		t.Fatalf("collector host:port parse failed from %q", collectorAddr)
	}

	pelicanPath, err := buildPelicanBinary(t, rootDir)
	if err != nil {
		t.Fatalf("build pelican: %v", err)
	}

	// Configure pelican_man as a managed daemon with a Unix socket HTTP API.
	daemonOverrides := map[string]string{
		"PELICAN_MANAGER_COLLECTOR_HOST":     collectorHostPort,
		"COLLECTOR_HOST":                     collectorHostPort,
		"PELICAN_MANAGER":                    pelicanPath,
		"PELICAN_MANAGER_ARGS":               "",
		"PELICAN_MANAGER_LOG":                "$(LOG)/PelicanManagerLog",
		"PELICAN_MANAGER_PIDFILE":            "$(LOG)/PelicanManager.pid",
		"PELICAN_REGISTRATION_SOCKET":        "$(SPOOL)/pelican_manager.sock",
		"PELICAN_MANAGER_WEB_TLS_CERT":       "$(SPOOL)/pelican-certs/server.crt",
		"PELICAN_MANAGER_WEB_TLS_KEY":        "$(SPOOL)/pelican-certs/server.key",
		"PELICAN_MANAGER_WEB_DB_PATH":        "$(SPOOL)/pelican_web.db",
		"PELICAN_MANAGER_POLL_INTERVAL":      "1s",
		"PELICAN_MANAGER_ADVERTISE_INTERVAL": "5s",
		"DAEMON_LIST":                        "MASTER, COLLECTOR, SHARED_PORT, NEGOTIATOR, SCHEDD, STARTD, PELICAN_MANAGER",
		"DC_DAEMON_LIST":                     "+ PELICAN_MANAGER",
	}

	// Write all overrides to config in one go
	f, err := os.OpenFile(configPath, os.O_APPEND|os.O_WRONLY, 0)
	if err != nil {
		t.Fatalf("open config for append: %v", err)
	}
	defer f.Close()

	for k, v := range daemonOverrides {
		if _, err := fmt.Fprintf(f, "\n%s = %s\n", k, v); err != nil {
			t.Fatalf("write config override: %v", err)
		}
	}

	// Ask master to pick up new config and start pelican_man.
	if err := condorCmd.Process.Signal(syscall.SIGHUP); err != nil {
		t.Fatalf("signal condor_master for reconfig: %v", err)
	}

	pidPath := filepath.Join(rootDir, "log", "PelicanManager.pid")
	pelicanPID, err := waitForPIDFile(pidPath, 45*time.Second)
	if err != nil {
		printHTCondorLogs(rootDir, t)
		t.Fatalf("pelican pid: %v", err)
	}

	pelicanLog := filepath.Join(rootDir, "log", "PelicanManagerLog")
	if rootMode {
		// Ensure condor_master started pelican_man as the condor user and wrote condor-owned logs.
		if euid, err := readEffectiveUID(pelicanPID); err == nil {
			if euid != condorUID {
				t.Fatalf("pelican_man euid=%d want condor uid=%d", euid, condorUID)
			}
		} else {
			t.Fatalf("read pelican euid: %v", err)
		}
		if uid, err := fileOwner(pelicanLog); err == nil {
			if uid != condorUID {
				t.Fatalf("pelican log owner uid=%d want condor uid=%d", uid, condorUID)
			}
		} else {
			t.Fatalf("pelican log stat: %v", err)
		}
	} else {
		if euid, err := readEffectiveUID(pelicanPID); err == nil {
			if euid != os.Geteuid() {
				t.Fatalf("pelican_man euid=%d want current uid=%d", euid, os.Geteuid())
			}
		}
	}

	scheddAddr, err := getScheddAddress(rootDir, 10*time.Second)
	if err != nil {
		t.Fatalf("schedd address: %v", err)
	}

	clusterID, err := submitSandboxJob(ctx, rootDir, scheddAddr)
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

	if err := verifyPelicanSummaryAds(ctx, collectorAddr, 60*time.Second, t); err != nil {
		printHTCondorLogs(rootDir, t)
		t.Fatalf("verify pelican summary ads: %v", err)
	}

	jobAd, err := fetchJobAd(ctx, scheddAddr, clusterID)
	if err != nil {
		t.Fatalf("fetch job ad: %v", err)
	}

	socketPath := filepath.Join(rootDir, "spool", "pelican_manager.sock")
	client := socketHTTPClient(socketPath)

	registerResp := registerSandbox(t, client, jobAd)

	files := fetchInputSandbox(t, client, clusterID, registerResp.Token)
	if _, ok := files["input.txt"]; !ok {
		t.Fatalf("input sandbox missing input.txt; saw files: %v", keys(files))
	}

	if err := uploadOutputSandbox(t, client, clusterID, registerResp.Token, jobAd, []byte("sandbox-upload")); err != nil {
		t.Fatalf("upload output sandbox: %v", err)
	}

	// Confirm the uploaded output landed in the job's Iwd.
	resultPath := filepath.Join(jobAd.Iwd, "result.txt")
	data, err := os.ReadFile(resultPath)
	if err != nil {
		t.Fatalf("read uploaded result: %v", err)
	}
	if string(bytes.TrimSpace(data)) != "sandbox-upload" {
		t.Fatalf("unexpected uploaded result content: %q", string(data))
	}
}

func submitSandboxJob(ctx context.Context, workDir, scheddAddr string) (int64, error) {
	scriptPath := filepath.Join(workDir, "job.sh")
	inputPath := filepath.Join(workDir, "input.txt")
	if err := os.WriteFile(scriptPath, []byte("#!/bin/sh\nset -e\ncat input.txt > result.txt\n"), 0o755); err != nil {
		return 0, fmt.Errorf("write job script: %w", err)
	}
	if err := os.WriteFile(inputPath, []byte("pelican_input"), 0o644); err != nil {
		return 0, fmt.Errorf("write input file: %w", err)
	}

	resultPath := filepath.Join(workDir, "result.txt")

	submit := fmt.Sprintf(
		"executable = %s\n"+
			"output = stdout.txt\n"+
			"error = stderr.txt\n"+
			"log = job.log\n"+
			"initialdir = %s\n"+
			"transfer_input_files = %s\n"+
			"transfer_output_files = result.txt\n"+
			"transfer_output_remaps = result.txt=%s\n"+
			"should_transfer_files = YES\n"+
			"when_to_transfer_output = ON_EXIT\n"+
			"transfer_executable = True\n"+
			"leave_in_queue = True\n"+
			"queue\n",
		scriptPath, workDir, inputPath, resultPath,
	)

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

func fetchJobAd(ctx context.Context, scheddAddr string, clusterID int64) (*webserver.RegisterRequest, error) {
	cfg, err := condorconfig.New()
	if err != nil {
		return nil, fmt.Errorf("load condor config: %w", err)
	}

	scheddName := "integration_schedd"
	if v, ok := cfg.Get("PELICAN_MANAGER_SCHEDD_NAME"); ok && v != "" {
		scheddName = v
	} else if v, ok := cfg.Get("SCHEDD_NAME"); ok && v != "" {
		scheddName = v
	}

	schedd := htcondor.NewSchedd(scheddName, scheddAddr)
	queryCtx, cancel := context.WithTimeout(ctx, 10*time.Second)
	defer cancel()

	ads, err := schedd.Query(queryCtx, fmt.Sprintf("ClusterId == %d && ProcId == 0", clusterID), []string{
		"ClusterId", "ProcId", "Owner", "Iwd", "Cmd", "TransferInput", "TransferOutput", "TransferExecutable", "In", "Out", "Err", "TransferOutputRemaps",
	})
	if err != nil {
		return nil, fmt.Errorf("query job ad: %w", err)
	}
	if len(ads) == 0 {
		return nil, fmt.Errorf("job %d ad not found", clusterID)
	}

	ad := ads[0]
	owner, _ := ad.EvaluateAttrString("Owner")
	iwd, _ := ad.EvaluateAttrString("Iwd")
	cmd, _ := ad.EvaluateAttrString("Cmd")
	transferInput, _ := ad.EvaluateAttrString("TransferInput")
	transferOutput, _ := ad.EvaluateAttrString("TransferOutput")
	transferExecutable, _ := ad.EvaluateAttrBool("TransferExecutable")
	inFile, _ := ad.EvaluateAttrString("In")
	outFile, _ := ad.EvaluateAttrString("Out")
	errFile, _ := ad.EvaluateAttrString("Err")
	remaps, _ := ad.EvaluateAttrString("TransferOutputRemaps")

	return &webserver.RegisterRequest{
		ClusterId:            int(clusterID),
		ProcId:               0,
		Owner:                owner,
		OsUser:               owner,
		Iwd:                  iwd,
		Cmd:                  cmd,
		TransferInput:        transferInput,
		TransferOutput:       transferOutput,
		TransferExecutable:   transferExecutable,
		In:                   inFile,
		Out:                  outFile,
		Err:                  errFile,
		TransferOutputRemaps: remaps,
	}, nil
}

func registerSandbox(t *testing.T, client *http.Client, req *webserver.RegisterRequest) *webserver.RegisterResponse {
	t.Helper()

	payload, err := jsonMarshal(req)
	if err != nil {
		t.Fatalf("marshal register request: %v", err)
	}

	httpReq, err := http.NewRequest(http.MethodPost, "https://unix/api/v1/sandbox/register", bytes.NewReader(payload))
	if err != nil {
		t.Fatalf("build register request: %v", err)
	}
	httpReq.Header.Set("Content-Type", "application/json")

	resp, err := client.Do(httpReq)
	if err != nil {
		t.Fatalf("register sandbox: %v", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		body, _ := io.ReadAll(resp.Body)
		t.Fatalf("register sandbox status=%d body=%s", resp.StatusCode, string(body))
	}

	var reg webserver.RegisterResponse
	if err := jsonNewDecoder(resp.Body).Decode(&reg); err != nil {
		t.Fatalf("decode register response: %v", err)
	}
	return &reg
}

func fetchInputSandbox(t *testing.T, client *http.Client, clusterID int64, token string) map[string][]byte {
	t.Helper()

	url := fmt.Sprintf("https://unix/sandboxes/%d.0/input", clusterID)
	req, err := http.NewRequest(http.MethodGet, url, nil)
	if err != nil {
		t.Fatalf("input sandbox request: %v", err)
	}
	req.Header.Set("Authorization", "Bearer "+token)

	resp, err := client.Do(req)
	if err != nil {
		t.Fatalf("fetch input sandbox: %v", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		body, _ := io.ReadAll(resp.Body)
		t.Fatalf("input sandbox status=%d body=%s", resp.StatusCode, string(body))
	}

	gzr, err := gzip.NewReader(resp.Body)
	if err != nil {
		t.Fatalf("gzip reader: %v", err)
	}
	defer gzr.Close()

	tr := tar.NewReader(gzr)
	files := make(map[string][]byte)
	for {
		hdr, err := tr.Next()
		if err == io.EOF {
			break
		}
		if err != nil {
			t.Fatalf("read tar: %v", err)
		}
		data, err := io.ReadAll(tr)
		if err != nil {
			t.Fatalf("read tar file: %v", err)
		}
		files[hdr.Name] = data
	}
	return files
}

func uploadOutputSandbox(t *testing.T, client *http.Client, clusterID int64, token string, jobAd *webserver.RegisterRequest, payload []byte) error {
	t.Helper()

	buf := new(bytes.Buffer)
	gzw := gzip.NewWriter(buf)
	tw := tar.NewWriter(gzw)

	if err := tw.WriteHeader(&tar.Header{Name: "result.txt", Mode: 0o644, Size: int64(len(payload))}); err != nil {
		return fmt.Errorf("tar header: %w", err)
	}
	if _, err := tw.Write(payload); err != nil {
		return fmt.Errorf("tar write: %w", err)
	}
	if err := tw.Close(); err != nil {
		return fmt.Errorf("tar close: %w", err)
	}
	if err := gzw.Close(); err != nil {
		return fmt.Errorf("gzip close: %w", err)
	}

	url := fmt.Sprintf("https://unix/sandboxes/%d.%d/output", jobAd.ClusterId, jobAd.ProcId)
	req, err := http.NewRequest(http.MethodPut, url, bytes.NewReader(buf.Bytes()))
	if err != nil {
		return fmt.Errorf("build output request: %w", err)
	}
	req.Header.Set("Authorization", "Bearer "+token)
	req.Header.Set("Content-Type", "application/x-tar")
	req.Header.Set("Content-Encoding", "gzip")

	resp, err := client.Do(req)
	if err != nil {
		return fmt.Errorf("upload output sandbox: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		body, _ := io.ReadAll(resp.Body)
		return fmt.Errorf("output sandbox status=%d body=%s", resp.StatusCode, string(body))
	}

	return nil
}

func socketHTTPClient(socketPath string) *http.Client {
	dialer := func(ctx context.Context, network, addr string) (net.Conn, error) {
		return net.DialTimeout("unix", socketPath, 5*time.Second)
	}

	tlsCfg := &tls.Config{InsecureSkipVerify: true, ServerName: "localhost"}

	transport := &http.Transport{
		DisableCompression: true,
		DialTLSContext: func(ctx context.Context, network, addr string) (net.Conn, error) {
			conn, err := dialer(ctx, network, addr)
			if err != nil {
				return nil, err
			}
			tlsConn := tls.Client(conn, tlsCfg)
			if err := tlsConn.HandshakeContext(ctx); err != nil {
				_ = conn.Close()
				return nil, err
			}
			return tlsConn, nil
		},
	}

	return &http.Client{Transport: transport}
}

func waitForPIDFile(pidPath string, timeout time.Duration) (int, error) {
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		data, err := os.ReadFile(pidPath)
		if err == nil {
			trimmed := strings.TrimSpace(string(data))
			if trimmed != "" {
				pid, err := strconv.Atoi(trimmed)
				if err == nil {
					return pid, nil
				}
			}
		}
		time.Sleep(500 * time.Millisecond)
	}
	return 0, fmt.Errorf("pid file %s not ready", pidPath)
}

func readEffectiveUID(pid int) (int, error) {
	statusPath := fmt.Sprintf("/proc/%d/status", pid)
	data, err := os.ReadFile(statusPath)
	if err != nil {
		return 0, err
	}
	for _, line := range strings.Split(string(data), "\n") {
		if strings.HasPrefix(line, "Uid:") {
			fields := strings.Fields(line)
			if len(fields) >= 3 {
				if euid, err := strconv.Atoi(fields[2]); err == nil {
					return euid, nil
				}
			}
		}
	}
	return 0, fmt.Errorf("Uid not found in %s", statusPath)
}

func fileOwner(path string) (int, error) {
	info, err := os.Stat(path)
	if err != nil {
		return 0, err
	}
	if stat, ok := info.Sys().(*syscall.Stat_t); ok {
		return int(stat.Uid), nil
	}
	return 0, fmt.Errorf("stat did not return syscall.Stat_t")
}

func chownRecursive(t *testing.T, path string, uid, gid int) {
	t.Helper()
	filepath.WalkDir(path, func(p string, d os.DirEntry, err error) error {
		if err != nil {
			return err
		}
		_ = os.Chown(p, uid, gid)
		return nil
	})
}

func lookupCondorUser(t *testing.T) (int, int, bool) {
	t.Helper()
	u, err := user.Lookup("condor")
	if err != nil {
		return 0, 0, false
	}
	uid, _ := strconv.Atoi(u.Uid)
	gid, _ := strconv.Atoi(u.Gid)
	return uid, gid, true
}

func moduleRoot(t *testing.T) string {
	t.Helper()
	cwd, err := os.Getwd()
	if err != nil {
		t.Fatalf("getwd: %v", err)
	}
	return filepath.Dir(cwd)
}

func keys(m map[string][]byte) []string {
	out := make([]string, 0, len(m))
	for k := range m {
		out = append(out, k)
	}
	return out
}

// jsonMarshal and jsonNewDecoder keep imports minimal without alias conflicts.
func jsonMarshal(v any) ([]byte, error) {
	return json.Marshal(v)
}

func jsonNewDecoder(r io.Reader) *json.Decoder {
	return json.NewDecoder(r)
}
