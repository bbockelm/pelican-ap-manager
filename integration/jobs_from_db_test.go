//go:build integration

package integration

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"os/exec"
	"os/user"
	"path/filepath"
	"strconv"
	"strings"
	"testing"
	"time"
)

// TestJobsServedFromHtcondordbWhenCaughtUp exercises the whole live-queue path against real
// daemons: htcondordb tails the schedd's job_queue.log, advertises its sync health, and pelican-man
// reads the queue from the database instead of parsing the log a second time.
//
// The trap this test has to avoid is that both sources produce the same answer. A mirror read and a
// log read yield the same jobs, so "the job shows up" passes whichever source ran and would keep
// passing if the mirror path broke entirely and fell back. So the assertions are paired: the daemon
// must say it chose the database (the source decision), and the submitted job must then appear (the
// read through that source actually worked). Neither alone is enough -- a mirror query that errors
// still logs the choice before falling back to the log.
func TestJobsServedFromHtcondordbWhenCaughtUp(t *testing.T) {
	requireCondorMaster(t)

	dbBin := buildHtcondordb(t)

	rootDir := t.TempDir()
	socketDir, err := os.MkdirTemp("/tmp", "peljobs_")
	if err != nil {
		t.Fatalf("socket dir: %v", err)
	}
	t.Cleanup(func() { _ = os.RemoveAll(socketDir) })

	configPath := filepath.Join(rootDir, "condor_config")
	managerPath, err := buildPelicanBinary(t, rootDir)
	if err != nil {
		t.Fatalf("build pelican-man: %v", err)
	}

	// Same privilege handling as the persistence test: under root both daemons drop to condor, so
	// condor must own what they write and be able to traverse every parent of the temp dir.
	dropPrivileges := os.Geteuid() == 0
	var condorUID, condorGID int
	if dropPrivileges {
		uid, gid, ok := lookupCondorUser(t)
		if !ok {
			t.Skip("running as root but no condor user to drop to")
		}
		condorUID, condorGID = uid, gid
		for _, dir := range []string{filepath.Dir(rootDir), rootDir} {
			if err := os.Chmod(dir, 0o755); err != nil {
				t.Fatalf("chmod %s: %v", dir, err)
			}
		}
	}
	for _, dir := range []string{rootDir, socketDir,
		filepath.Join(rootDir, "log"), filepath.Join(rootDir, "spool"),
		filepath.Join(rootDir, "execute"), filepath.Join(rootDir, "run"),
		filepath.Join(rootDir, "lock")} {
		if err := os.MkdirAll(dir, 0o755); err != nil {
			t.Fatalf("mkdir %s: %v", dir, err)
		}
		if dropPrivileges {
			chownRecursive(t, dir, condorUID, condorGID)
		}
	}

	statePath := filepath.Join(rootDir, "pelican_state.json")
	mirrorPath := filepath.Join(rootDir, "job_mirror.json")

	// The pool comes up without the two extra daemons, because the collector's port is not known
	// until it is running: COLLECTOR_HOST starts as 127.0.0.1:0. Starting htcondordb and
	// pelican-man before that is resolved would give both a collector address that goes nowhere,
	// and the freshness gate would have nothing to read -- the test would then be asserting the
	// fallback path while looking like it tested the mirror.
	if err := writeMiniCondorConfig(configPath, rootDir, socketDir, statePath, mirrorPath, t, nil); err != nil {
		t.Fatalf("write condor config: %v", err)
	}
	t.Setenv("CONDOR_CONFIG", configPath)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	condorCmd, err := startCondorMaster(ctx, configPath, rootDir)
	if err != nil {
		t.Fatalf("start condor_master: %v", err)
	}
	t.Cleanup(func() { stopCondorMaster(condorCmd, t) })

	if err := waitForCondor(rootDir, 90*time.Second, t); err != nil {
		printHTCondorLogs(rootDir, t)
		t.Fatalf("condor readiness: %v", err)
	}

	collectorAddr, err := discoverCollectorAddress(rootDir, 30*time.Second)
	if err != nil {
		printHTCondorLogs(rootDir, t)
		t.Fatalf("collector address discovery: %v", err)
	}
	collectorHostPort := stripHostPort(collectorAddr)
	if collectorHostPort == "" {
		t.Fatalf("collector host:port parse failed from %q", collectorAddr)
	}

	scheddAddr, err := getScheddAddress(rootDir, 30*time.Second)
	if err != nil {
		t.Fatalf("get schedd address: %v", err)
	}

	// A job in the queue before the tailer starts, so there is something the mirror can return
	// that is not the empty set -- an empty queue would read the same from either source.
	cluster := submitQueueJob(t, ctx, rootDir, configPath, scheddAddr, dropPrivileges)
	t.Logf("submitted cluster %d", cluster)

	// Now resolve the collector and add the two daemons. Later definitions win in an HTCondor
	// config, so appending is enough.
	dbAddr := filepath.Join(rootDir, "log", ".htcondordb_address")
	for macro, value := range map[string]string{
		"COLLECTOR_HOST":                   collectorHostPort,
		"PELICAN_MANAGER_COLLECTOR_HOST":   collectorHostPort,
		"HTCONDORDB":                       dbBin,
		"HTCONDORDB_DEBUG":                 "cedar:warn",
		"HTCONDORDB_SYNC_SCHEDD":           "true",
		"PELICAN_MANAGER":                  managerPath,
		"PELICAN_MANAGER_ENFORCEMENT_MODE": "observing",
		"PELICAN_MANAGER_RULE_DB_ADDRESS":  dbAddr,
		"PELICAN_MANAGER_EPOCH_DB_ADDRESS": dbAddr,
		// Tail the archives rather than querying them each poll. Enabled here
		// because it is off by default, so nothing else in this suite would run
		// the daemon with it on at all.
		"PELICAN_MANAGER_EPOCH_DB_WATCH":   "true",
		"PELICAN_MANAGER_STATE_DB_ADDRESS": dbAddr,
		"DAEMON_LIST":                      "MASTER, COLLECTOR, SHARED_PORT, NEGOTIATOR, SCHEDD, STARTD, HTCONDORDB, PELICAN_MANAGER",
		"DC_DAEMON_LIST":                   "+HTCONDORDB PELICAN_MANAGER",
	} {
		if err := appendConfigOverride(configPath, macro, value); err != nil {
			t.Fatalf("append %s: %v", macro, err)
		}
	}
	if dropPrivileges {
		chownRecursive(t, rootDir, condorUID, condorGID)
	}

	if out, err := exec.CommandContext(ctx, "condor_reconfig").CombinedOutput(); err != nil {
		t.Fatalf("condor_reconfig: %v\n%s", err, out)
	}

	published, err := waitForAddressFile(dbAddr, 90*time.Second)
	if err != nil {
		dumpLog(t, filepath.Join(rootDir, "log", "MasterLog"))
		dumpLog(t, filepath.Join(rootDir, "log", "HtcondordbLog"))
		t.Fatalf("htcondordb never published an address: %v", err)
	}
	t.Logf("htcondordb published %s", published)

	managerLog := filepath.Join(rootDir, "log", "PelicanManagerLog")

	// --- the daemon chose the database -------------------------------------
	if err := waitForLogLine(managerLog, "reading jobs from htcondordb", 120*time.Second); err != nil {
		dumpLog(t, managerLog)
		dumpLog(t, filepath.Join(rootDir, "log", "HtcondordbLog"))
		t.Fatalf("pelican-man never routed the live queue to htcondordb: %v", err)
	}

	// --- and the queue it read through that choice is the real one ---------
	if err := waitForMirrorJob(mirrorPath, cluster, 90*time.Second); err != nil {
		dumpLog(t, managerLog)
		t.Fatalf("job never reached the mirror: %v", err)
	}

	// --- the tail is enabled, and the reads still arrive --------------------
	//
	// PELICAN_MANAGER_EPOCH_DB_WATCH is on above, so everything asserted here is
	// asserted with the tail running.
	//
	// There is deliberately no assertion that a read was served FROM the tail,
	// because it cannot be yet: htcondordb's archives can be watched, but their
	// change-log head cannot be read -- WatchHead resolves only mutable tables --
	// so every subscription fails and every read falls back to the query. The fix
	// is in classad ("dbrpc: let WatchHead resolve archives and view backings");
	// once it is released and bumped here, add:
	//
	//	waitForLogLine(managerLog, "reads are being served from the tail", ...)
	//
	// Asserting the fallback line instead would be worse than nothing: it would
	// pass today and start failing the moment the feature began working.
}

// waitForLogLine blocks until the log contains substr.
func waitForLogLine(path, substr string, within time.Duration) error {
	deadline := time.Now().Add(within)
	for time.Now().Before(deadline) {
		if data, err := os.ReadFile(path); err == nil && strings.Contains(string(data), substr) {
			return nil
		}
		time.Sleep(2 * time.Second)
	}
	return fmt.Errorf("%q never appeared in %s within %s", substr, path, within)
}

// waitForMirrorJob blocks until the cluster appears in pelican-man's job mirror, at any status.
func waitForMirrorJob(mirrorPath string, clusterID int64, within time.Duration) error {
	deadline := time.Now().Add(within)
	for time.Now().Before(deadline) {
		if data, err := os.ReadFile(mirrorPath); err == nil {
			var snap jobMirrorSnapshot
			if json.Unmarshal(data, &snap) == nil {
				for _, job := range snap.Jobs {
					if job.ClusterID == clusterID {
						return nil
					}
				}
			}
		}
		time.Sleep(1 * time.Second)
	}
	return fmt.Errorf("mirror %s never listed cluster %d within %s", mirrorPath, clusterID, within)
}

// submitQueueJob puts one job in the queue. The job only has to exist in job_queue.log, so it is
// never expected to run. Under root it submits through an unprivileged account, because HTCondor
// refuses a submission from root and a Go process cannot drop privileges for one call.
func submitQueueJob(t *testing.T, ctx context.Context, rootDir, configPath, scheddAddr string, dropped bool) int64 {
	t.Helper()

	if !dropped {
		cluster, err := submitSandboxJob(ctx, rootDir, scheddAddr)
		if err != nil {
			printHTCondorLogs(rootDir, t)
			t.Fatalf("submit job: %v", err)
		}
		return cluster
	}

	owner := submitAsUser()
	u, uerr := user.Lookup(owner)
	if uerr != nil {
		t.Fatalf("submit account %q does not exist: %v", owner, uerr)
	}
	uid, _ := strconv.Atoi(u.Uid)
	gid, _ := strconv.Atoi(u.Gid)

	submitDir := filepath.Join(rootDir, "submit")
	if err := os.MkdirAll(submitDir, 0o755); err != nil {
		t.Fatalf("mkdir %s: %v", submitDir, err)
	}
	submitFile := writeSandboxSubmitFile(t, submitDir)
	chownRecursive(t, submitDir, uid, gid)

	script := fmt.Sprintf("CONDOR_CONFIG=%s condor_submit -terse %s", configPath, submitFile)
	out, err := exec.CommandContext(ctx, "runuser", "-u", owner, "--", "/bin/sh", "-c", script).CombinedOutput()
	if err != nil {
		t.Logf("runuser -u %s -- sh -c %q\nexit: %v\noutput: %q", owner, script, err, string(out))
		printHTCondorLogs(rootDir, t)
		t.Fatalf("submitting as %s: %v", owner, err)
	}
	var cluster, proc int64
	if _, serr := fmt.Sscanf(strings.TrimSpace(string(out)), "%d.%d", &cluster, &proc); serr != nil {
		t.Fatalf("parsing condor_submit -terse output %q: %v", string(out), serr)
	}
	return cluster
}
