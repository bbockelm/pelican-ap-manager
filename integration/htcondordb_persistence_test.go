//go:build integration

package integration

import (
	"context"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"syscall"
	"testing"
	"time"

	condorconfig "github.com/bbockelm/golang-htcondor/config"
	"github.com/bbockelm/pelican-ap-manager/internal/ratelimit"
	"github.com/bbockelm/pelican-ap-manager/internal/store"
)

// TestRulesAndStatePersistToHtcondordb runs pelican-man and a real htcondordb
// under one condor_master and checks that what the daemon says it stored is
// actually in the database.
//
// This is the test whose absence let a daemon ship that could not persist
// anything at all. Everything else covering the database backends either
// exercised a pure function -- what a rule serializes to, which rows changed --
// or the path where the connection fails. None of it could see that the server
// rejected every write: pelican-man sent ads in the bracketed new-ClassAd form
// while dbrpc parses written ads with classad.ParseOld, so every write came back
// a syntax error, the rule store stayed empty, and the state was never saved.
// The daemon otherwise looked healthy.
//
// So the assertions here are deliberately made from the database's side, with a
// second client, rather than from anything pelican-man reports about itself.
func TestRulesAndStatePersistToHtcondordb(t *testing.T) {
	// Before building anything: the plain integration job has no HTCondor, and
	// every other test here skips in that case. Building first meant this one
	// failed instead -- and failed on the build, which is not even what it
	// tests.
	requireCondorMaster(t)

	dbBin := buildHtcondordb(t)

	rootDir := t.TempDir()
	socketDir, err := os.MkdirTemp("/tmp", "peldb_")
	if err != nil {
		t.Fatalf("socket dir: %v", err)
	}
	t.Cleanup(func() { _ = os.RemoveAll(socketDir) })

	configPath := filepath.Join(rootDir, "condor_config")
	managerPath, err := buildPelicanBinary(t, rootDir)
	if err != nil {
		t.Fatalf("build pelican-man: %v", err)
	}

	// Started as root, both daemons drop to the condor user before opening
	// anything, so condor has to own the directories they write -- including the
	// log directory the address file lands in. Without this the daemons come up
	// and then cannot publish an address, which is not a failure mode worth
	// re-deriving from a 90-second timeout.
	//
	// The only CI job with a real condor_master runs as root, so this is the
	// path that matters there; unprivileged it is a no-op.
	dropPrivileges := os.Geteuid() == 0
	var condorUID, condorGID int
	if dropPrivileges {
		uid, gid, ok := lookupCondorUser(t)
		if !ok {
			t.Skip("running as root but no condor user to drop to")
		}
		condorUID, condorGID = uid, gid

		// t.TempDir() creates its directories 0700 owned by the invoking user
		// (root here), including a parent above the one it returns. HTCondor
		// writes its logs as condor even while the process is root, so condor
		// has to be able to *traverse* every parent -- and chowning the inner
		// directories cannot fix a parent it may not enter. Without this,
		// condor_master dies at startup with "Cannot open log file" and every
		// daemon under it, htcondordb included, never starts.
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

	// A named shared-port socket, so the published address is predictable and
	// this test can assert -sock took effect.
	// pelican-man is pointed at htcondordb's address file rather than a literal
	// address, because the config is written before either daemon starts and the
	// port is not known until then. It is also the form an AP would use, and it
	// is resolved per connection attempt, so the daemon starting first does not
	// matter.
	dbAddr := filepath.Join(rootDir, "log", ".htcondordb_address")

	overrides := map[string]string{
		"PELICAN_MANAGER":                    managerPath,
		"PELICAN_MANAGER_POLL_INTERVAL":      "1s",
		"PELICAN_MANAGER_ADVERTISE_INTERVAL": "2s",
		"PELICAN_MANAGER_DEBUG":              "cedar:warn",
		"PELICAN_MANAGER_ENFORCEMENT_MODE":   "observing",

		"HTCONDORDB":       dbBin,
		"HTCONDORDB_DEBUG": "cedar:warn",
		// Not syncing the schedd: this test is about pelican-man's own writes,
		// and tailing the queue would only add moving parts.
		"HTCONDORDB_SYNC_SCHEDD": "false",

		"DAEMON_LIST":    "MASTER, COLLECTOR, SHARED_PORT, NEGOTIATOR, SCHEDD, STARTD, HTCONDORDB, PELICAN_MANAGER",
		"DC_DAEMON_LIST": "+HTCONDORDB PELICAN_MANAGER",

		// One static rule, which the daemon must write to the database on
		// startup and on every reconfigure.
		"PELICAN_MANAGER_RATE_RULES":          "persisted",
		"PELICAN_MANAGER_RATE_RULE_PERSISTED": `user=alice site=UCSD rate=4 window=60s note="integration"`,
		"PELICAN_MANAGER_RULE_DB_ADDRESS":     dbAddr,
		"PELICAN_MANAGER_EPOCH_DB_ADDRESS":    dbAddr,
		"PELICAN_MANAGER_STATE_DB_ADDRESS":    dbAddr,
	}

	if dropPrivileges {
		overrides["CONDOR_IDS"] = fmt.Sprintf("%d.%d", condorUID, condorGID)
	}

	statePath := filepath.Join(rootDir, "pelican_state.json")
	mirrorPath := filepath.Join(rootDir, "job_mirror.json")
	if err := writeMiniCondorConfig(configPath, rootDir, socketDir, statePath, mirrorPath, t, overrides); err != nil {
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

	// Whatever address htcondordb published; the test connects to that rather
	// than to anything it assumed.
	//
	// Deliberately not asserting the socket name. In this mini-pool the daemons
	// are not handed a SharedPort token by the master, so they self-register
	// under a generated <subsys>_<pid>_<rand> name and -sock never reaches the
	// naming decision at all. That is a property of the harness, not of the
	// daemon, and asserting it here would test the wrong thing.
	published, err := waitForAddressFile(filepath.Join(rootDir, "log", ".htcondordb_address"), 90*time.Second)
	if err != nil {
		// A bare timeout says nothing about why. Dump what the master and the
		// daemon had to say, since the likely causes -- the daemon never
		// starting, or starting and failing to write into a directory it does
		// not own after dropping privileges -- look identical from here.
		dumpLog(t, filepath.Join(rootDir, "log", "MasterLog"))
		dumpLog(t, filepath.Join(rootDir, "log", "HtcondordbLog"))
		listDir(t, filepath.Join(rootDir, "log"))
		t.Fatalf("htcondordb never published an address: %v", err)
	}
	t.Logf("htcondordb published %s", published)

	// --- the rule reached the database -------------------------------------
	//
	// Asked of the database with a separate client, not of pelican-man: the
	// daemon's own view would be just as empty whether the write succeeded or
	// was rejected.
	rules := waitForRules(t, published, configPath, 90*time.Second)
	if len(rules) != 1 {
		dumpLog(t, filepath.Join(rootDir, "log", "PelicanManagerLog"))
		t.Fatalf("%d rules in htcondordb after startup, want 1", len(rules))
	}
	got := rules[0]
	if got.Name != "persisted" || got.User != "alice" || got.Site != "UCSD" || got.RateCount != 4 {
		t.Errorf("stored rule = %+v, want persisted/alice/UCSD/4", got)
	}
	if got.Origin != ratelimit.OriginStatic {
		t.Errorf("stored rule origin = %q, want %q", got.Origin, ratelimit.OriginStatic)
	}

	// --- the working state reached the database ----------------------------
	stateStore, _, err := store.OpenState(store.StateOptions{
		DBAddress: published,
		DBTable:   store.DefaultStateTable,
		Config:    condorConfigFor(t, configPath),
	})
	if err != nil {
		t.Fatalf("open state store: %v", err)
	}
	defer func() { _ = stateStore.Close() }()

	deadline := time.Now().Add(90 * time.Second)
	var loaded bool
	for time.Now().Before(deadline) {
		qctx, qcancel := context.WithTimeout(context.Background(), 20*time.Second)
		st, err := stateStore.Load(qctx)
		qcancel()
		// A state that round-trips at all means rows were written and read back.
		if err == nil && st != nil {
			if _, buckets := st.Snapshot(); buckets != nil {
				loaded = true
				break
			}
		}
		time.Sleep(2 * time.Second)
	}
	if !loaded {
		dumpLog(t, filepath.Join(rootDir, "log", "PelicanManagerLog"))
		t.Error("the daemon's state never became readable from htcondordb")
	}

	// --- and it did all that without complaining ---------------------------
	//
	// The bug this test exists for showed up as an error on every cycle while
	// everything else carried on, so a silent log is part of the assertion.
	assertNoPersistenceErrors(t, filepath.Join(rootDir, "log", "PelicanManagerLog"))
}

// buildHtcondordb produces the htcondordb daemon. It is a separate project, but
// this module already depends on it, so its command builds straight out of the
// module graph -- no checkout, no second CI step.
func buildHtcondordb(t *testing.T) string {
	t.Helper()
	if prebuilt := os.Getenv("HTCONDORDB_BINARY"); prebuilt != "" {
		t.Logf("using prebuilt htcondordb from %s", prebuilt)
		return prebuilt
	}

	out := filepath.Join(t.TempDir(), "htcondordb")
	cmd := exec.Command("go", "build", "-o", out, "github.com/bbockelm/htcondordb/cmd/htcondordb")
	cmd.Dir = moduleRoot(t)
	cmd.Env = append(os.Environ(), "GOFLAGS=-mod=mod")
	if b, err := cmd.CombinedOutput(); err != nil {
		t.Skipf("cannot build htcondordb (%v): %s", err, strings.TrimSpace(string(b)))
	}
	return out
}

// waitForAddressFile blocks until a daemon publishes its command address. It
// returns an error rather than failing, so the caller can say what it looked at
// before giving up.
func waitForAddressFile(path string, within time.Duration) (string, error) {
	deadline := time.Now().Add(within)
	for time.Now().Before(deadline) {
		if b, err := os.ReadFile(path); err == nil {
			if addr := strings.TrimSpace(strings.SplitN(string(b), "\n", 2)[0]); addr != "" {
				return addr, nil
			}
		}
		time.Sleep(time.Second)
	}
	return "", fmt.Errorf("nothing at %s within %s", path, within)
}

// listDir reports what a directory holds and who owns it, which is how a
// privilege-drop problem shows itself.
func listDir(t *testing.T, dir string) {
	t.Helper()
	entries, err := os.ReadDir(dir)
	if err != nil {
		t.Logf("cannot list %s: %v", dir, err)
		return
	}
	var b strings.Builder
	for _, e := range entries {
		info, ierr := e.Info()
		if ierr != nil {
			continue
		}
		uid, gid := -1, -1
		if st, ok := info.Sys().(*syscall.Stat_t); ok {
			uid, gid = int(st.Uid), int(st.Gid)
		}
		fmt.Fprintf(&b, "  %-28s %6d bytes  uid=%d gid=%d  %v\n", e.Name(), info.Size(), uid, gid, info.Mode())
	}
	t.Logf("=== %s ===\n%s", dir, b.String())
}

// waitForRules polls the rule table until the daemon has written to it.
func waitForRules(t *testing.T, addr, configPath string, within time.Duration) []ratelimit.Rule {
	t.Helper()
	rs, _, err := store.Open(store.Options{
		DBAddress: addr,
		DBTable:   store.DefaultRuleTable,
		Config:    condorConfigFor(t, configPath),
	})
	if err != nil {
		t.Fatalf("open rule store: %v", err)
	}
	t.Cleanup(func() { _ = rs.Close() })

	deadline := time.Now().Add(within)
	for time.Now().Before(deadline) {
		qctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
		rules, err := rs.ListRules(qctx)
		cancel()
		if err == nil && len(rules) > 0 {
			return rules
		}
		time.Sleep(2 * time.Second)
	}
	return nil
}

func condorConfigFor(t *testing.T, configPath string) *condorconfig.Config {
	t.Helper()
	t.Setenv("CONDOR_CONFIG", configPath)
	cfg, err := condorconfig.New()
	if err != nil {
		t.Fatalf("condor config: %v", err)
	}
	return cfg
}

// assertNoPersistenceErrors fails on the log lines this bug produced. Checking
// the log matters because every one of these failures is non-fatal by design:
// the daemon logs, carries on, and looks fine.
func assertNoPersistenceErrors(t *testing.T, logPath string) {
	t.Helper()
	b, err := os.ReadFile(logPath)
	if err != nil {
		t.Fatalf("reading %s: %v", logPath, err)
	}
	// The history mirror is not in scope: this pool runs with
	// HTCONDORDB_SYNC_SCHEDD off, so the archives do not exist and falling back
	// to the schedd is correct. These are the write paths.
	for _, bad := range []string{
		"state save error",
		"syncing static rate rules",
		"rate rule store read error",
		"error parsing old ClassAd format",
	} {
		if strings.Contains(string(b), bad) {
			for _, line := range strings.Split(string(b), "\n") {
				if strings.Contains(line, bad) {
					t.Errorf("persistence error in the daemon log: %s", strings.TrimSpace(line))
					break
				}
			}
		}
	}
}

func dumpLog(t *testing.T, path string) {
	t.Helper()
	if b, err := os.ReadFile(path); err == nil {
		t.Logf("=== %s ===\n%s", filepath.Base(path), string(b))
	}
}
