//go:build integration

package integration

import (
	"context"
	"fmt"
	"os"
	"os/exec"
	"os/user"
	"path/filepath"
	"regexp"
	"strconv"
	"strings"
	"syscall"
	"testing"
	"time"

	htcondor "github.com/bbockelm/golang-htcondor"
	condorconfig "github.com/bbockelm/golang-htcondor/config"
)

// requireRootEnv, when set to "1", turns "cannot run as root" from a skip into a
// failure. CI sets it so a misconfigured runner fails loudly instead of quietly
// skipping the only test that covers the privileged path.
const requireRootEnv = "PELICAN_REQUIRE_ROOT_TEST"

// TestRootStaticRuleEnforcement is the end-to-end proof of the deployment this
// project is for: condor_master starts pelican_man and pelican_web as root, both
// drop to the condor user, the manager installs the operator's static rate rules
// as schedd startup limits, and the web daemon accepts a sandbox registration --
// all while the control loop is in observing mode, so nothing but the operator's
// own policy is enforced.
//
// Running as root is the point. The privilege drop, the condor-owned spool, and
// a job that actually executes are invisible to an unprivileged run, and the
// failure modes they guard against (a daemon that stays root, or that drops so
// early it cannot open its own log) only exist when the process starts
// privileged. Set PELICAN_REQUIRE_ROOT_TEST=1 to make an unrunnable environment
// a failure rather than a skip, so CI cannot quietly stop covering this.
func TestRootStaticRuleEnforcement(t *testing.T) {
	requireCondorMaster(t)

	if os.Geteuid() != 0 {
		msg := "this test must run as root: it covers the privilege drop and the condor-owned spool"
		if os.Getenv(requireRootEnv) == "1" {
			t.Fatalf("%s (%s=1)", msg, requireRootEnv)
		}
		t.Skip(msg + " (set " + requireRootEnv + "=1 to make this a failure)")
	}

	condorUID, condorGID, ok := lookupCondorUser(t)
	if !ok {
		if os.Getenv(requireRootEnv) == "1" {
			t.Fatalf("the condor user does not exist; the daemons have nothing to drop to (%s=1)", requireRootEnv)
		}
		t.Skip("condor user not present; skipping")
	}

	env := setupPool(t, poolOptions{condorUID: condorUID, condorGID: condorGID, dropPrivileges: true})

	// The privileged half: both daemons were started as root by condor_master
	// and are no longer root.
	assertDroppedPrivileges(t, env, condorUID)

	assertStaticRuleBehavior(t, env)
}

// TestStaticRuleEnforcement covers the same behavior without root: the operator's
// static rules reach the schedd, observing mode withholds everything else, the
// rules persist, and the sandbox API works.
//
// It exists so the logic is exercised on a developer laptop and in unprivileged
// CI. It deliberately does not replace the root test -- it cannot see the
// privilege drop at all.
func TestStaticRuleEnforcement(t *testing.T) {
	requireCondorMaster(t)

	if os.Geteuid() == 0 {
		t.Skip("running as root; TestRootStaticRuleEnforcement covers this case with the privilege drop")
	}

	assertStaticRuleBehavior(t, setupPool(t, poolOptions{}))
}

// assertStaticRuleBehavior is everything both variants check.
func assertStaticRuleBehavior(t *testing.T, env *rootPool) {
	t.Helper()

	// The operator's static rules reached the schedd as startup limits.
	installed := assertStaticRulesInstalled(t, env)

	// Observing mode withheld the control loop's own rules: what is installed is
	// exactly the static set, nothing more.
	assertOnlyStaticRules(t, env, installed)

	// The rules are persisted, not merely held in memory.
	assertRulesPersisted(t, env)

	// Record how each daemon got its command socket (see the function comment:
	// this is an observation, not a claim).
	logCommandSocketMode(t, env)

	// An admin can see all of this from the command line. HTCondor exposes no
	// way to list startup limits, so if these commands regress there is no
	// other way to answer "is my rule actually in force?".
	assertInspectionCLI(t, env)

	// The web daemon observes sandboxes: a job's sandbox registers and its input
	// can be fetched back with the issued token.
	assertSandboxObserved(t, env)

	// Last, because it is destructive: it kills pelican_man to watch the leases
	// expire, and condor_master is configured not to restart it.
	assertLimitLease(t, env)
}

// rootPool is a running mini-HTCondor with both pelican daemons under its master.
type rootPool struct {
	rootDir    string
	socketDir  string
	configPath string
	scheddAddr string
	socketPath string
	rulesPath  string
	managerBin string

	// droppedPrivileges records whether the daemons were meant to drop to the
	// condor user, which gates the ownership assertions.
	droppedPrivileges bool
}

// staticRules is the operator policy these tests install. Two rules, chosen to
// cover the shapes the config syntax supports: a (user, site) rule, and a
// site-wide rule with a note.
var staticRules = map[string]string{
	"PELICAN_MANAGER_RATE_RULES":          "slow_ucsd, all_psu",
	"PELICAN_MANAGER_RATE_RULE_SLOW_UCSD": `user=` + testJobOwner() + ` site=UCSD rate=3 window=60s note="integration: per-user cap"`,
	"PELICAN_MANAGER_RATE_RULE_ALL_PSU":   `site=PSU-LIGO rate=7 window=2m note="integration: site-wide cap"`,
}

// expectedLimits maps each rule's schedd limit name to the rate it must carry.
// The names come from ratelimit.Rule.LimitName(): pelican_<origin>_<rule name>.
var expectedLimits = map[string]int{
	"pelican_static_slow_ucsd": 3,
	"pelican_static_all_psu":   7,
}

// testJobOwner names the owner the per-user rule selects on. It only has to be
// a plausible account name -- the rule is asserted by inspecting the installed
// limit's expression, not by watching a job get throttled.
func testJobOwner() string { return "pelicanuser" }

// poolOptions selects between the privileged and unprivileged shapes of the pool.
type poolOptions struct {
	// dropPrivileges pins the daemons to the condor user via CONDOR_IDS and
	// hands the pool directories to that user. Only meaningful when the test
	// itself runs as root.
	dropPrivileges bool
	condorUID      int
	condorGID      int
}

// setupPool writes the configuration, builds both binaries, starts
// condor_master, and waits for everything to be reachable.
func setupPool(t *testing.T, opts poolOptions) *rootPool {
	t.Helper()

	rootDir := t.TempDir()
	// A short directory for Unix sockets: sun_path is capped at 108 bytes on
	// Linux (104 on macOS), and the test's temp dir alone can approach that.
	socketDir, err := os.MkdirTemp("/tmp", "pelroot_")
	if err != nil {
		t.Fatalf("socket dir: %v", err)
	}
	t.Cleanup(func() { _ = os.RemoveAll(socketDir) })

	statePath := filepath.Join(rootDir, "pelican_state.json")
	mirrorPath := filepath.Join(rootDir, "job_mirror.json")
	rulesPath := filepath.Join(rootDir, "spool", "pelican_rate_rules.json")
	configPath := filepath.Join(rootDir, "condor_config")

	managerPath, err := buildPelicanBinary(t, rootDir)
	if err != nil {
		t.Fatalf("build pelican_man: %v", err)
	}
	webPath, err := buildWebBinary(t, rootDir)
	if err != nil {
		t.Fatalf("build pelican_web: %v", err)
	}

	overrides := map[string]string{
		"PELICAN_MANAGER":                    managerPath,
		"PELICAN_MANAGER_POLL_INTERVAL":      "1s",
		"PELICAN_MANAGER_ADVERTISE_INTERVAL": "5s",
		"PELICAN_MANAGER_DEBUG":              "cedar:warn",
		// A killed pelican_man must stay dead long enough for the lease test to
		// watch its limits expire; without this the master restarts it within
		// seconds and reinstalls them.
		"MASTER_BACKOFF_CONSTANT": "3600",
		"MASTER_BACKOFF_CEILING":  "3600",
		// The whole point: the control loop observes and publishes, but only the
		// operator's static rules are installed.
		"PELICAN_MANAGER_ENFORCEMENT_MODE": "observing",
		"PELICAN_WEB":                      webPath,
		"PELICAN_WEB_LOG":                  filepath.Join(rootDir, "log", "PelicanWebLog"),
		"PELICAN_WEB_DEBUG":                "cedar:warn",
		"DAEMON_LIST":                      "MASTER, COLLECTOR, SHARED_PORT, NEGOTIATOR, SCHEDD, STARTD, PELICAN_MANAGER, PELICAN_WEB",
	}
	if opts.dropPrivileges {
		// Name the identity the daemons must drop to; this is what makes the
		// root variant a privilege-drop test rather than just a root run.
		overrides["CONDOR_IDS"] = fmt.Sprintf("%d.%d", opts.condorUID, opts.condorGID)
	}
	for k, v := range staticRules {
		overrides[k] = v
	}

	if err := writeMiniCondorConfig(configPath, rootDir, socketDir, statePath, mirrorPath, t, overrides); err != nil {
		t.Fatalf("write condor config: %v", err)
	}
	t.Setenv("CONDOR_CONFIG", configPath)

	// t.TempDir() creates its directories 0700 owned by the invoking user (root
	// here). HTCondor daemons write their logs as the condor user even while the
	// process is root, so condor must be able to *traverse* every parent of the
	// pool directory -- otherwise condor_master fails at startup with "Cannot
	// open log file". Widen the temp root and its parent before handing the tree
	// over.
	if opts.dropPrivileges {
		for _, dir := range []string{filepath.Dir(rootDir), rootDir} {
			if err := os.Chmod(dir, 0o755); err != nil {
				t.Fatalf("chmod %s: %v", dir, err)
			}
		}
	}

	// The daemons run as condor after the drop, so condor has to own everything
	// they write.
	for _, dir := range []string{rootDir, socketDir,
		filepath.Join(rootDir, "log"), filepath.Join(rootDir, "spool"),
		filepath.Join(rootDir, "execute"), filepath.Join(rootDir, "run"),
		filepath.Join(rootDir, "lock")} {
		if err := os.MkdirAll(dir, 0o755); err != nil {
			t.Fatalf("mkdir %s: %v", dir, err)
		}
		if opts.dropPrivileges {
			chownRecursive(t, dir, opts.condorUID, opts.condorGID)
		}
	}

	seedEpochHistory(t, moduleRoot(t), filepath.Join(rootDir, "spool"))

	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)

	condorCmd, err := startCondorMaster(ctx, configPath, rootDir)
	if err != nil {
		t.Fatalf("start condor_master: %v", err)
	}
	t.Cleanup(func() { stopCondorMaster(condorCmd, t) })

	if err := waitForCondor(rootDir, 90*time.Second, t); err != nil {
		// When condor_master cannot even open its own log there is nothing for
		// printHTCondorLogs to show, and the cause is almost always that some
		// parent directory is not traversable by the condor user. Report the
		// tree's ownership and modes so that is visible from the CI log alone.
		dumpDirPermissions(t, rootDir)
		printHTCondorLogs(rootDir, t)
		t.Fatalf("condor readiness: %v", err)
	}

	scheddAddr, err := getScheddAddress(rootDir, 30*time.Second)
	if err != nil {
		printHTCondorLogs(rootDir, t)
		t.Fatalf("schedd address: %v", err)
	}

	env := &rootPool{
		managerBin: managerPath,
		rootDir:    rootDir,
		socketDir:  socketDir,
		configPath: configPath,
		scheddAddr: scheddAddr,
		socketPath: filepath.Join(socketDir, "pelican_manager.sock"),
		rulesPath:  rulesPath,

		droppedPrivileges: opts.dropPrivileges,
	}

	// Both daemons must actually be up before anything is asserted.
	if err := waitForLogFile(filepath.Join(rootDir, "log", "PelicanManagerLog"), 60*time.Second); err != nil {
		printHTCondorLogs(rootDir, t)
		t.Fatalf("pelican_man did not start: %v", err)
	}
	if err := waitForSocket(env.socketPath, 60*time.Second); err != nil {
		dumpPelicanLogs(t, env)
		t.Fatalf("pelican_web did not open its sandbox socket: %v", err)
	}

	return env
}

// assertDroppedPrivileges checks that both daemons run as the condor user rather
// than as the root condor_master started them as. The log files are the evidence
// that survives the daemon: they are created after the drop, so their owner is
// the identity the daemon actually runs under.
func assertDroppedPrivileges(t *testing.T, env *rootPool, condorUID int) {
	t.Helper()

	for _, name := range []string{"PelicanManagerLog", "PelicanWebLog"} {
		path := filepath.Join(env.rootDir, "log", name)
		if err := waitForLogFile(path, 60*time.Second); err != nil {
			dumpPelicanLogs(t, env)
			t.Fatalf("%s never appeared: %v", name, err)
		}
		owner, err := fileOwner(path)
		if err != nil {
			t.Fatalf("stat %s: %v", path, err)
		}
		if owner == 0 {
			t.Errorf("%s is owned by root: the daemon did not drop privileges", name)
		}
		if owner != condorUID {
			t.Errorf("%s owner uid = %d, want the condor user (%d)", name, owner, condorUID)
		}
	}

	// The daemon also says so, and that line is what an admin will look for.
	if log := readFileString(t, filepath.Join(env.rootDir, "log", "PelicanManagerLog")); !strings.Contains(log, "dropped privileges") {
		t.Errorf("pelican_man log has no \"dropped privileges\" line; it may have been started unprivileged")
	}
}

// assertStaticRulesInstalled waits for the manager to push the configured static
// rules into the schedd and returns what it found, keyed by limit name.
func assertStaticRulesInstalled(t *testing.T, env *rootPool) map[string]*htcondor.StartupLimit {
	t.Helper()

	schedd := htcondor.NewSchedd("integration_schedd", env.scheddAddr)
	deadline := time.Now().Add(90 * time.Second)
	var last map[string]*htcondor.StartupLimit

	for time.Now().Before(deadline) {
		ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
		limits, err := schedd.QueryStartupLimits(ctx, "", "")
		cancel()
		if err != nil {
			time.Sleep(2 * time.Second)
			continue
		}

		byName := make(map[string]*htcondor.StartupLimit, len(limits))
		for _, l := range limits {
			byName[l.Name] = l
		}
		last = byName

		if len(byName) >= len(expectedLimits) {
			complete := true
			for name := range expectedLimits {
				if _, ok := byName[name]; !ok {
					complete = false
					break
				}
			}
			if complete {
				break
			}
		}
		time.Sleep(2 * time.Second)
	}

	for name, wantRate := range expectedLimits {
		limit, ok := last[name]
		if !ok {
			dumpPelicanLogs(t, env)
			t.Fatalf("static rule %q was never installed in the schedd; found %v", name, sortedKeys(last))
		}
		if limit.RateCount != wantRate {
			t.Errorf("limit %s: RateCount = %d, want %d", name, limit.RateCount, wantRate)
		}
		if limit.Expression == "" {
			t.Errorf("limit %s has an empty match expression", name)
		}
	}

	// The (user, site) rule must have produced a match expression that names
	// both, or it would throttle jobs it was never meant to.
	if limit, ok := last["pelican_static_slow_ucsd"]; ok {
		if !strings.Contains(limit.Expression, testJobOwner()) {
			t.Errorf("limit slow_ucsd expression %q does not constrain the owner", limit.Expression)
		}
		if !strings.Contains(limit.Expression, "UCSD") {
			t.Errorf("limit slow_ucsd expression %q does not constrain the site", limit.Expression)
		}
	}

	return last
}

// assertOnlyStaticRules checks that observing mode withheld the control loop's
// own conclusions.
//
// Two checks, because either alone is weak. The first confirms the daemon
// actually resolved the mode from configuration -- otherwise a typo in the
// macro name would leave it silently enforcing. The second confirms nothing
// dynamic reached the schedd; on a freshly-started pool the controller has no
// unhealthy (user, site) pair to act on anyway, so treat this as a guard against
// regression rather than proof on its own.
func assertOnlyStaticRules(t *testing.T, env *rootPool, installed map[string]*htcondor.StartupLimit) {
	t.Helper()

	log := readFileString(t, filepath.Join(env.rootDir, "log", "PelicanManagerLog"))
	if !strings.Contains(log, "rate limit enforcement mode: observing") {
		t.Errorf("pelican_man did not report observing mode; the configuration may not have taken effect")
	}

	for name := range installed {
		if strings.HasPrefix(name, "pelican_dynamic_") {
			t.Errorf("limit %q was installed in observing mode; only static rules may be enforced", name)
		}
	}
}

// assertRulesPersisted checks the rule store on disk. The store is what makes the
// operator's policy survive a restart and inspectable while running.
func assertRulesPersisted(t *testing.T, env *rootPool) {
	t.Helper()

	deadline := time.Now().Add(60 * time.Second)
	var body string
	for time.Now().Before(deadline) {
		if data, err := os.ReadFile(env.rulesPath); err == nil && len(data) > 0 {
			body = string(data)
			break
		}
		time.Sleep(time.Second)
	}
	if body == "" {
		dumpPelicanLogs(t, env)
		t.Fatalf("rate rule store %s was never written", env.rulesPath)
	}

	for _, name := range []string{"slow_ucsd", "all_psu"} {
		if !strings.Contains(body, `"name": "`+name+`"`) {
			t.Errorf("rule %q missing from the store:\n%s", name, body)
		}
	}
	if !strings.Contains(body, `"origin": "static"`) {
		t.Errorf("store has no static rules:\n%s", body)
	}
	if !strings.Contains(body, `"config_managed": true`) {
		t.Errorf("config-declared rules are not marked config-managed:\n%s", body)
	}

	if env.droppedPrivileges {
		owner, err := fileOwner(env.rulesPath)
		if err != nil {
			t.Fatalf("stat %s: %v", env.rulesPath, err)
		}
		if owner == 0 {
			t.Errorf("rule store is owned by root; it was written before the privilege drop")
		}
	}
}

// assertSandboxObserved runs a real job and drives the sandbox API the Pelican
// transfer plugin uses: register the job's sandbox, then fetch its input back
// with the token that registration issued.
func assertSandboxObserved(t *testing.T, env *rootPool) {
	t.Helper()

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
	defer cancel()

	clusterID := submitJobForSandbox(t, ctx, env)

	jobAd, err := fetchJobAd(ctx, env.scheddAddr, clusterID)
	if err != nil {
		t.Fatalf("fetch job ad: %v", err)
	}

	client := socketHTTPClient(env.socketPath)
	registerResp := registerSandbox(t, client, jobAd, env.rootDir)
	if registerResp.Token == "" {
		t.Fatal("sandbox registration returned no token")
	}

	files := fetchInputSandbox(t, client, clusterID, registerResp.Token)
	if _, ok := files["input.txt"]; !ok {
		t.Errorf("input sandbox is missing input.txt; got %v", keys(files))
	}
}

// submitJobForSandbox puts one job in the queue and returns its cluster id.
//
// HTCondor refuses to accept a submission from root ("NewCluster failed with
// error code 13"), so the privileged variant cannot use the Go submit API
// directly -- the whole process is root. It shells out to condor_submit through
// an unprivileged account instead. Only the job ad is needed downstream, so the
// job does not have to run.
func submitJobForSandbox(t *testing.T, ctx context.Context, env *rootPool) int64 {
	t.Helper()

	if !env.droppedPrivileges {
		clusterID, err := submitSandboxJob(ctx, env.rootDir, env.scheddAddr)
		if err != nil {
			printHTCondorLogs(env.rootDir, t)
			t.Fatalf("submit job: %v", err)
		}
		return clusterID
	}

	owner := submitAsUser()
	u, uerr := user.Lookup(owner)
	if uerr != nil {
		t.Fatalf("submit account %q does not exist: %v -- create it (useradd %s) or set PELICAN_SUBMIT_USER", owner, uerr, owner)
	}
	uid, _ := strconv.Atoi(u.Uid)
	gid, _ := strconv.Atoi(u.Gid)

	// The submitting user needs a directory it owns: condor_submit writes the
	// job log and the schedd's spool copies read the executable and input.
	submitDir := filepath.Join(env.rootDir, "submit")
	if err := os.MkdirAll(submitDir, 0o755); err != nil {
		t.Fatalf("mkdir %s: %v", submitDir, err)
	}
	submitFile := writeSandboxSubmitFile(t, submitDir)
	chownRecursive(t, submitDir, uid, gid)
	// An external command rather than the Go API: setuid in a running Go process
	// affects only the calling thread, so this process cannot drop for one call.
	//
	// runuser rather than su: su goes through PAM, which commonly refuses a
	// login for a locked system account like condor even when the caller is
	// root. runuser is the root-only tool that skips authentication.
	script := fmt.Sprintf("CONDOR_CONFIG=%s condor_submit -terse %s", env.configPath, submitFile)
	out, err := exec.CommandContext(ctx, "runuser", "-u", owner, "--", "/bin/sh", "-c", script).CombinedOutput()
	if err != nil {
		t.Logf("runuser -u %s -- sh -c %q\nexit: %v\noutput: %q", owner, script, err, string(out))
		printHTCondorLogs(env.rootDir, t)
		t.Fatalf("submitting as %s: %v", owner, err)
	}

	// -terse prints "cluster.proc - cluster.proc".
	var cluster, proc int64
	if _, serr := fmt.Sscanf(strings.TrimSpace(string(out)), "%d.%d", &cluster, &proc); serr != nil {
		t.Fatalf("parsing condor_submit -terse output %q: %v", string(out), serr)
	}
	return cluster
}

// submitAsUser is the unprivileged account the privileged variant submits as.
//
// Not root (HTCondor rejects the submission outright) and not condor: the
// schedd refuses to create a User record for its own daemon account -- "The
// given user is not allowed to own jobs". It has to be an ordinary account,
// which CI creates. PELICAN_SUBMIT_USER overrides the name.
func submitAsUser() string {
	if u := os.Getenv("PELICAN_SUBMIT_USER"); u != "" {
		return u
	}
	return "pelicansubmit"
}

// writeSandboxSubmitFile lays down the job and its input, and returns the path
// to the submit description.
func writeSandboxSubmitFile(t *testing.T, workDir string) string {
	t.Helper()

	scriptPath := filepath.Join(workDir, "job.sh")
	inputPath := filepath.Join(workDir, "input.txt")
	submitPath := filepath.Join(workDir, "job.sub")
	resultPath := filepath.Join(workDir, "result.txt")

	if err := os.WriteFile(scriptPath, []byte("#!/bin/sh\nset -e\ncat input.txt > result.txt\n"), 0o755); err != nil {
		t.Fatalf("write job script: %v", err)
	}
	if err := os.WriteFile(inputPath, []byte("pelican_input"), 0o644); err != nil {
		t.Fatalf("write input file: %v", err)
	}

	submit := fmt.Sprintf(
		"executable = %s\n"+
			"output = stdout.txt\n"+
			"error = stderr.txt\n"+
			"log = job.log\n"+
			"initialdir = %s\n"+
			"transfer_input_files = %s\n"+
			"transfer_output_files = result.txt\n"+
			"transfer_output_remaps = \"result.txt=%s\"\n"+
			"should_transfer_files = YES\n"+
			"when_to_transfer_output = ON_EXIT\n"+
			"transfer_executable = True\n"+
			"leave_in_queue = True\n"+
			"queue\n",
		scriptPath, workDir, inputPath, resultPath,
	)
	if err := os.WriteFile(submitPath, []byte(submit), 0o644); err != nil {
		t.Fatalf("write submit file: %v", err)
	}
	return submitPath
}

// logCommandSocketMode records how each daemon obtained its command socket:
// an endpoint inherited from condor_master, or one it self-registered in the
// shared-port directory.
//
// Deliberately an observation, not an assertion. DC_DAEMON_LIST is set in this
// pool, but what it buys is the master's liveness supervision (DC_CHILDALIVE),
// not an inherited shared-port endpoint -- the daemons self-register one, which
// works and is how they are reachable. Which of the two paths a daemon takes is
// worth recording when something goes wrong, but is not a property to pin.
func logCommandSocketMode(t *testing.T, env *rootPool) {
	t.Helper()

	for _, name := range []string{"PelicanManagerLog", "PelicanWebLog"} {
		log := readFileString(t, filepath.Join(env.rootDir, "log", name))
		switch {
		case strings.Contains(log, "accepting shared-port forwarded connections"):
			t.Logf("%s: adopted the shared-port endpoint inherited from condor_master", name)
		case strings.Contains(log, "self-registered shared-port endpoint"):
			t.Logf("%s: self-registered a shared-port endpoint (no SharedPort token inherited)", name)
		default:
			t.Logf("%s: bound its own command socket", name)
		}
	}
}

// testLease must match PELICAN_MANAGER_LIMIT_LEASE (and
// STARTUP_LIMIT_MAX_EXPIRATION) in the test pool's configuration.
const testLease = 5 * time.Second

// assertLimitLease checks the property the whole lease design exists for: the
// limits this daemon installs live only as long as it keeps renewing them.
//
// Two halves, and both matter:
//
//   - A healthy daemon holds its limits past the lease. Without renewal they
//     lapse after one lease and enforcement silently stops -- which is exactly
//     what happened before, unnoticed, because every other assertion here runs
//     within seconds of installation.
//   - A dead daemon loses them within about a lease, so a crash cannot leave an
//     access point throttled with nothing left running to lift the limit.
func assertLimitLease(t *testing.T, env *rootPool) {
	t.Helper()

	schedd := htcondor.NewSchedd("integration_schedd", env.scheddAddr)
	pelicanLimits := func(what string) []*htcondor.StartupLimit {
		t.Helper()
		ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
		defer cancel()
		limits, err := schedd.QueryStartupLimits(ctx, "", "")
		if err != nil {
			t.Fatalf("querying limits %s: %v", what, err)
		}
		var ours []*htcondor.StartupLimit
		for _, l := range limits {
			if strings.HasPrefix(l.Name, "pelican_") {
				ours = append(ours, l)
			}
		}
		return ours
	}

	// The lease the schedd actually granted. This is the direct check, and the
	// one that would have caught sending Expiration as a Unix timestamp: the
	// schedd clamps an over-long request to STARTUP_LIMIT_MAX_EXPIRATION, so the
	// symptom was every limit silently coming out at the schedd's maximum
	// instead of the configured lease.
	for _, l := range pelicanLimits("for their lease") {
		remaining := time.Until(time.Unix(l.ExpiresAt, 0))
		if remaining <= 0 {
			t.Errorf("limit %s has already expired (ExpiresAt=%d)", l.Name, l.ExpiresAt)
			continue
		}
		if remaining > testLease+2*time.Second {
			t.Errorf("limit %s holds a %v lease, want about %v -- is Expiration being sent as a duration?",
				l.Name, remaining.Truncate(time.Second), testLease)
		}
	}

	// Past a full lease, with the daemon running: renewal must have kept them.
	time.Sleep(testLease + 3*time.Second)
	if n := len(pelicanLimits("after one lease")); n < len(expectedLimits) {
		dumpPelicanLogs(t, env)
		t.Fatalf("%d pelican limits remain after one lease (%s); want %d -- the daemon is not renewing them",
			n, testLease, len(expectedLimits))
	}

	// Now stop renewing, the way a crash would.
	killPelicanManager(t, env)

	// The limits must stop applying within a lease. "Stop applying" is the
	// property, not "disappear": StartupLimitsAllowJob skips a limit the moment
	// its expires_at passes, while the schedd reaps the entry lazily on a timer.
	// Asserting on the listing would be testing the reaper's schedule.
	deadline := time.Now().Add(4 * testLease)
	for time.Now().Before(deadline) {
		live := 0
		for _, l := range pelicanLimits("after the daemon died") {
			if time.Until(time.Unix(l.ExpiresAt, 0)) > 0 {
				live++
			}
		}
		if live == 0 {
			return
		}
		time.Sleep(time.Second)
	}

	var still []string
	for _, l := range pelicanLimits("at the end") {
		if remaining := time.Until(time.Unix(l.ExpiresAt, 0)); remaining > 0 {
			still = append(still, fmt.Sprintf("%s (%v left)", l.Name, remaining.Truncate(time.Second)))
		}
	}
	t.Errorf("pelican limits were still in force %s after the daemon died: %v -- a crashed daemon must not leave the AP throttled",
		4*testLease, still)
}

// killPelicanManager stops pelican_man abruptly so the test can watch its leases
// run out. SIGKILL rather than a graceful shutdown, because a crash is the case
// the lease exists for; the pool's MASTER_BACKOFF_* settings keep condor_master
// from restarting it during the assertion.
//
// The pid comes from this pool's MasterLog rather than pgrep. A pattern match on
// the binary name would reach every pelican_man on the host -- a developer's own
// daemon, or one left behind by an earlier run -- and matching the full path does
// not work, because condor_master does not exec the daemon with its path as
// argv[0].
func killPelicanManager(t *testing.T, env *rootPool) {
	t.Helper()

	pid := pelicanManagerPID(t, env)
	if out, err := exec.Command("kill", "-9", strconv.Itoa(pid)).CombinedOutput(); err != nil {
		t.Fatalf("kill -9 %d: %v (%s)", pid, err, strings.TrimSpace(string(out)))
	}
	t.Logf("killed pelican_man (pid %d); watching its limits expire", pid)
}

// pelicanManagerPID reads the running daemon's pid out of its own log, which
// records it unconditionally at startup:
//
//	msg="daemon starting" ... subsystem=PELICAN_MANAGER pid=33700 under_master=true
//
// Not the MasterLog: the master only records a daemon's pid at D_FULLDEBUG, so
// keying on it would make this test depend on the pool's log verbosity.
func pelicanManagerPID(t *testing.T, env *rootPool) int {
	t.Helper()

	log := readFileString(t, filepath.Join(env.rootDir, "log", "PelicanManagerLog"))
	re := regexp.MustCompile(`subsystem=PELICAN_MANAGER pid=(\d+)`)

	// Last match wins: the master may have restarted the daemon.
	matches := re.FindAllStringSubmatch(log, -1)
	if len(matches) == 0 {
		t.Fatalf("no pid in PelicanManagerLog; cannot kill the daemon to observe its leases expiring")
	}
	pid, err := strconv.Atoi(matches[len(matches)-1][1])
	if err != nil {
		t.Fatalf("parsing pid %q: %v", matches[len(matches)-1][1], err)
	}
	return pid
}

// assertInspectionCLI runs `pelican_man -limits` and `-rules` against the live
// pool and checks they report the rules that are actually installed.
func assertInspectionCLI(t *testing.T, env *rootPool) {
	t.Helper()

	run := func(args ...string) string {
		t.Helper()
		cmd := exec.Command(env.managerBin, args...)
		cmd.Env = append(os.Environ(), "CONDOR_CONFIG="+env.configPath)
		out, err := cmd.CombinedOutput()
		if err != nil {
			t.Fatalf("%s %v: %v\n%s", env.managerBin, args, err, string(out))
		}
		return string(out)
	}

	limits := run("-limits")
	t.Logf("pelican_man -limits:\n%s", limits)
	for name := range expectedLimits {
		if !strings.Contains(limits, name) {
			t.Errorf("-limits did not report %q; got:\n%s", name, limits)
		}
	}

	rules := run("-rules")
	t.Logf("pelican_man -rules:\n%s", rules)
	for _, name := range []string{"slow_ucsd", "all_psu"} {
		if !strings.Contains(rules, name) {
			t.Errorf("-rules did not report %q; got:\n%s", name, rules)
		}
	}
	// The mode is the thing an admin most needs this command to be honest about.
	if !strings.Contains(rules, "Enforcement mode: observing") {
		t.Errorf("-rules did not report observing mode; got:\n%s", rules)
	}
}

// requireCondorMaster skips (or fails, under requireRootEnv) when there is no
// HTCondor to run against.
func requireCondorMaster(t *testing.T) {
	t.Helper()
	if _, err := condorconfig.New(); err != nil && os.Getenv(requireRootEnv) == "1" {
		t.Fatalf("HTCondor configuration unavailable (%s=1): %v", requireRootEnv, err)
	}
	if _, err := lookPathCondorMaster(); err != nil {
		if os.Getenv(requireRootEnv) == "1" {
			t.Fatalf("condor_master not on PATH (%s=1)", requireRootEnv)
		}
		t.Skip("condor_master not found; skipping integration test")
	}
}

func lookPathCondorMaster() (string, error) {
	return exec.LookPath("condor_master")
}

// dumpPelicanLogs prints both daemons' logs. Called on the failures where the
// daemon's own account of what happened is the only useful evidence.
func dumpPelicanLogs(t *testing.T, env *rootPool) {
	t.Helper()
	for _, name := range []string{"PelicanManagerLog", "PelicanWebLog"} {
		path := filepath.Join(env.rootDir, "log", name)
		if data, err := os.ReadFile(path); err == nil {
			t.Logf("=== %s ===\n%s\n=== end %s ===", name, string(data), name)
		} else {
			t.Logf("=== %s unreadable: %v ===", name, err)
		}
	}
	printHTCondorLogs(env.rootDir, t)
}

func readFileString(t *testing.T, path string) string {
	t.Helper()
	data, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("read %s: %v", path, err)
	}
	return string(data)
}

func sortedKeys[V any](m map[string]V) []string {
	out := make([]string, 0, len(m))
	for k := range m {
		out = append(out, k)
	}
	return out
}

// dumpDirPermissions logs owner and mode for the pool directory and each of its
// parents, which is what distinguishes "the daemon crashed" from "the daemon
// could not reach its own spool".
func dumpDirPermissions(t *testing.T, dir string) {
	t.Helper()
	for p := dir; ; p = filepath.Dir(p) {
		info, err := os.Stat(p)
		if err != nil {
			t.Logf("perm %s: %v", p, err)
		} else {
			uid := -1
			if st, ok := info.Sys().(*syscall.Stat_t); ok {
				uid = int(st.Uid)
			}
			t.Logf("perm %s: mode=%v uid=%d", p, info.Mode().Perm(), uid)
		}
		if p == "/" || filepath.Dir(p) == p {
			return
		}
	}
}
