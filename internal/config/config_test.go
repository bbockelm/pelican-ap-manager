package config

import (
	"testing"
	"time"

	condorconfig "github.com/bbockelm/golang-htcondor/config"
	"github.com/bbockelm/pelican-ap-manager/internal/ratelimit"
)

// newCondorConfig builds an in-memory HTCondor configuration with the given
// macros, so the loader can be exercised without a condor_config on disk.
func newCondorConfig(t *testing.T, macros map[string]string) *condorconfig.Config {
	t.Helper()
	t.Setenv("CONDOR_CONFIG", "ONLY_ENV")
	cfg, err := condorconfig.New()
	if err != nil {
		t.Fatalf("condor config: %v", err)
	}
	for k, v := range macros {
		cfg.Set(k, v)
	}
	return cfg
}

func TestLoadStaticRules(t *testing.T) {
	cfg, err := LoadFrom(newCondorConfig(t, map[string]string{
		"PELICAN_MANAGER_RATE_RULES":          "ligo_ucsd, psu_all",
		"PELICAN_MANAGER_RATE_RULE_LIGO_UCSD": `user=ligo site=UCSD rate=20 window=60s`,
		"PELICAN_MANAGER_RATE_RULE_PSU_ALL":   `site=PSU-LIGO rate=5 window=2m note="incident 4471"`,
		"PELICAN_MANAGER_ENFORCEMENT_MODE":    "observing",
		"PELICAN_MANAGER_RULE_DB_ADDRESS":     "db.example.org:9618",
		"PELICAN_MANAGER_RULE_DB_TABLE":       "my_rules",
		"PELICAN_MANAGER_SITE_ATTRIBUTE":      "GLIDEIN_Site",
		"PELICAN_MANAGER_RULE_STORE_PATH":     "/var/lib/pelican/rules.json",
		"PELICAN_MANAGER_ADVERTISE_INTERVAL":  "5s",
	}))
	if err != nil {
		t.Fatalf("LoadFrom: %v", err)
	}

	if cfg.EnforcementMode != ratelimit.ModeObserving {
		t.Errorf("EnforcementMode = %q, want %q", cfg.EnforcementMode, ratelimit.ModeObserving)
	}
	if cfg.RuleDBAddress != "db.example.org:9618" || cfg.RuleDBTable != "my_rules" {
		t.Errorf("rule DB = %q/%q, want db.example.org:9618/my_rules", cfg.RuleDBAddress, cfg.RuleDBTable)
	}
	if cfg.RuleStorePath != "/var/lib/pelican/rules.json" {
		t.Errorf("RuleStorePath = %q", cfg.RuleStorePath)
	}

	if len(cfg.StaticRules) != 2 {
		t.Fatalf("%d static rules, want 2", len(cfg.StaticRules))
	}
	// SortRules orders by name: ligo_ucsd before psu_all.
	ligo, psu := cfg.StaticRules[0], cfg.StaticRules[1]
	if ligo.Name != "ligo_ucsd" || ligo.User != "ligo" || ligo.Site != "UCSD" ||
		ligo.RateCount != 20 || ligo.RateWindow != time.Minute {
		t.Errorf("ligo_ucsd = %+v", ligo)
	}
	if psu.Name != "psu_all" || psu.Site != "PSU-LIGO" || psu.RateCount != 5 ||
		psu.RateWindow != 2*time.Minute || psu.Note != "incident 4471" {
		t.Errorf("psu_all = %+v", psu)
	}
	for _, r := range cfg.StaticRules {
		if !r.ConfigManaged {
			t.Errorf("rule %s should be marked config-managed", r.Name)
		}
		if r.Origin != ratelimit.OriginStatic {
			t.Errorf("rule %s origin = %q, want static", r.Name, r.Origin)
		}
	}
}

func TestLoadStaticRulesErrors(t *testing.T) {
	tests := []struct {
		name   string
		macros map[string]string
	}{
		{
			name:   "named rule with no body",
			macros: map[string]string{"PELICAN_MANAGER_RATE_RULES": "ghost"},
		},
		{
			name: "unparseable body",
			macros: map[string]string{
				"PELICAN_MANAGER_RATE_RULES":    "bad",
				"PELICAN_MANAGER_RATE_RULE_BAD": `user=alice raet=20`,
			},
		},
		{
			name: "duplicate name",
			macros: map[string]string{
				"PELICAN_MANAGER_RATE_RULES":    "dup, dup",
				"PELICAN_MANAGER_RATE_RULE_DUP": `site=UCSD rate=1`,
			},
		},
		{
			name:   "unknown enforcement mode",
			macros: map[string]string{"PELICAN_MANAGER_ENFORCEMENT_MODE": "sometimes"},
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			// A misconfigured rate limit must stop the daemon, not be skipped:
			// an operator who thinks a limit is in force when it is not is
			// worse off than one whose daemon refuses to start.
			if _, err := LoadFrom(newCondorConfig(t, tc.macros)); err == nil {
				t.Error("LoadFrom accepted a bad configuration")
			}
		})
	}
}

func TestEnforcementModeDefaultsToEnforcing(t *testing.T) {
	cfg, err := LoadFrom(newCondorConfig(t, nil))
	if err != nil {
		t.Fatalf("LoadFrom: %v", err)
	}
	if cfg.EnforcementMode != ratelimit.ModeEnforcing {
		t.Errorf("EnforcementMode = %q, want %q (preserving historical behavior)",
			cfg.EnforcementMode, ratelimit.ModeEnforcing)
	}
	if len(cfg.StaticRules) != 0 {
		t.Errorf("%d static rules with none configured", len(cfg.StaticRules))
	}
}

// TestEpochDBDefaultsToTheRuleDB: a site that already keeps its rules in
// htcondordb is, by construction, running the database this would read from, so
// making it opt in twice buys nothing.
func TestEpochDBDefaultsToTheRuleDB(t *testing.T) {
	cfg, err := LoadFrom(newCondorConfig(t, map[string]string{
		"PELICAN_MANAGER_RULE_DB_ADDRESS": "db.example.org:9618",
	}))
	if err != nil {
		t.Fatalf("LoadFrom: %v", err)
	}
	if cfg.EpochDBAddress != "db.example.org:9618" {
		t.Errorf("EpochDBAddress = %q, want the rule DB address", cfg.EpochDBAddress)
	}
}

// TestEpochDBCanBeSplitFromTheRuleDB: the rules are small and want to live
// wherever the admin edits them; the history is large and wants to live
// wherever it is already being mirrored. They need not be the same instance.
func TestEpochDBCanBeSplitFromTheRuleDB(t *testing.T) {
	cfg, err := LoadFrom(newCondorConfig(t, map[string]string{
		"PELICAN_MANAGER_RULE_DB_ADDRESS":         "rules.example.org:9618",
		"PELICAN_MANAGER_EPOCH_DB_ADDRESS":        "history.example.org:9618",
		"PELICAN_MANAGER_EPOCH_DB_JOB_TABLE":      "ap_history",
		"PELICAN_MANAGER_EPOCH_DB_TRANSFER_TABLE": "ap_epochs",
	}))
	if err != nil {
		t.Fatalf("LoadFrom: %v", err)
	}
	if cfg.EpochDBAddress != "history.example.org:9618" {
		t.Errorf("EpochDBAddress = %q, want history.example.org:9618", cfg.EpochDBAddress)
	}
	// The two reads come from two different schedd files, so the tables are
	// configured independently.
	if cfg.EpochDBJobTable != "ap_history" || cfg.EpochDBTransferTable != "ap_epochs" {
		t.Errorf("epoch tables = %q/%q, want ap_history/ap_epochs", cfg.EpochDBJobTable, cfg.EpochDBTransferTable)
	}
}

// TestEpochDBIsOffByDefault: reading from the schedd is what this has always
// done, and no configuration should quietly change where the daemon's data
// comes from.
func TestEpochDBIsOffByDefault(t *testing.T) {
	cfg, err := LoadFrom(newCondorConfig(t, nil))
	if err != nil {
		t.Fatalf("LoadFrom: %v", err)
	}
	if cfg.EpochDBAddress != "" {
		t.Errorf("EpochDBAddress = %q, want empty (read from the schedd)", cfg.EpochDBAddress)
	}
}

// TestLeaseWarningTracksTheScheddCeiling: the schedd silently clamps a lease
// longer than STARTUP_LIMIT_MAX_EXPIRATION, so raising one without the other
// gets a value nobody asked for and nothing reports.
func TestLeaseWarningTracksTheScheddCeiling(t *testing.T) {
	cfg, err := LoadFrom(newCondorConfig(t, map[string]string{
		"PELICAN_MANAGER_LIMIT_LEASE":  "10m",
		"STARTUP_LIMIT_MAX_EXPIRATION": "300",
	}))
	if err != nil {
		t.Fatalf("LoadFrom: %v", err)
	}
	if cfg.LimitLeaseWarning == "" {
		t.Error("no warning for a 10m lease against a 5m schedd ceiling")
	}

	// Raising the schedd's knob to match makes the lease honorable.
	cfg, err = LoadFrom(newCondorConfig(t, map[string]string{
		"PELICAN_MANAGER_LIMIT_LEASE":  "10m",
		"STARTUP_LIMIT_MAX_EXPIRATION": "600",
	}))
	if err != nil {
		t.Fatalf("LoadFrom: %v", err)
	}
	if cfg.LimitLeaseWarning != "" {
		t.Errorf("unexpected warning with a matching ceiling: %s", cfg.LimitLeaseWarning)
	}
}

// TestLeaseWarningAssumesTheScheddDefault: an unset knob still clamps, so the
// warning has to be driven by the schedd's built-in default rather than by
// silence.
func TestLeaseWarningAssumesTheScheddDefault(t *testing.T) {
	cfg, err := LoadFrom(newCondorConfig(t, map[string]string{
		"PELICAN_MANAGER_LIMIT_LEASE": "10m",
	}))
	if err != nil {
		t.Fatalf("LoadFrom: %v", err)
	}
	if cfg.LimitLeaseWarning == "" {
		t.Error("no warning for a lease above the schedd's built-in 5m default")
	}
}

// TestDefaultLeaseIsNotWarnedAbout: the stock configuration must be quiet, or
// the warning is noise and gets ignored when it matters.
func TestDefaultLeaseIsNotWarnedAbout(t *testing.T) {
	cfg, err := LoadFrom(newCondorConfig(t, nil))
	if err != nil {
		t.Fatalf("LoadFrom: %v", err)
	}
	if cfg.LimitLeaseWarning != "" {
		t.Errorf("stock configuration warns: %s", cfg.LimitLeaseWarning)
	}
}

// TestPollIntervalNoLongerAffectsTheLease: renewal runs on its own timer now,
// so a poll interval longer than the lease is no longer a problem and must not
// be reported as one.
func TestPollIntervalNoLongerAffectsTheLease(t *testing.T) {
	cfg, err := LoadFrom(newCondorConfig(t, map[string]string{
		"PELICAN_MANAGER_POLL_INTERVAL": "5m",
		"PELICAN_MANAGER_LIMIT_LEASE":   "60s",
	}))
	if err != nil {
		t.Fatalf("LoadFrom: %v", err)
	}
	if cfg.LimitLeaseWarning != "" {
		t.Errorf("poll interval still drives the lease warning: %s", cfg.LimitLeaseWarning)
	}
}

// TestStateDBFollowsTheRuleDB: one htcondordb is the common case, so a site
// that already put its rules there should not have to name the same address
// twice.
func TestStateDBFollowsTheRuleDB(t *testing.T) {
	cfg, err := LoadFrom(newCondorConfig(t, map[string]string{
		"PELICAN_MANAGER_RULE_DB_ADDRESS": "db.example.org:9618",
	}))
	if err != nil {
		t.Fatalf("LoadFrom: %v", err)
	}
	if cfg.StateDBAddress != "db.example.org:9618" {
		t.Errorf("StateDBAddress = %q, want the rule DB address", cfg.StateDBAddress)
	}
}

// TestStateDBIsOffByDefault: the JSON document under SPOOL is what every
// existing deployment uses, and no configuration should quietly move it.
func TestStateDBIsOffByDefault(t *testing.T) {
	cfg, err := LoadFrom(newCondorConfig(t, nil))
	if err != nil {
		t.Fatalf("LoadFrom: %v", err)
	}
	if cfg.StateDBAddress != "" {
		t.Errorf("StateDBAddress = %q, want empty (state stays in the SPOOL file)", cfg.StateDBAddress)
	}
	if cfg.StatePath == "" {
		t.Error("no StatePath default; there would be nowhere to write state")
	}
}

// TestStateDBCanBeSplitFromTheRuleDB: state is written every poll cycle and the
// rules almost never, so a site may well want them in different places.
func TestStateDBCanBeSplitFromTheRuleDB(t *testing.T) {
	cfg, err := LoadFrom(newCondorConfig(t, map[string]string{
		"PELICAN_MANAGER_RULE_DB_ADDRESS":  "rules.example.org:9618",
		"PELICAN_MANAGER_STATE_DB_ADDRESS": "state.example.org:9618",
		"PELICAN_MANAGER_STATE_DB_TABLE":   "ap40_state",
	}))
	if err != nil {
		t.Fatalf("LoadFrom: %v", err)
	}
	if cfg.StateDBAddress != "state.example.org:9618" || cfg.StateDBTable != "ap40_state" {
		t.Errorf("state DB = %q/%q", cfg.StateDBAddress, cfg.StateDBTable)
	}
}
