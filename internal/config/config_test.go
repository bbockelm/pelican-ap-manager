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
