package config

import (
	"fmt"
	"os"
	"strings"
	"time"

	condorconfig "github.com/bbockelm/golang-htcondor/config"
	"github.com/bbockelm/pelican-ap-manager/internal/ratelimit"
)

// Config holds runtime options for the pelican_man daemon.
type Config struct {
	PollInterval      time.Duration
	AdvertiseInterval time.Duration
	EpochLookback     time.Duration
	StatsWindow       time.Duration
	DirectorCacheTTL  time.Duration
	StatePath         string
	InfoPath          string
	CollectorHost     string
	ScheddName        string
	SiteAttribute     string
	JobMirrorPath     string
	JobQueueLogPath   string
	LogPath           string
	AddressFilePath   string

	// EnforcementMode decides what the daemon does with the rate limits it
	// derives: install them, or compute and publish them while installing only
	// the operator's static rules. See internal/ratelimit.
	EnforcementMode ratelimit.Mode

	// StaticRules are the operator's rate rules, declared in the HTCondor
	// configuration (PELICAN_MANAGER_RATE_RULES plus one
	// PELICAN_MANAGER_RATE_RULE_<NAME> per rule). They are applied in every
	// enforcement mode.
	StaticRules []ratelimit.Rule

	// RuleStorePath is the JSON document backing the rate-rule store when no
	// htcondordb is configured.
	RuleStorePath string

	// RuleDBAddress, when set, points the rate-rule store at an htcondordb
	// daemon instead of the local JSON document. RuleDBTable names the table.
	RuleDBAddress string
	RuleDBTable   string

	condorCfg *condorconfig.Config // Store for logging initialization
}

const (
	defaultPollInterval      = 30 * time.Second
	defaultAdvertiseInterval = 1 * time.Minute
	defaultEpochLookback     = 24 * time.Hour
	defaultStatsWindow       = 1 * time.Hour
	defaultDirectorCacheTTL  = 15 * time.Minute
	defaultCollectorHost     = "localhost:9618"
	defaultScheddName        = ""
	defaultSiteAttribute     = "MachineAttrGLIDEIN_ResourceName0"
	defaultJobMirrorPath     = ""
	defaultJobQueueLogPath   = ""

	macroPollInterval            = "PELICAN_MANAGER_POLL_INTERVAL"
	macroPollIntervalLegacy      = "PEL_POLL_INTERVAL"
	macroAdvertiseInterval       = "PELICAN_MANAGER_ADVERTISE_INTERVAL"
	macroAdvertiseIntervalLegacy = "PEL_ADVERTISE_INTERVAL"
	macroEpochLookback           = "PELICAN_MANAGER_EPOCH_LOOKBACK"
	macroEpochLookbackLegacy     = "PEL_EPOCH_LOOKBACK"
	macroStatePath               = "PELICAN_MANAGER_STATE_PATH"
	macroStatePathLegacy         = "PEL_STATE_PATH"
	macroInfoPath                = "PELICAN_MANAGER_INFO_PATH"
	macroInfoPathLegacy          = "PEL_INFO_PATH"
	macroSpool                   = "SPOOL"
	macroCollectorHost           = "PELICAN_MANAGER_COLLECTOR_HOST"
	macroCollectorHostLegacy     = "COLLECTOR_HOST"
	macroScheddName              = "PELICAN_MANAGER_SCHEDD_NAME"
	macroScheddNameLegacy        = "SCHEDD_NAME"
	macroSiteAttribute           = "PELICAN_MANAGER_SITE_ATTRIBUTE"
	macroSiteAttributeLegacy     = "PEL_SITE_ATTRIBUTE"
	macroJobMirrorPath           = "PELICAN_MANAGER_JOB_MIRROR_PATH"
	macroJobMirrorPathLegacy     = "PEL_JOB_MIRROR_PATH"
	macroStatsWindow             = "PELICAN_MANAGER_STATS_WINDOW"
	macroDirectorCacheTTL        = "PELICAN_MANAGER_DIRECTOR_CACHE_TTL"
	macroJobQueueLog             = "JOB_QUEUE_LOG"
	macroAddressFilePath         = "PELICAN_MANAGER_ADDRESS_FILE"
	macroEnforcementMode         = "PELICAN_MANAGER_ENFORCEMENT_MODE"
	macroRateRules               = "PELICAN_MANAGER_RATE_RULES"
	macroRateRulePrefix          = "PELICAN_MANAGER_RATE_RULE_"
	macroRuleStorePath           = "PELICAN_MANAGER_RULE_STORE_PATH"
	macroRuleDBAddress           = "PELICAN_MANAGER_RULE_DB_ADDRESS"
	macroRuleDBTable             = "PELICAN_MANAGER_RULE_DB_TABLE"
)

// defaultEnforcementMode preserves the daemon's historical behavior: limits
// derived by the control loop are installed. An operator evaluating the
// controller sets PELICAN_MANAGER_ENFORCEMENT_MODE = observing, which keeps the
// static rules in force and withholds only the derived ones.
const defaultEnforcementMode = ratelimit.ModeEnforcing

// Load returns configuration derived from the active HTCondor configuration,
// mirroring how condor tools discover settings. Macros can be set in the
// condor config; defaults are provided for missing values.
func Load() (*Config, error) {
	condorCfg, err := condorconfig.New()
	if err != nil {
		return nil, fmt.Errorf("condor config: %w", err)
	}
	return LoadFrom(condorCfg)
}

// LoadFrom is Load over an already-loaded HTCondor configuration. The daemon
// bootstrap (daemon.New) owns config loading -- it needs the subsystem and
// local-name scoping in place before it drops privileges and opens the log --
// so the daemon hands its config here rather than having us load a second,
// unscoped copy. Reconfigure (SIGHUP) takes the same path with the freshly
// reloaded config.
func LoadFrom(condorCfg *condorconfig.Config) (*Config, error) {
	if condorCfg == nil {
		return nil, fmt.Errorf("condor config: nil")
	}

	// Get SPOOL directory for default paths
	spoolDir := firstStringMacro(condorCfg, macroSpool)
	if spoolDir == "" {
		spoolDir = "./data"
	}

	// Get LOG directory for default paths
	logDir := firstStringMacro(condorCfg, "LOG")
	if logDir == "" {
		logDir = "./log"
	}

	cfg := &Config{
		PollInterval:      defaultPollInterval,
		AdvertiseInterval: defaultAdvertiseInterval,
		EpochLookback:     defaultEpochLookback,
		StatsWindow:       defaultStatsWindow,
		DirectorCacheTTL:  defaultDirectorCacheTTL,
		StatePath:         fmt.Sprintf("%s/pelican_state.json", spoolDir),
		InfoPath:          fmt.Sprintf("%s/pelican_info.json", spoolDir),
		CollectorHost:     defaultCollectorHost,
		ScheddName:        defaultScheddName,
		SiteAttribute:     defaultSiteAttribute,
		JobMirrorPath:     defaultJobMirrorPath,
		JobQueueLogPath:   defaultJobQueueLogPath,
		LogPath:           logDir,
		AddressFilePath:   "", // Will be set based on LOG directory
		EnforcementMode:   defaultEnforcementMode,
		RuleStorePath:     fmt.Sprintf("%s/pelican_rate_rules.json", spoolDir),
		condorCfg:         condorCfg,
	}

	if d, err := parseDurationMacro(condorCfg, macroPollInterval, macroPollIntervalLegacy); err != nil {
		return nil, fmt.Errorf("invalid %s: %w", macroPollInterval, err)
	} else if d > 0 {
		cfg.PollInterval = d
	}

	if d, err := parseDurationMacro(condorCfg, macroAdvertiseInterval, macroAdvertiseIntervalLegacy); err != nil {
		return nil, fmt.Errorf("invalid %s: %w", macroAdvertiseInterval, err)
	} else if d > 0 {
		cfg.AdvertiseInterval = d
	}

	if d, err := parseDurationMacro(condorCfg, macroEpochLookback, macroEpochLookbackLegacy); err != nil {
		return nil, fmt.Errorf("invalid %s: %w", macroEpochLookback, err)
	} else if d > 0 {
		cfg.EpochLookback = d
	}

	if d, err := parseDurationMacro(condorCfg, macroStatsWindow); err != nil {
		return nil, fmt.Errorf("invalid %s: %w", macroStatsWindow, err)
	} else if d > 0 {
		cfg.StatsWindow = d
	}

	if d, err := parseDurationMacro(condorCfg, macroDirectorCacheTTL); err != nil {
		return nil, fmt.Errorf("invalid %s: %w", macroDirectorCacheTTL, err)
	} else if d > 0 {
		cfg.DirectorCacheTTL = d
	}

	if v := firstStringMacro(condorCfg, macroStatePath, macroStatePathLegacy); v != "" {
		cfg.StatePath = v
	}
	if v := firstStringMacro(condorCfg, macroInfoPath, macroInfoPathLegacy); v != "" {
		cfg.InfoPath = v
	}
	if v := firstStringMacro(condorCfg, macroCollectorHost, macroCollectorHostLegacy); v != "" {
		cfg.CollectorHost = v
	}

	// If COLLECTOR_HOST ends with :0 or is not resolvable, try reading .collector_address file
	if needsCollectorAddressFile(cfg.CollectorHost) {
		if addr := readCollectorAddressFile(cfg.LogPath); addr != "" {
			cfg.CollectorHost = addr
		}
	}

	if v := firstStringMacro(condorCfg, macroScheddName, macroScheddNameLegacy); v != "" {
		cfg.ScheddName = v
	}

	if v := firstStringMacro(condorCfg, macroSiteAttribute, macroSiteAttributeLegacy); v != "" {
		cfg.SiteAttribute = v
	}
	if v := firstStringMacro(condorCfg, macroJobMirrorPath, macroJobMirrorPathLegacy); v != "" {
		cfg.JobMirrorPath = v
	}
	if v := firstStringMacro(condorCfg, macroJobQueueLog); v != "" {
		cfg.JobQueueLogPath = v
	}

	// Set address file path using LOG directory
	if cfg.LogPath != "" {
		// Only set AddressFilePath if not explicitly configured
		if v := firstStringMacro(condorCfg, macroAddressFilePath); v != "" {
			cfg.AddressFilePath = v
		} else {
			cfg.AddressFilePath = fmt.Sprintf("%s/.pelican_manager_address", cfg.LogPath)
		}
	}

	if v := firstStringMacro(condorCfg, macroEnforcementMode); v != "" {
		mode, err := ratelimit.ParseMode(v)
		if err != nil {
			return nil, fmt.Errorf("invalid %s: %w", macroEnforcementMode, err)
		}
		cfg.EnforcementMode = mode
	}
	if v := firstStringMacro(condorCfg, macroRuleStorePath); v != "" {
		cfg.RuleStorePath = v
	}
	if v := firstStringMacro(condorCfg, macroRuleDBAddress); v != "" {
		cfg.RuleDBAddress = v
	}
	if v := firstStringMacro(condorCfg, macroRuleDBTable); v != "" {
		cfg.RuleDBTable = v
	}

	rules, err := loadStaticRules(condorCfg)
	if err != nil {
		return nil, err
	}
	cfg.StaticRules = rules

	// Keep the underlying HTCondor configuration for downstream components (logging, HTTP handler, etc.).
	cfg.condorCfg = condorCfg

	return cfg, nil
}

// loadStaticRules reads the operator's rate rules. The list macro names them;
// each name has its own macro carrying the rule body:
//
//	PELICAN_MANAGER_RATE_RULES = ligo_ucsd, psu_all
//	PELICAN_MANAGER_RATE_RULE_LIGO_UCSD = user=ligo site=UCSD rate=20 window=60s
//	PELICAN_MANAGER_RATE_RULE_PSU_ALL   = site=PSU-LIGO rate=5 window=1m note="incident 4471"
//
// One macro per rule rather than one macro holding all of them: it keeps each
// rule independently overridable from a config.d drop-in, which is how these
// tend to be deployed (and undeployed).
//
// A named rule with no body, or a body that does not parse, is an error rather
// than a skipped rule. Silently ignoring a malformed rate limit would leave the
// operator believing a limit is in force when it is not.
func loadStaticRules(condorCfg *condorconfig.Config) ([]ratelimit.Rule, error) {
	raw := firstStringMacro(condorCfg, macroRateRules)
	if raw == "" {
		return nil, nil
	}

	var rules []ratelimit.Rule
	seen := make(map[string]bool)
	for _, name := range strings.Split(raw, ",") {
		name = strings.TrimSpace(name)
		if name == "" {
			continue
		}
		if seen[name] {
			return nil, fmt.Errorf("%s lists %q more than once", macroRateRules, name)
		}
		seen[name] = true

		macro := macroRateRulePrefix + strings.ToUpper(name)
		spec := firstStringMacro(condorCfg, macro)
		if spec == "" {
			return nil, fmt.Errorf("%s names rule %q but %s is not set", macroRateRules, name, macro)
		}
		rule, err := ratelimit.ParseRule(name, spec)
		if err != nil {
			return nil, fmt.Errorf("%s: %w", macro, err)
		}
		rule.ConfigManaged = true
		rules = append(rules, rule)
	}
	ratelimit.SortRules(rules)
	return rules, nil
}

// WithOverrides applies optional overrides for unit tests or CLI flags.
func (c *Config) WithOverrides(poll, adv, lookback, statsWindow, directorTTL time.Duration, statePath, infoPath, collector, schedd, site, jobMirrorPath string) *Config {
	if poll > 0 {
		c.PollInterval = poll
	}
	if adv > 0 {
		c.AdvertiseInterval = adv
	}
	if lookback > 0 {
		c.EpochLookback = lookback
	}
	if statsWindow > 0 {
		c.StatsWindow = statsWindow
	}
	if directorTTL > 0 {
		c.DirectorCacheTTL = directorTTL
	}
	if statePath != "" {
		c.StatePath = statePath
	}
	if infoPath != "" {
		c.InfoPath = infoPath
	}
	if collector != "" {
		c.CollectorHost = collector
	}
	if schedd != "" {
		c.ScheddName = schedd
	}
	if site != "" {
		c.SiteAttribute = site
	}
	if jobMirrorPath != "" {
		c.JobMirrorPath = jobMirrorPath
	}
	return c
}

// HTCondorConfig returns the underlying HTCondor configuration object for use by logging and other components.
func (c *Config) HTCondorConfig() *condorconfig.Config {
	return c.condorCfg
}

// EffectiveIntervals exposes derived intervals useful for logging or validation.
func (c *Config) EffectiveIntervals() string {
	return fmt.Sprintf("poll=%s advertise=%s lookback=%s", c.PollInterval, c.AdvertiseInterval, c.EpochLookback)
}

// EnvMap returns a view of the environment variables used for configuration.
func (c *Config) EnvMap() map[string]string {
	return map[string]string{
		macroPollInterval:      c.PollInterval.String(),
		macroAdvertiseInterval: c.AdvertiseInterval.String(),
		macroEpochLookback:     c.EpochLookback.String(),
		macroStatsWindow:       c.StatsWindow.String(),
		macroDirectorCacheTTL:  c.DirectorCacheTTL.String(),
		macroStatePath:         c.StatePath,
		macroInfoPath:          c.InfoPath,
		macroCollectorHost:     c.CollectorHost,
		macroScheddName:        c.ScheddName,
		macroSiteAttribute:     c.SiteAttribute,
		macroJobMirrorPath:     c.JobMirrorPath,
		macroJobQueueLog:       c.JobQueueLogPath,
	}
}

func parseDurationMacro(cfg *condorconfig.Config, names ...string) (time.Duration, error) {
	for _, name := range names {
		v, ok := cfg.Get(name)
		if !ok || v == "" {
			continue
		}
		d, err := time.ParseDuration(v)
		if err != nil {
			return 0, err
		}
		return d, nil
	}
	return 0, nil
}

func firstStringMacro(cfg *condorconfig.Config, names ...string) string {
	for _, name := range names {
		if v, ok := cfg.Get(name); ok && v != "" {
			return v
		}
	}
	return ""
}

// needsCollectorAddressFile checks if the collector host needs to be resolved from the address file.
// Returns true if the host ends with :0 (dynamic port) or if it's not resolvable.
func needsCollectorAddressFile(collectorHost string) bool {
	if collectorHost == "" {
		return false
	}

	// Check if it ends with :0 (dynamic port assignment)
	if len(collectorHost) > 2 && collectorHost[len(collectorHost)-2:] == ":0" {
		return true
	}

	return false
}

// readCollectorAddressFile reads the collector address from LOG/.collector_address file.
// This is used when COLLECTOR_HOST is configured with a dynamic port (:0).
func readCollectorAddressFile(logDir string) string {
	if logDir == "" {
		return ""
	}

	addrFile := fmt.Sprintf("%s/.collector_address", logDir)
	data, err := os.ReadFile(addrFile)
	if err != nil {
		return ""
	}

	// Parse the address file - it may contain multiple lines, we want the sinful string
	for _, line := range strings.Split(string(data), "\n") {
		line = strings.TrimSpace(line)
		if line == "" || strings.Contains(line, "(null)") {
			continue
		}
		// Look for sinful string format: <IP:port...>
		if strings.HasPrefix(line, "<") {
			// Extract host:port from sinful string
			if idx := strings.Index(line, "?"); idx > 0 {
				// Remove the sinful wrapper and query params
				return line[1:idx]
			}
			if idx := strings.Index(line, ">"); idx > 0 {
				return line[1:idx]
			}
		}
	}

	return ""
}
