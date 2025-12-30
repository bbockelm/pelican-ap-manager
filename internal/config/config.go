package config

import (
	"fmt"
	"time"

	condorconfig "github.com/bbockelm/golang-htcondor/config"
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
	condorCfg         *condorconfig.Config // Store for logging initialization
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
)

// Load returns configuration derived from the active HTCondor configuration,
// mirroring how condor tools discover settings. Macros can be set in the
// condor config; defaults are provided for missing values.
func Load() (*Config, error) {
	condorCfg, err := condorconfig.New()
	if err != nil {
		return nil, fmt.Errorf("condor config: %w", err)
	}

	// Get SPOOL directory for default paths
	spoolDir := firstStringMacro(condorCfg, macroSpool)
	if spoolDir == "" {
		spoolDir = "./data"
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

	return cfg, nil
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
