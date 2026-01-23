package config

import (
	"fmt"
	"os"
	"strings"
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
	ScheddAddr        string
	ScheddName        string
	SiteAttribute     string
	JobMirrorPath     string
	JobQueueLogPath   string
	LogPath           string
	WebListenAddress  string
	WebSocketPath     string
	WebTLSCert        string
	WebTLSKey         string
	WebDBPath         string
	AddressFilePath   string
	MasterSockPath    string
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
	defaultWebSocketPath     = "" // Will be set to $(SPOOL)/pelican_manager.sock

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
	macroWebListenAddress        = "PELICAN_MANAGER_WEB_LISTEN_ADDRESS"
	macroWebSocketPath           = "PELICAN_REGISTRATION_SOCKET"
	macroWebTLSCert              = "PELICAN_MANAGER_WEB_TLS_CERT"
	macroWebTLSKey               = "PELICAN_MANAGER_WEB_TLS_KEY"
	macroWebDBPath               = "PELICAN_MANAGER_WEB_DB_PATH"
	macroAddressFilePath         = "PELICAN_MANAGER_ADDRESS_FILE"
	macroMasterSockPath          = "MASTER_ADDR_FILE"
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
		ScheddAddr:        "",
		ScheddName:        defaultScheddName,
		SiteAttribute:     defaultSiteAttribute,
		JobMirrorPath:     defaultJobMirrorPath,
		JobQueueLogPath:   defaultJobQueueLogPath,
		LogPath:           logDir,
		WebListenAddress:  "",
		WebSocketPath:     fmt.Sprintf("%s/pelican_manager.sock", spoolDir),
		WebTLSCert:        fmt.Sprintf("%s/pelican-certs/server.crt", spoolDir),
		WebTLSKey:         fmt.Sprintf("%s/pelican-certs/server.key", spoolDir),
		WebDBPath:         fmt.Sprintf("%s/pelican_web.db", spoolDir),
		AddressFilePath:   "", // Will be set based on LOG directory
		MasterSockPath:    "", // Will be discovered from master
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

	// Resolve Schedd address from config or address file
	if scheddHost := firstStringMacro(condorCfg, "SCHEDD_HOST"); scheddHost != "" {
		scheddPort := firstStringMacro(condorCfg, "SCHEDD_PORT")
		if scheddPort == "" {
			scheddPort = "9618"
		}
		cfg.ScheddAddr = fmt.Sprintf("%s:%s", scheddHost, scheddPort)
	} else if addr := readScheddAddressFile(cfg.LogPath); addr != "" {
		cfg.ScheddAddr = addr
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
	if v := firstStringMacro(condorCfg, macroWebListenAddress); v != "" {
		cfg.WebListenAddress = v
	}
	if v := firstStringMacro(condorCfg, macroWebSocketPath); v != "" {
		cfg.WebSocketPath = v
	}
	if v := firstStringMacro(condorCfg, macroWebTLSCert); v != "" {
		cfg.WebTLSCert = v
	}
	if v := firstStringMacro(condorCfg, macroWebTLSKey); v != "" {
		cfg.WebTLSKey = v
	}
	if v := firstStringMacro(condorCfg, macroWebDBPath); v != "" {
		cfg.WebDBPath = v
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

	// Get master socket path for heartbeat communication
	if v := firstStringMacro(condorCfg, macroMasterSockPath); v != "" {
		cfg.MasterSockPath = v
	}

	// Keep the underlying HTCondor configuration for downstream components (logging, HTTP handler, etc.).
	cfg.condorCfg = condorCfg

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

// readScheddAddressFile reads the schedd address from LOG/.schedd_address file.
func readScheddAddressFile(logDir string) string {
	if logDir == "" {
		return ""
	}

	addrFile := fmt.Sprintf("%s/.schedd_address", logDir)
	data, err := os.ReadFile(addrFile)
	if err != nil {
		return ""
	}

	for _, line := range strings.Split(string(data), "\n") {
		line = strings.TrimSpace(line)
		if line == "" || strings.Contains(line, "(null)") {
			continue
		}
		if strings.HasPrefix(line, "<") {
			if idx := strings.Index(line, "?"); idx > 0 {
				return line[1:idx]
			}
			if idx := strings.Index(line, ">"); idx > 0 {
				return line[1:idx]
			}
		}
	}

	return ""
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
