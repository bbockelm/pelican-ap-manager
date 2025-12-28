package daemon

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"log"
	"math"
	"time"

	htcondor "github.com/bbockelm/golang-htcondor"
	"github.com/bbockelm/pelican-ap-manager/internal/control"
	"github.com/bbockelm/pelican-ap-manager/internal/stats"
)

const (
	// defaultLimitInterval is the rate window for schedd limits (aligned with negotiation cycle)
	defaultLimitInterval = 60 * time.Second

	// limitExpirationInactivity is how long a limit can go without being hit before removal
	limitExpirationInactivity = 600 * time.Second

	// ewmaAlpha is the smoothing factor for EWMA calculation (window ~= 2/(alpha+1) intervals)
	ewmaAlpha = 0.2
)

// limitManager tracks active schedd startup limits and their usage
type limitManager struct {
	schedd       *htcondor.Schedd
	logger       *log.Logger
	activeLimits map[string]*limitState
	cfg          limitConfig
	daemonName   string
}

type limitConfig struct {
	interval              time.Duration
	expirationInactivity  time.Duration
	ewmaAlpha             float64
	enabled               bool
}

type limitState struct {
	uuid          string
	pairKey       control.PairKey
	lastHit       time.Time
	lastUpdated   time.Time
	rateCount     int
	capacityGBMin float64
}

// newLimitManager creates a limit manager for the schedd
func newLimitManager(schedd *htcondor.Schedd, daemonName string, logger *log.Logger) *limitManager {
	m := &limitManager{
		schedd:       schedd,
		logger:       logger,
		activeLimits: make(map[string]*limitState),
		daemonName:   daemonName,
		cfg: limitConfig{
			interval:             defaultLimitInterval,
			expirationInactivity: limitExpirationInactivity,
			ewmaAlpha:            ewmaAlpha,
			enabled:              true,
		},
	}
	
	// Test if schedd supports rate limits by attempting a query
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	_, err := schedd.QueryStartupLimits(ctx, "", "")
	if err != nil {
		logger.Printf("schedd does not support startup limits (disabling limit manager): %v", err)
		m.cfg.enabled = false
	}
	
	return m
}

// updateLimits synchronizes schedd limits based on pair controller states
// Only creates/updates limits when a pair is in RED state; removes stale limits
func (m *limitManager) updateLimits(ctx context.Context, pairs map[control.PairKey]control.PairState, tracker *stats.Tracker, controlCfg control.Config) error {
	if !m.cfg.enabled {
		return nil
	}

	now := time.Now()

	// Determine which pairs need limits (RED state only)
	pairsNeedingLimits := make(map[control.PairKey]control.PairState)
	for pair, state := range pairs {
		metrics := pairMetrics(controlCfg, nil, tracker, pair.Source, pair.Destination)
		
		// Check if error or cost metrics are in RED band
		errorBand := control.ClassifyBand(metrics.ErrorRate, controlCfg.ErrorGreenThreshold, controlCfg.ErrorYellowThreshold)
		costBand := control.ClassifyBand(metrics.CostPct/100.0, controlCfg.CostGreenThresholdPercent/100.0, controlCfg.CostYellowThresholdPercent/100.0)
		
		if errorBand == control.BandRed || costBand == control.BandRed {
			pairsNeedingLimits[pair] = state
		}
	}

	// Update or create limits for pairs in RED state
	for pair, state := range pairsNeedingLimits {
		tag := m.limitTag(pair)
		existing, exists := m.activeLimits[tag]
		
		rateCount := m.calculateRateCount(state, tracker, pair, controlCfg)
		
		if exists {
			// Update existing limit if rate changed significantly
			if m.shouldUpdateLimit(existing, rateCount, state.CapacityGBPerMin) {
				if err := m.updateLimit(ctx, existing, pair, rateCount, state.CapacityGBPerMin); err != nil {
					m.logger.Printf("limit update error for %s->%s: %v", pair.Source, pair.Destination, err)
				}
			}
		} else {
			// Create new limit
			if err := m.createLimit(ctx, pair, rateCount, state.CapacityGBPerMin); err != nil {
				m.logger.Printf("limit create error for %s->%s: %v", pair.Source, pair.Destination, err)
			}
		}
	}

	// Remove stale limits (not hit recently and no longer in RED state)
	if err := m.removeStale(ctx, now, pairsNeedingLimits); err != nil {
		m.logger.Printf("limit cleanup error: %v", err)
	}

	// Query current limits to update lastHit times
	if err := m.refreshLimitStats(ctx); err != nil {
		m.logger.Printf("limit refresh error: %v", err)
	}

	return nil
}

// calculateRateCount converts capacity (GB/min) to jobs per interval using EWMA for initial estimate
func (m *limitManager) calculateRateCount(state control.PairState, tracker *stats.Tracker, pair control.PairKey, cfg control.Config) int {
	capacityGBPerMin := state.CapacityGBPerMin
	if capacityGBPerMin <= 0 {
		capacityGBPerMin = cfg.MinCapacityGBPerMin
	}

	// Scale capacity to the limit interval
	intervalMin := m.cfg.interval.Minutes()
	capacityGBPerInterval := capacityGBPerMin * intervalMin

	// Estimate job cost (data size per job) using EWMA if available
	jobCostGB := cfg.DefaultJobCostGB
	if tracker != nil {
		recentRate := tracker.AverageRate(pair.Source, pair.Destination)
		if recentRate > 0 {
			// Use EWMA to smooth the job cost estimate
			// Average job duration * rate = bytes per job
			// Assume jobs take ~5 minutes on average for initial estimate
			const avgJobDurationMin = 5.0
			bytesPerJob := recentRate * (avgJobDurationMin * 60)
			gbPerJob := bytesPerJob / (1024 * 1024 * 1024)
			
			if gbPerJob > 0 {
				// Apply EWMA smoothing with previous estimate
				if jobCostGB > 0 {
					jobCostGB = m.cfg.ewmaAlpha*gbPerJob + (1-m.cfg.ewmaAlpha)*jobCostGB
				} else {
					jobCostGB = gbPerJob
				}
			}
		}
	}

	if jobCostGB <= 0 {
		jobCostGB = cfg.DefaultJobCostGB
	}

	// Calculate jobs per interval
	jobsPerInterval := capacityGBPerInterval / jobCostGB
	
	// Apply floor
	minJobsPerInterval := cfg.MinJobStartPerMinute * intervalMin
	if jobsPerInterval < minJobsPerInterval {
		jobsPerInterval = minJobsPerInterval
	}

	return int(math.Ceil(jobsPerInterval))
}

// shouldUpdateLimit determines if a limit needs updating based on rate changes
func (m *limitManager) shouldUpdateLimit(existing *limitState, newRateCount int, newCapacityGBMin float64) bool {
	// Update if rate count differs by more than 20% or capacity changed significantly
	rateDiff := math.Abs(float64(newRateCount-existing.rateCount)) / float64(existing.rateCount)
	capacityDiff := math.Abs(newCapacityGBMin-existing.capacityGBMin) / existing.capacityGBMin
	
	return rateDiff > 0.2 || capacityDiff > 0.2
}

// createLimit creates a new schedd startup limit
func (m *limitManager) createLimit(ctx context.Context, pair control.PairKey, rateCount int, capacityGBMin float64) error {
	tag := m.limitTag(pair)
	name := fmt.Sprintf("pelican_%s_to_%s", sanitizeLimitLabel(pair.Source), sanitizeLimitLabel(pair.Destination))
	
	// Build ClassAd expression to match jobs with this source->destination pair
	// This is a simplified expression; production would use actual job attributes
	expression := fmt.Sprintf("(PelicanSource == %q && PelicanDestination == %q)", pair.Source, pair.Destination)
	
	// Set expiration to 2x the inactivity timeout to allow for some delay in cleanup
	expirationTime := time.Now().Add(m.cfg.expirationInactivity * 2).Unix()
	
	req := &htcondor.StartupLimitRequest{
		Tag:        tag,
		Name:       name,
		Expression: expression,
		RateCount:  rateCount,
		RateWindow: int(m.cfg.interval.Seconds()),
		Expiration: int(expirationTime),
	}
	
	uuid, err := m.schedd.CreateStartupLimit(ctx, req)
	if err != nil {
		return fmt.Errorf("create startup limit: %w", err)
	}
	
	m.activeLimits[tag] = &limitState{
		uuid:          uuid,
		pairKey:       pair,
		lastHit:       time.Now(),
		lastUpdated:   time.Now(),
		rateCount:     rateCount,
		capacityGBMin: capacityGBMin,
	}
	
	m.logger.Printf("created limit %s for %s->%s: %d jobs/%ds (%.1f GB/min capacity)",
		uuid, pair.Source, pair.Destination, rateCount, int(m.cfg.interval.Seconds()), capacityGBMin)
	
	return nil
}

// updateLimit updates an existing schedd startup limit
func (m *limitManager) updateLimit(ctx context.Context, existing *limitState, pair control.PairKey, rateCount int, capacityGBMin float64) error {
	name := fmt.Sprintf("pelican_%s_to_%s", sanitizeLimitLabel(pair.Source), sanitizeLimitLabel(pair.Destination))
	expression := fmt.Sprintf("(PelicanSource == %q && PelicanDestination == %q)", pair.Source, pair.Destination)
	
	// Refresh expiration time on every update
	expirationTime := time.Now().Add(m.cfg.expirationInactivity * 2).Unix()
	
	req := &htcondor.StartupLimitRequest{
		UUID:       existing.uuid,
		Tag:        m.limitTag(pair),
		Name:       name,
		Expression: expression,
		RateCount:  rateCount,
		RateWindow: int(m.cfg.interval.Seconds()),
		Expiration: int(expirationTime),
	}
	
	uuid, err := m.schedd.CreateStartupLimit(ctx, req)
	if err != nil {
		return fmt.Errorf("update startup limit: %w", err)
	}
	
	existing.uuid = uuid
	existing.rateCount = rateCount
	existing.capacityGBMin = capacityGBMin
	existing.lastUpdated = time.Now()
	
	m.logger.Printf("updated limit %s for %s->%s: %d jobs/%ds (%.1f GB/min capacity)",
		uuid, pair.Source, pair.Destination, rateCount, int(m.cfg.interval.Seconds()), capacityGBMin)
	
	return nil
}

// removeStale removes limits that haven't been hit recently and are no longer in RED state
func (m *limitManager) removeStale(ctx context.Context, now time.Time, activeRedPairs map[control.PairKey]control.PairState) error {
	for tag, limit := range m.activeLimits {
		// Keep limit if still in RED state
		if _, inRed := activeRedPairs[limit.pairKey]; inRed {
			continue
		}
		
		// Remove if not hit for expiration period
		if now.Sub(limit.lastHit) > m.cfg.expirationInactivity {
			// To remove a limit, we query and delete via the tag
			// The golang-htcondor API handles removal through the schedd
			// For now, just remove from our tracking (schedd will expire it)
			delete(m.activeLimits, tag)
			m.logger.Printf("removed stale limit for %s->%s (inactive for %v)",
				limit.pairKey.Source, limit.pairKey.Destination, now.Sub(limit.lastHit))
		}
	}
	
	return nil
}

// refreshLimitStats queries the schedd to update lastHit times based on actual usage
func (m *limitManager) refreshLimitStats(ctx context.Context) error {
	for tag, limit := range m.activeLimits {
		limits, err := m.schedd.QueryStartupLimits(ctx, limit.uuid, "")
		if err != nil {
			return fmt.Errorf("query limit %s: %w", limit.uuid, err)
		}
		
		if len(limits) > 0 {
			limitInfo := limits[0]
			// Update lastHit if the limit was actually hit (jobs were skipped)
			if limitInfo.LastIgnored > 0 && time.Unix(limitInfo.LastIgnored, 0).After(limit.lastHit) {
				limit.lastHit = time.Unix(limitInfo.LastIgnored, 0)
				m.logger.Printf("limit %s for %s->%s was hit at %v (skipped=%d)",
					limit.uuid, limit.pairKey.Source, limit.pairKey.Destination,
					limit.lastHit, limitInfo.JobsSkipped)
			}
			m.activeLimits[tag] = limit
		}
	}
	
	return nil
}

// limitTag generates a stable tag for a source->destination pair using daemon name
func (m *limitManager) limitTag(pair control.PairKey) string {
	// Tag format: <daemon_name>:<pair_hash>
	// This allows filtering limits by daemon while still uniquely identifying pairs
	h := sha256.New()
	h.Write([]byte(pair.Source))
	h.Write([]byte{0})
	h.Write([]byte(pair.Destination))
	sum := h.Sum(nil)
	return m.daemonName + ":pelican_" + hex.EncodeToString(sum[:8])
}

// sanitizeLimitLabel cleans a string for use in limit names
func sanitizeLimitLabel(s string) string {
	if len(s) > 20 {
		s = s[:20]
	}
	result := ""
	for _, r := range s {
		if (r >= 'a' && r <= 'z') || (r >= 'A' && r <= 'Z') || (r >= '0' && r <= '9') || r == '_' {
			result += string(r)
		} else {
			result += "_"
		}
	}
	return result
}

// getLimitInfo returns the active limit information for a pair, if any
func (m *limitManager) getLimitInfo(pair control.PairKey) (rateCount int, rateWindow int, active bool) {
	if !m.cfg.enabled {
		return 0, 0, false
	}
	
	tag := m.limitTag(pair)
	if limit, exists := m.activeLimits[tag]; exists {
		return limit.rateCount, int(m.cfg.interval.Seconds()), true
	}
	
	return 0, int(m.cfg.interval.Seconds()), false
}
