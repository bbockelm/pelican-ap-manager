package daemon

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"log"
	"math"
	"strings"
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

// UserSitePair identifies a (user,site) combination for rate limiting
type UserSitePair struct {
	User string
	Site string
}

// limitManager tracks active schedd startup limits and their usage
type limitManager struct {
	schedd        *htcondor.Schedd
	logger        *log.Logger
	activeLimits  map[string]*limitState
	cfg           limitConfig
	daemonName    string
	siteAttribute string
}

type limitConfig struct {
	interval             time.Duration
	expirationInactivity time.Duration
	ewmaAlpha            float64
	enabled              bool
}

type limitState struct {
	uuid          string
	userSitePair  UserSitePair
	lastHit       time.Time
	lastUpdated   time.Time
	rateCount     int
	capacityGBMin float64
	hitCount      int64
	jobsSkipped   int64
}

// newLimitManager creates a limit manager for the schedd
func newLimitManager(schedd *htcondor.Schedd, daemonName string, siteAttribute string, logger *log.Logger) *limitManager {
	m := &limitManager{
		schedd:        schedd,
		logger:        logger,
		activeLimits:  make(map[string]*limitState),
		daemonName:    daemonName,
		siteAttribute: siteAttribute,
		cfg: limitConfig{
			interval:             defaultLimitInterval,
			expirationInactivity: limitExpirationInactivity,
			ewmaAlpha:            ewmaAlpha,
			enabled:              true,
		},
	}

	// Test if schedd supports rate limits and re-adopt existing limits
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	// Query for limits with our daemon's tag to re-adopt them
	limits, err := schedd.QueryStartupLimits(ctx, "", m.daemonName)
	if err != nil {
		logger.Printf("schedd does not support startup limits (disabling limit manager): %v", err)
		m.cfg.enabled = false
		return m
	}

	// Re-adopt existing limits
	for _, limitInfo := range limits {
		user, site, ok := parseLimitExpression(limitInfo.Expression, siteAttribute)
		if !ok {
			logger.Printf("skipping limit %s: could not parse expression %q", limitInfo.UUID, limitInfo.Expression)
			continue
		}

		pair := UserSitePair{User: user, Site: site}
		pairTag := pairKey(pair)

		lastHit := time.Now()
		if limitInfo.LastIgnored > 0 {
			lastHit = time.Unix(limitInfo.LastIgnored, 0)
		}

		m.activeLimits[pairTag] = &limitState{
			uuid:          limitInfo.UUID,
			userSitePair:  pair,
			lastHit:       lastHit,
			lastUpdated:   time.Now(),
			rateCount:     limitInfo.RateCount,
			capacityGBMin: 0, // Will be recomputed on next update
			hitCount:      0,
			jobsSkipped:   0,
		}

		logger.Printf("re-adopted limit %s for user=%s site=%s: %d jobs/%ds",
			limitInfo.UUID, user, site, limitInfo.RateCount, limitInfo.RateWindow)
	}

	if len(m.activeLimits) > 0 {
		logger.Printf("re-adopted %d existing limits from schedd", len(m.activeLimits))
	}

	return m
}

// updateLimits synchronizes schedd limits based on user+site pairs and their states
// Only creates/updates limits when a pair is in RED state; removes stale limits
func (m *limitManager) updateLimits(ctx context.Context, userSitePairs map[UserSitePair]control.PairState, tracker *stats.Tracker, controlCfg control.Config) error {
	if !m.cfg.enabled {
		return nil
	}

	now := time.Now()

	// Determine which user+site pairs need limits (RED state only)
	// The pairs are already filtered by the caller based on their control state
	pairsNeedingLimits := make(map[UserSitePair]control.PairState)
	for pair, state := range userSitePairs {
		// Note: Caller should pre-filter to only pass pairs in RED state
		pairsNeedingLimits[pair] = state
	}

	// Update or create limits for pairs in RED state
	for pair, state := range pairsNeedingLimits {
		pairTag := pairKey(pair)
		existing, exists := m.activeLimits[pairTag]

		// For now, use a default rate count based on capacity
		// TODO: Improve rate count calculation with actual sandbox/transfer metrics
		rateCount := m.calculateRateCountFromCapacity(state.CapacityGBPerMin, controlCfg)

		if exists {
			// Update existing limit if rate changed significantly
			if m.shouldUpdateLimit(existing, rateCount, state.CapacityGBPerMin) {
				if err := m.updateLimit(ctx, existing, pair, rateCount, state.CapacityGBPerMin); err != nil {
					m.logger.Printf("limit update error for user=%s site=%s: %v", pair.User, pair.Site, err)
				}
			}
		} else {
			// Create new limit
			if err := m.createLimit(ctx, pair, rateCount, state.CapacityGBPerMin); err != nil {
				m.logger.Printf("limit create error for user=%s site=%s: %v", pair.User, pair.Site, err)
			}
		}
	}

	// Remove stale limits (not hit recently and no longer in RED state)
	if err := m.removeStale(ctx, now, pairsNeedingLimits); err != nil {
		m.logger.Printf("limit cleanup error: %v", err)
	}

	// Query current limits to update lastHit times and statistics
	if err := m.refreshLimitStats(ctx); err != nil {
		m.logger.Printf("limit refresh error: %v", err)
	}

	return nil
}

// calculateRateCountFromCapacity converts capacity (GB/min) to jobs per interval
func (m *limitManager) calculateRateCountFromCapacity(capacityGBPerMin float64, cfg control.Config) int {
	if capacityGBPerMin <= 0 {
		capacityGBPerMin = cfg.MinCapacityGBPerMin
	}

	// Scale capacity to the limit interval
	intervalMin := m.cfg.interval.Minutes()
	capacityGBPerInterval := capacityGBPerMin * intervalMin

	// Use default job cost for now
	// TODO: Improve with actual sandbox size estimates
	jobCostGB := cfg.DefaultJobCostGB
	if jobCostGB <= 0 {
		jobCostGB = 10.0 // Default 10GB per job
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
func (m *limitManager) createLimit(ctx context.Context, pair UserSitePair, rateCount int, capacityGBMin float64) error {
	tag := m.limitTag()
	name := fmt.Sprintf("pelican_%s_at_%s", sanitizeLimitLabel(pair.User), sanitizeLimitLabel(pair.Site))

	// Build ClassAd expression to match jobs with this user at this site
	// The expression matches on User and the target machine's site attribute
	expression := fmt.Sprintf("(User == %q && TARGET.%s == %q)", pair.User, m.siteAttribute, pair.Site)

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

	pairTag := pairKey(pair)
	m.activeLimits[pairTag] = &limitState{
		uuid:          uuid,
		userSitePair:  pair,
		lastHit:       time.Now(),
		lastUpdated:   time.Now(),
		rateCount:     rateCount,
		capacityGBMin: capacityGBMin,
		hitCount:      0,
		jobsSkipped:   0,
	}

	m.logger.Printf("created limit %s for user=%s site=%s: %d jobs/%ds (%.1f GB/min capacity)",
		uuid, pair.User, pair.Site, rateCount, int(m.cfg.interval.Seconds()), capacityGBMin)

	return nil
}

// updateLimit updates an existing schedd startup limit
func (m *limitManager) updateLimit(ctx context.Context, existing *limitState, pair UserSitePair, rateCount int, capacityGBMin float64) error {
	name := fmt.Sprintf("pelican_%s_at_%s", sanitizeLimitLabel(pair.User), sanitizeLimitLabel(pair.Site))
	expression := fmt.Sprintf("(User == %q && TARGET.%s == %q)", pair.User, m.siteAttribute, pair.Site)

	// Refresh expiration time on every update
	expirationTime := time.Now().Add(m.cfg.expirationInactivity * 2).Unix()

	req := &htcondor.StartupLimitRequest{
		UUID:       existing.uuid,
		Tag:        m.limitTag(),
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

	m.logger.Printf("updated limit %s for user=%s site=%s: %d jobs/%ds (%.1f GB/min capacity)",
		uuid, pair.User, pair.Site, rateCount, int(m.cfg.interval.Seconds()), capacityGBMin)

	return nil
}

// removeStale removes limits that haven't been hit recently and are no longer in RED state
func (m *limitManager) removeStale(ctx context.Context, now time.Time, activeRedPairs map[UserSitePair]control.PairState) error {
	for pairTag, limit := range m.activeLimits {
		// Keep limit if still in RED state
		if _, inRed := activeRedPairs[limit.userSitePair]; inRed {
			continue
		}

		// Remove if not hit for expiration period
		if now.Sub(limit.lastHit) > m.cfg.expirationInactivity {
			// To remove a limit, we query and delete via the tag
			// The golang-htcondor API handles removal through the schedd
			// For now, just remove from our tracking (schedd will expire it)
			m.logger.Printf("removing stale limit %s for user=%s site=%s (last hit %v ago)",
				limit.uuid, limit.userSitePair.User, limit.userSitePair.Site, now.Sub(limit.lastHit))
			delete(m.activeLimits, pairTag)
		}
	}

	return nil
}

// refreshLimitStats queries the schedd to update lastHit times and statistics based on actual usage
func (m *limitManager) refreshLimitStats(ctx context.Context) error {
	for pairTag, limit := range m.activeLimits {
		limits, err := m.schedd.QueryStartupLimits(ctx, limit.uuid, "")
		if err != nil {
			m.logger.Printf("query limit %s error (skipping): %v", limit.uuid, err)
			continue
		}

		if len(limits) > 0 {
			limitInfo := limits[0]
			// Update lastHit if the limit was actually hit (jobs were skipped)
			if limitInfo.LastIgnored > 0 {
				newLastHit := time.Unix(limitInfo.LastIgnored, 0)
				if newLastHit.After(limit.lastHit) {
					limit.lastHit = newLastHit
					limit.hitCount++
					limit.jobsSkipped = int64(limitInfo.JobsSkipped)
					m.logger.Printf("limit %s for user=%s site=%s was hit at %v (total skipped=%d)",
						limit.uuid, limit.userSitePair.User, limit.userSitePair.Site,
						limit.lastHit, limit.jobsSkipped)
				}
			}
			m.activeLimits[pairTag] = limit
		}
	}

	return nil
}

// limitTag returns the static tag used for all limits managed by this daemon
func (m *limitManager) limitTag() string {
	return m.daemonName
}

// pairKey generates a stable hash for a user+site pair to use as map key
func pairKey(pair UserSitePair) string {
	h := sha256.New()
	h.Write([]byte(pair.User))
	h.Write([]byte{0})
	h.Write([]byte(pair.Site))
	sum := h.Sum(nil)
	return hex.EncodeToString(sum[:16])
}

// parseLimitExpression extracts user and site from a limit expression
// Expected format: (User == "user" && TARGET.<SiteAttribute> == "site")
func parseLimitExpression(expr string, siteAttribute string) (user, site string, ok bool) {
	// Simple parser for the expression format we generate
	// Look for User == "..." and TARGET.<SiteAttribute> == "..."
	var inQuotes bool
	var currentField string
	var currentValue string
	var fieldName string

	for i := 0; i < len(expr); i++ {
		ch := expr[i]

		if ch == '"' {
			if inQuotes {
				// End of quoted value
				if fieldName == "User" {
					user = currentValue
				} else if strings.HasSuffix(fieldName, "."+siteAttribute) || strings.HasSuffix(fieldName, siteAttribute) {
					site = currentValue
				}
				currentValue = ""
				fieldName = ""
				inQuotes = false
			} else {
				// Start of quoted value
				inQuotes = true
				fieldName = currentField
				currentField = ""
			}
		} else if inQuotes {
			currentValue += string(ch)
		} else if (ch >= 'a' && ch <= 'z') || (ch >= 'A' && ch <= 'Z') || (ch >= '0' && ch <= '9') || ch == '.' || ch == '_' {
			currentField += string(ch)
		} else {
			// Non-alphanumeric character, keep only if it could be part of a field name we care about
			if currentField != "" && currentField != "User" && !strings.Contains(currentField, siteAttribute) {
				currentField = ""
			}
		}
	}

	ok = user != "" && site != ""
	return
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

// getLimitInfo returns the active limit information for a user+site pair, if any
func (m *limitManager) getLimitInfo(pair UserSitePair) (rateCount int, rateWindow int, active bool) {
	if !m.cfg.enabled {
		return 0, 0, false
	}

	pairTag := pairKey(pair)
	if limit, exists := m.activeLimits[pairTag]; exists {
		return limit.rateCount, int(m.cfg.interval.Seconds()), true
	}

	return 0, int(m.cfg.interval.Seconds()), false
}

// getLimitStats returns statistics for a user+site pair limit
func (m *limitManager) getLimitStats(pair UserSitePair) (hitCount int64, jobsSkipped int64, lastHit time.Time, exists bool) {
	if !m.cfg.enabled {
		return 0, 0, time.Time{}, false
	}

	pairTag := pairKey(pair)
	if limit, ok := m.activeLimits[pairTag]; ok {
		return limit.hitCount, limit.jobsSkipped, limit.lastHit, true
	}

	return 0, 0, time.Time{}, false
}
