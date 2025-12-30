package daemon

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"math"
	"strings"
	"time"

	"github.com/PelicanPlatform/classad/ast"
	"github.com/PelicanPlatform/classad/parser"
	htcondor "github.com/bbockelm/golang-htcondor"
	htcondorlogging "github.com/bbockelm/golang-htcondor/logging"
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
	logger        *htcondorlogging.Logger
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
func newLimitManager(schedd *htcondor.Schedd, daemonName string, siteAttribute string, logger *htcondorlogging.Logger) *limitManager {
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
		logger.Infof(htcondorlogging.DestinationGeneral, "schedd does not support startup limits (disabling limit manager): %v", err)
		m.cfg.enabled = false
		return m
	}

	// Re-adopt existing limits
	for _, limitInfo := range limits {
		user, site, ok := parseLimitExpression(limitInfo.Expression, siteAttribute)
		if !ok {
			logger.Infof(htcondorlogging.DestinationGeneral, "skipping limit %s: could not parse expression %q", limitInfo.UUID, limitInfo.Expression)
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

		logger.Infof(htcondorlogging.DestinationGeneral, "re-adopted limit %s for user=%s site=%s: %d jobs/%ds",
			limitInfo.UUID, user, site, limitInfo.RateCount, limitInfo.RateWindow)
	}

	if len(m.activeLimits) > 0 {
		logger.Infof(htcondorlogging.DestinationGeneral, "re-adopted %d existing limits from schedd", len(m.activeLimits))
	}

	return m
}

// updateLimits synchronizes schedd limits based on user+site pairs and their states
// Only creates/updates limits when a pair is in RED state; removes stale limits
func (m *limitManager) updateLimits(ctx context.Context, userSitePairs map[UserSitePair]control.PairState, tracker *stats.Tracker, controlCfg control.Config, windowMetrics map[UserSitePair]outcomeMetrics) error {
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

		// Extract sources from window metrics
		sources := []string{}
		if wm, ok := windowMetrics[pair]; ok {
			sources = wm.sources
		}

		// For now, use a default rate count based on capacity
		// TODO: Improve rate count calculation with actual sandbox/transfer metrics
		rateCount := m.calculateRateCountFromCapacity(state.CapacityGBPerMin, controlCfg)

		if exists {
			// Update existing limit if rate changed significantly
			if m.shouldUpdateLimit(existing, rateCount, state.CapacityGBPerMin) {
				if err := m.updateLimit(ctx, existing, pair, rateCount, state.CapacityGBPerMin, sources); err != nil {
					m.logger.Infof(htcondorlogging.DestinationGeneral, "limit update error for user=%s site=%s: %v", pair.User, pair.Site, err)
				}
			}
		} else {
			// Create new limit
			if err := m.createLimit(ctx, pair, rateCount, state.CapacityGBPerMin, sources); err != nil {
				m.logger.Infof(htcondorlogging.DestinationGeneral, "limit create error for user=%s site=%s: %v", pair.User, pair.Site, err)
			}
		}
	}

	// Remove stale limits (not hit recently and no longer in RED state)
	if err := m.removeStale(ctx, now, pairsNeedingLimits); err != nil {
		m.logger.Infof(htcondorlogging.DestinationGeneral, "limit cleanup error: %v", err)
	}

	// Query current limits to update lastHit times and statistics
	if err := m.refreshLimitStats(ctx); err != nil {
		m.logger.Infof(htcondorlogging.DestinationGeneral, "limit refresh error: %v", err)
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
func (m *limitManager) createLimit(ctx context.Context, pair UserSitePair, rateCount int, capacityGBMin float64, sources []string) error {
	tag := m.limitTag()
	name := fmt.Sprintf("pelican_%s_at_%s", sanitizeLimitLabel(pair.User), sanitizeLimitLabel(pair.Site))

	// Build ClassAd expression to match jobs with this user at this site
	// The expression matches on:
	// - JOB.Owner (user)
	// - PelicanInputPrefixes (source URL prefixes)
	// - MACHINE.GLIDEIN_Site (execution site)
	expression := m.buildLimitExpression(pair.User, pair.Site, sources)

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

	m.logger.Infof(htcondorlogging.DestinationGeneral, "created limit %s for user=%s site=%s: %d jobs/%ds (%.1f GB/min capacity)",
		uuid, pair.User, pair.Site, rateCount, int(m.cfg.interval.Seconds()), capacityGBMin)

	return nil
}

// updateLimit updates an existing schedd startup limit
func (m *limitManager) updateLimit(ctx context.Context, existing *limitState, pair UserSitePair, rateCount int, capacityGBMin float64, sources []string) error {
	name := fmt.Sprintf("pelican_%s_at_%s", sanitizeLimitLabel(pair.User), sanitizeLimitLabel(pair.Site))
	expression := m.buildLimitExpression(pair.User, pair.Site, sources)

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

	m.logger.Infof(htcondorlogging.DestinationGeneral, "updated limit %s for user=%s site=%s: %d jobs/%ds (%.1f GB/min capacity)",
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
			m.logger.Infof(htcondorlogging.DestinationGeneral, "removing stale limit %s for user=%s site=%s (last hit %v ago)",
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
			m.logger.Infof(htcondorlogging.DestinationGeneral, "query limit %s error (skipping): %v", limit.uuid, err)
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
					m.logger.Infof(htcondorlogging.DestinationGeneral, "limit %s for user=%s site=%s was hit at %v (total skipped=%d)",
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

// parseLimitExpression extracts user and site from a limit expression using ClassAd AST parsing
// Handles both old format: (User == "user" && TARGET.<SiteAttribute> == "site")
// And new format: (JOB.Owner =?= "user" && stringListMember(...) && TARGET.<SiteAttribute> =?= "site")
func parseLimitExpression(expr string, siteAttribute string) (user, site string, ok bool) {
	// Wrap the expression in a ClassAd format so the parser accepts it
	// parser.Parse only works with full ClassAd documents
	wrappedExpr := fmt.Sprintf("[ tmp = %s ]", expr)

	// Parse as a ClassAd
	classAd, err := parser.ParseClassAd(wrappedExpr)
	if err != nil {
		return "", "", false
	}

	// Extract the 'tmp' attribute which contains our expression
	if len(classAd.Attributes) == 0 {
		return "", "", false
	}

	// Find the tmp attribute in the list
	var tmpExpr ast.Expr
	for _, attr := range classAd.Attributes {
		if attr.Name == "tmp" {
			tmpExpr = attr.Value
			break
		}
	}

	if tmpExpr == nil {
		return "", "", false
	}

	// Walk the AST to find user and site values
	user, site = walkExprForUserAndSite(tmpExpr, siteAttribute)
	ok = user != "" && site != ""
	return
}

// walkExprForUserAndSite recursively walks a ClassAd AST node to find user and site values
func walkExprForUserAndSite(node ast.Expr, siteAttribute string) (user, site string) {
	if node == nil {
		return "", ""
	}

	switch n := node.(type) {
	case *ast.BinaryOp:
		// Check if this is a comparison operator (==, =?=, or is)
		// Note: The AST represents =?= as "is"
		if n.Op == "==" || n.Op == "=?=" || n.Op == "is" {
			// Check if right side is a string literal
			if strLit, ok := n.Right.(*ast.StringLiteral); ok {
				// Check left side for different patterns

				// Pattern 1: Simple attribute reference (e.g., User)
				if attrRef, ok := n.Left.(*ast.AttributeReference); ok {
					// Match User (no scope) or Owner (MY scope)
					if (attrRef.Scope == ast.NoScope && attrRef.Name == "User") ||
						(attrRef.Scope == ast.MyScope && attrRef.Name == "Owner") {
						user = strLit.Value
					}
					// Match TARGET.<SiteAttribute>
					if attrRef.Scope == ast.TargetScope && attrRef.Name == siteAttribute {
						site = strLit.Value
					}
				}

				// Pattern 2: Select expression (e.g., JOB.Owner, TARGET.Site)
				if selectExpr, ok := n.Left.(*ast.SelectExpr); ok {
					// Check if Record is an AttributeReference
					if recordAttr, ok := selectExpr.Record.(*ast.AttributeReference); ok {
						// Match JOB.Owner
						if recordAttr.Name == "JOB" && selectExpr.Attr == "Owner" {
							user = strLit.Value
						}
						// Match TARGET.<SiteAttribute>
						if recordAttr.Name == "TARGET" && selectExpr.Attr == siteAttribute {
							site = strLit.Value
						}
					}
				}
			}
		}

		// Recursively search in logical operators (&& or ||)
		if n.Op == "&&" || n.Op == "||" {
			u1, s1 := walkExprForUserAndSite(n.Left, siteAttribute)
			u2, s2 := walkExprForUserAndSite(n.Right, siteAttribute)
			if user == "" && u1 != "" {
				user = u1
			}
			if user == "" && u2 != "" {
				user = u2
			}
			if site == "" && s1 != "" {
				site = s1
			}
			if site == "" && s2 != "" {
				site = s2
			}
		}

	case *ast.FunctionCall:
		// Recursively check function arguments in case there are nested conditions
		for _, arg := range n.Args {
			u, s := walkExprForUserAndSite(arg, siteAttribute)
			if user == "" && u != "" {
				user = u
			}
			if site == "" && s != "" {
				site = s
			}
		}

	case ast.Expr:
		// For other expression types, we don't need to handle them
		// but this makes the type switch exhaustive for the Expr interface
	}

	return user, site
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

// buildLimitExpression constructs a ClassAd expression that matches jobs with:
// - Specific user (JOB.Owner)
// - Input sources matching federation prefixes (using stringListMember)
// - Target machine's site
func (m *limitManager) buildLimitExpression(user, site string, sources []string) string {
	// Match on user
	userExpr := fmt.Sprintf("JOB.Owner =?= %q", user)

	// Match on machine site
	siteExpr := fmt.Sprintf("TARGET.%s =?= %q", m.siteAttribute, site)

	// Match on source prefixes if any are provided
	var sourceExpr string
	if len(sources) > 0 {
		// Build expression: any source in sources list matches any prefix in PelicanInputPrefixes
		// Use stringListMember(source, JOB.PelicanInputPrefixes ?: "")
		conditions := make([]string, len(sources))
		for i, source := range sources {
			conditions[i] = fmt.Sprintf("stringListMember(%q, JOB.PelicanInputPrefixes ?: \"\")", source)
		}
		// Combine with OR
		if len(conditions) == 1 {
			sourceExpr = conditions[0]
		} else {
			sourceExpr = "(" + strings.Join(conditions, " || ") + ")"
		}
		return fmt.Sprintf("(%s && %s && %s)", userExpr, sourceExpr, siteExpr)
	}

	// No source filtering, just user and site
	return fmt.Sprintf("(%s && %s)", userExpr, siteExpr)
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

// getLimitUUID returns the UUID of the active limit for a user+site pair
func (m *limitManager) getLimitUUID(pair UserSitePair) (uuid string, exists bool) {
	if !m.cfg.enabled {
		return "", false
	}

	pairTag := pairKey(pair)
	if limit, ok := m.activeLimits[pairTag]; ok {
		return limit.uuid, true
	}

	return "", false
}
