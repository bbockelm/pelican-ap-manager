package daemon

import (
	"context"
	"fmt"
	"math"
	"sync"
	"time"

	"github.com/PelicanPlatform/classad/ast"
	"github.com/PelicanPlatform/classad/parser"
	htcondor "github.com/bbockelm/golang-htcondor"
	htcondorlogging "github.com/bbockelm/golang-htcondor/logging"
	"github.com/bbockelm/pelican-ap-manager/internal/control"
	"github.com/bbockelm/pelican-ap-manager/internal/ratelimit"
)

const (
	// defaultLimitInterval is the rate window for schedd limits (aligned with negotiation cycle)
	defaultLimitInterval = 60 * time.Second

	// defaultLimitLease is how long a limit the daemon installs stays alive
	// without being renewed.
	//
	// The point of a short lease is that this daemon is the only thing keeping
	// its limits in force: if it crashes, whatever it was throttling must go
	// back to running at full rate within about a minute rather than staying
	// throttled until someone notices. The daemon renews at a third of this
	// interval, so the lease only ever runs out when the daemon is gone.
	//
	// The schedd caps this at STARTUP_LIMIT_MAX_EXPIRATION (5 minutes by
	// default) and silently clamps anything larger, so asking for more than
	// that achieves nothing without also raising the schedd's knob.
	defaultLimitLease = 60 * time.Second

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

// limitManager owns the schedd startup limits this daemon installs.
//
// It is driven by rules (internal/ratelimit), not by control state directly:
// the caller decides which rules should exist -- the operator's static policy
// plus, when enforcing, the control loop's dynamic conclusions -- and this
// reconciles the schedd against that set. Keeping the decision and the
// installation apart is what lets observing mode withhold the dynamic rules
// without the schedd-facing code knowing anything about modes.
// scheddLimits is the slice of the schedd this manager uses. It exists so the
// reconcile/renew/expire logic can be tested without a running HTCondor -- the
// absence of which is how a lease that was never renewed, and an expiration
// sent in the wrong units, both went unnoticed.
type scheddLimits interface {
	CreateStartupLimit(ctx context.Context, req *htcondor.StartupLimitRequest) (string, error)
	QueryStartupLimits(ctx context.Context, uuid, tag string) ([]*htcondor.StartupLimit, error)
	// Name is here for the log lines: a failure to talk to the schedd is only
	// actionable if it says which schedd.
	Name() string
}

type limitManager struct {
	schedd scheddLimits
	logger *htcondorlogging.Logger

	// mu guards activeLimits and cfg. Lease renewal runs on its own timer,
	// independent of the advertise cycle that reconciles policy, so the two can
	// reach this map at once.
	mu           sync.Mutex
	activeLimits map[string]*limitState // keyed by the schedd limit name (rule.LimitName())
	cfg          limitConfig

	daemonName    string
	siteAttribute string
}

type limitConfig struct {
	interval             time.Duration
	expirationInactivity time.Duration
	lease                time.Duration
	ewmaAlpha            float64
	enabled              bool
}

type limitState struct {
	uuid        string
	rule        ratelimit.Rule
	lastHit     time.Time
	lastUpdated time.Time
	rateCount   int
	rateWindow  time.Duration
	hitCount    int64
	jobsSkipped int64
}

// newLimitManager creates a limit manager for the schedd
func newLimitManager(schedd scheddLimits, daemonName string, siteAttribute string, logger *htcondorlogging.Logger) *limitManager {
	m := &limitManager{
		schedd:        schedd,
		logger:        logger,
		activeLimits:  make(map[string]*limitState),
		daemonName:    daemonName,
		siteAttribute: siteAttribute,
		cfg: limitConfig{
			interval:             defaultLimitInterval,
			expirationInactivity: limitExpirationInactivity,
			lease:                defaultLimitLease,
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
		// Not necessarily a schedd too old for startup limits: an unreachable
		// schedd, or one this daemon cannot authenticate to, lands here too. Say
		// which schedd, and do not assert the cause.
		logger.Infof(htcondorlogging.DestinationGeneral,
			"cannot read startup limits from schedd %s; rate limiting disabled (schedd too old, unreachable, or not authorized): %v",
			schedd.Name(), err)
		m.cfg.enabled = false
		return m
	}

	// Re-adopt limits this daemon (or a previous incarnation of it) installed.
	// The limit's Name is derived from the rule name, so it is the key that
	// survives a restart; the expression is parsed only to describe what was
	// adopted, since reconcile rewrites it from the rule anyway.
	for _, limitInfo := range limits {
		if limitInfo.Name == "" {
			logger.Infof(htcondorlogging.DestinationGeneral, "skipping unnamed limit %s during re-adoption", limitInfo.UUID)
			continue
		}

		lastHit := time.Now()
		if limitInfo.LastIgnored > 0 {
			lastHit = time.Unix(limitInfo.LastIgnored, 0)
		}

		window := time.Duration(limitInfo.RateWindow) * time.Second
		if window <= 0 {
			window = m.cfg.interval
		}

		m.activeLimits[limitInfo.Name] = &limitState{
			uuid:        limitInfo.UUID,
			rule:        ratelimit.Rule{Name: limitInfo.Name, Expression: limitInfo.Expression, RateCount: limitInfo.RateCount, RateWindow: window},
			lastHit:     lastHit,
			lastUpdated: time.Now(),
			rateCount:   limitInfo.RateCount,
			rateWindow:  window,
		}

		if user, site, ok := parseLimitExpression(limitInfo.Expression, siteAttribute); ok {
			logger.Infof(htcondorlogging.DestinationGeneral, "re-adopted limit %s (%s) for user=%s site=%s: %d jobs/%ds",
				limitInfo.UUID, limitInfo.Name, user, site, limitInfo.RateCount, limitInfo.RateWindow)
		} else {
			logger.Infof(htcondorlogging.DestinationGeneral, "re-adopted limit %s (%s): %d jobs/%ds (expression %q)",
				limitInfo.UUID, limitInfo.Name, limitInfo.RateCount, limitInfo.RateWindow, limitInfo.Expression)
		}
	}

	if len(m.activeLimits) > 0 {
		logger.Infof(htcondorlogging.DestinationGeneral, "re-adopted %d existing limits from schedd", len(m.activeLimits))
	}

	return m
}

// reconcile makes the schedd's startup limits match the given rule set: it
// creates limits that are missing, updates those whose rate has moved
// materially, and drops those whose rule is gone and which have not been hit
// recently.
//
// The rules passed in are already filtered for the enforcement mode -- an
// observing daemon simply hands over fewer of them.
func (m *limitManager) reconcile(ctx context.Context, rules []ratelimit.Rule) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	if !m.cfg.enabled {
		return nil
	}

	now := time.Now()
	desired := make(map[string]ratelimit.Rule, len(rules))

	for _, rule := range rules {
		key := rule.LimitName()
		desired[key] = rule

		existing, exists := m.activeLimits[key]
		if !exists {
			if err := m.createLimit(ctx, rule); err != nil {
				m.logger.Infof(htcondorlogging.DestinationGeneral, "limit create error for rule %s: %v", rule.Name, err)
			}
			continue
		}

		// Rewrite on every cycle, whether or not the rate moved. This also
		// renews the lease, but it is not what keeps the limits alive -- see
		// renewAll, which runs on its own timer. Rewriting here is what applies
		// a rate the controller has just changed.
		//
		// shouldUpdateLimit no longer decides whether to talk to the schedd --
		// only whether the rate has moved enough to be worth reporting.
		changed := m.shouldUpdateLimit(existing, rule)
		if err := m.renewLimit(ctx, existing, rule, changed); err != nil {
			m.logger.Infof(htcondorlogging.DestinationGeneral, "limit renew error for rule %s: %v", rule.Name, err)
		}
	}

	if err := m.removeStale(ctx, now, desired); err != nil {
		m.logger.Infof(htcondorlogging.DestinationGeneral, "limit cleanup error: %v", err)
	}

	if err := m.refreshLimitStats(ctx); err != nil {
		m.logger.Infof(htcondorlogging.DestinationGeneral, "limit refresh error: %v", err)
	}

	return nil
}

// dynamicRuleName is the stable rule name for the control loop's own limit on a
// (user, site) pair. It has to be deterministic: it is how a limit installed by
// a previous incarnation of the daemon is recognized as this pair's limit
// rather than orphaned and re-created.
func dynamicRuleName(pair UserSitePair) string {
	return sanitizeLimitLabel(pair.User) + "_at_" + sanitizeLimitLabel(pair.Site)
}

// dynamicRule builds the control loop's rule for a (user, site) pair from the
// capacity the AIMD controller settled on.
func (m *limitManager) dynamicRule(pair UserSitePair, state control.PairState, cfg control.Config, sources []string) ratelimit.Rule {
	return ratelimit.Rule{
		Name:       dynamicRuleName(pair),
		Origin:     ratelimit.OriginDynamic,
		User:       pair.User,
		Site:       pair.Site,
		Sources:    sources,
		RateCount:  m.calculateRateCountFromCapacity(state.CapacityGBPerMin, cfg),
		RateWindow: m.cfg.interval,
		Note: fmt.Sprintf("control loop: %.1f GB/min capacity for %s at %s",
			state.CapacityGBPerMin, pair.User, pair.Site),
		UpdatedAt: time.Now(),
	}
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

// shouldUpdateLimit reports whether an installed limit has drifted from its
// rule enough to be worth a schedd round trip.
//
// A static rule is compared exactly: the operator wrote a number and expects
// that number, so any change is worth pushing. A dynamic rule is compared with
// a 20% deadband, because the AIMD controller moves its capacity on every cycle
// and rewriting the limit each time would be pure churn.
func (m *limitManager) shouldUpdateLimit(existing *limitState, rule ratelimit.Rule) bool {
	if existing.rateWindow != rule.Window() {
		return true
	}
	// The expression can move independently of the rate (a rule's source list
	// grows as new origins are observed), so it has to be compared too.
	if existing.rule.ClassAdExpression(m.siteAttribute) != rule.ClassAdExpression(m.siteAttribute) {
		return true
	}
	if rule.Origin == ratelimit.OriginStatic {
		return existing.rateCount != rule.RateCount
	}
	if existing.rateCount == 0 {
		return rule.RateCount != 0
	}
	rateDiff := math.Abs(float64(rule.RateCount-existing.rateCount)) / float64(existing.rateCount)
	return rateDiff > 0.2
}

// createLimit installs a new schedd startup limit for a rule.
func (m *limitManager) createLimit(ctx context.Context, rule ratelimit.Rule) error {
	uuid, err := m.pushLimit(ctx, "", rule)
	if err != nil {
		return fmt.Errorf("create startup limit: %w", err)
	}

	m.activeLimits[rule.LimitName()] = &limitState{
		uuid:        uuid,
		rule:        rule,
		lastHit:     time.Now(),
		lastUpdated: time.Now(),
		rateCount:   rule.RateCount,
		rateWindow:  rule.Window(),
	}

	m.logger.Infof(htcondorlogging.DestinationGeneral, "created %s limit %s (%s): %d jobs/%s",
		rule.Origin, uuid, rule.Name, rule.RateCount, rule.Window())
	return nil
}

// renewLimit extends an installed limit's lease, rewriting its rate at the same
// time. changed says whether the rate moved enough to be worth a log line --
// renewal itself happens every cycle and would otherwise fill the log.
func (m *limitManager) renewLimit(ctx context.Context, existing *limitState, rule ratelimit.Rule, changed bool) error {
	uuid, err := m.pushLimit(ctx, existing.uuid, rule)
	if err != nil {
		return fmt.Errorf("renew startup limit: %w", err)
	}

	existing.uuid = uuid
	existing.rule = rule
	existing.rateCount = rule.RateCount
	existing.rateWindow = rule.Window()
	existing.lastUpdated = time.Now()

	if changed {
		m.logger.Infof(htcondorlogging.DestinationGeneral, "updated %s limit %s (%s): %d jobs/%s",
			rule.Origin, uuid, rule.Name, rule.RateCount, rule.Window())
	}
	return nil
}

// renewalInterval is how often renewAll has to run.
//
// A third of the lease, so two consecutive failures -- a schedd restart, a
// dropped connection -- still leave a third of the lease to recover in. Tying
// renewal to the lease rather than to a poll or advertise interval is what keeps
// the two independent: shortening the lease shortens the renewal to match, and
// no combination of the other intervals can produce a limit that lapses on a
// healthy daemon.
func (m *limitManager) renewalInterval() time.Duration {
	lease := m.cfg.lease
	if lease <= 0 {
		lease = defaultLimitLease
	}
	if iv := lease / 3; iv >= time.Second {
		return iv
	}
	return time.Second
}

// renewAll refreshes the lease on every limit currently installed, without
// reconsidering policy.
//
// Renewal is a liveness signal, not a decision: it says "this daemon is still
// here", and the limits it has already decided on stay in force until a
// reconcile decides otherwise. Keeping it separate is what lets it run on a
// timer derived from the lease instead of from whatever interval the policy
// cycle happens to use -- which is how limits at the default settings used to
// lapse, the advertise interval and the lease both being one minute.
func (m *limitManager) renewAll(ctx context.Context) {
	m.mu.Lock()
	defer m.mu.Unlock()

	if !m.cfg.enabled {
		return
	}
	for _, limit := range m.activeLimits {
		// changed=false: nothing about the rule moved, so this is silent.
		if err := m.renewLimit(ctx, limit, limit.rule, false); err != nil {
			m.logger.Infof(htcondorlogging.DestinationGeneral,
				"limit lease renewal error for rule %s: %v", limit.rule.Name, err)
		}
	}
}

// pushLimit sends one create-or-update to the schedd. uuid empty means create.
func (m *limitManager) pushLimit(ctx context.Context, uuid string, rule ratelimit.Rule) (string, error) {
	req := &htcondor.StartupLimitRequest{
		UUID:       uuid,
		Tag:        m.limitTag(),
		Name:       rule.LimitName(),
		Expression: rule.ClassAdExpression(m.siteAttribute),
		RateCount:  rule.RateCount,
		RateWindow: int(rule.Window().Seconds()),
		Expiration: int(m.leaseFor(rule, time.Now()).Seconds()),
	}
	return m.schedd.CreateStartupLimit(ctx, req)
}

// leaseFor is how long the schedd should keep this limit without hearing from
// us again.
//
// StartupLimitRequest.Expiration is *seconds from now*, not an absolute time.
// Sending a Unix timestamp here -- as this once did -- asks for a lease roughly
// 56 years long, which the schedd silently clamps to
// STARTUP_LIMIT_MAX_EXPIRATION. The bug was invisible because the clamp made
// every limit come out at the schedd's maximum instead of the intended value.
//
// A rule with its own deadline never gets a lease that outlives it, so a lapsed
// rule cannot keep throttling jobs just because the daemon died at the wrong
// moment.
func (m *limitManager) leaseFor(rule ratelimit.Rule, now time.Time) time.Duration {
	lease := m.cfg.lease
	if lease <= 0 {
		lease = defaultLimitLease
	}
	if !rule.ExpiresAt.IsZero() {
		if until := rule.ExpiresAt.Sub(now); until < lease {
			lease = until
		}
	}
	// The schedd rejects an expiration that is not in the future; one second is
	// the shortest lease it will accept.
	if lease < time.Second {
		lease = time.Second
	}
	return lease
}

// removeStale drops limits whose rule is no longer desired and which have not
// been hit for the inactivity window. The delay matters: a pair can drop out of
// RED for one poll and come straight back, and tearing the limit down and
// rebuilding it would reset the schedd's token bucket each time.
func (m *limitManager) removeStale(ctx context.Context, now time.Time, desired map[string]ratelimit.Rule) error {
	for key, limit := range m.activeLimits {
		if _, wanted := desired[key]; wanted {
			continue
		}
		if now.Sub(limit.lastHit) <= m.cfg.expirationInactivity {
			continue
		}
		// The schedd expires limits on its own (see pushLimit), so dropping our
		// tracking is enough to stop refreshing it; it lapses shortly after.
		m.logger.Infof(htcondorlogging.DestinationGeneral, "releasing stale limit %s (%s), last hit %v ago",
			limit.uuid, key, now.Sub(limit.lastHit).Truncate(time.Second))
		delete(m.activeLimits, key)
	}
	return nil
}

// refreshLimitStats queries the schedd to update lastHit times and statistics
// based on actual usage, and notices limits that are no longer there.
func (m *limitManager) refreshLimitStats(ctx context.Context) error {
	for key, limit := range m.activeLimits {
		limits, err := m.schedd.QueryStartupLimits(ctx, limit.uuid, "")
		if err != nil {
			m.logger.Infof(htcondorlogging.DestinationGeneral, "query limit %s error (skipping): %v", limit.uuid, err)
			continue
		}

		if len(limits) == 0 {
			// The limit is gone from the schedd although we still want it --
			// its lease ran out while we were not looking, or a schedd restart
			// dropped it. Forget it so the next cycle installs it again;
			// without this the daemon believes it is still enforcing something
			// that no longer exists.
			m.logger.Infof(htcondorlogging.DestinationGeneral,
				"limit %s (%s) is no longer in the schedd; reinstalling on the next cycle", limit.uuid, key)
			delete(m.activeLimits, key)
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
					limit.jobsSkipped = limitInfo.JobsSkipped
					m.logger.Infof(htcondorlogging.DestinationGeneral, "limit %s (%s) was hit at %v (total skipped=%d)",
						limit.uuid, key, limit.lastHit, limit.jobsSkipped)
				}
			}
			m.activeLimits[key] = limit
		}
	}

	return nil
}

// limitTag returns the static tag used for all limits managed by this daemon
func (m *limitManager) limitTag() string {
	return m.daemonName
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
	case *ast.ParenExpr:
		// The parser preserves explicit parentheses as their own node, and
		// buildLimitExpression wraps both the whole expression and the source
		// disjunction in them, so descend through.
		return walkExprForUserAndSite(n.Inner, siteAttribute)

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

// getLimitInfo returns the control loop's own limit for a user+site pair, if
// one is installed. A static rule that happens to cover the same pair is not
// reported here: these accessors feed the per-pair fields of the summary ads,
// which describe what the controller decided, not the whole policy in force.
func (m *limitManager) getLimitInfo(pair UserSitePair) (rateCount int, rateWindow int, active bool) {
	m.mu.Lock()
	defer m.mu.Unlock()

	if !m.cfg.enabled {
		return 0, 0, false
	}

	if limit, ok := m.dynamicLimit(pair); ok {
		return limit.rateCount, int(limit.rateWindow.Seconds()), true
	}
	return 0, int(m.cfg.interval.Seconds()), false
}

// getLimitStats returns statistics for a user+site pair limit
func (m *limitManager) getLimitStats(pair UserSitePair) (hitCount int64, jobsSkipped int64, lastHit time.Time, exists bool) {
	m.mu.Lock()
	defer m.mu.Unlock()

	if !m.cfg.enabled {
		return 0, 0, time.Time{}, false
	}

	if limit, ok := m.dynamicLimit(pair); ok {
		return limit.hitCount, limit.jobsSkipped, limit.lastHit, true
	}
	return 0, 0, time.Time{}, false
}

// getLimitUUID returns the UUID of the active limit for a user+site pair
func (m *limitManager) getLimitUUID(pair UserSitePair) (uuid string, exists bool) {
	m.mu.Lock()
	defer m.mu.Unlock()

	if !m.cfg.enabled {
		return "", false
	}

	if limit, ok := m.dynamicLimit(pair); ok {
		return limit.uuid, true
	}
	return "", false
}

// dynamicLimit finds the installed limit for a pair's control-loop rule.
func (m *limitManager) dynamicLimit(pair UserSitePair) (*limitState, bool) {
	key := ratelimit.Rule{Name: dynamicRuleName(pair), Origin: ratelimit.OriginDynamic}.LimitName()
	limit, ok := m.activeLimits[key]
	return limit, ok
}
