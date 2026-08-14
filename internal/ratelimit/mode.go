package ratelimit

import (
	"fmt"
	"strings"
	"time"
)

// Mode is the daemon's enforcement posture: what it does with the rules it has
// decided on.
type Mode string

const (
	// ModeEnforcing installs every active rule -- both the operator's static
	// policy and the control loop's dynamic conclusions -- as schedd startup
	// limits.
	ModeEnforcing Mode = "enforcing"

	// ModeObserving installs only static rules. The control loop still runs,
	// still classifies (user, site) pairs, and still publishes what it would
	// have done in the summary ads; it simply does not act on it. This is how
	// an operator evaluates the controller against a real workload without
	// handing it the throttle, while keeping a few hand-written limits in
	// force.
	ModeObserving Mode = "observing"
)

// ParseMode reads a Mode from configuration, accepting a few spellings an
// operator is likely to reach for.
func ParseMode(raw string) (Mode, error) {
	switch strings.ToLower(strings.TrimSpace(raw)) {
	case "enforcing", "enforce", "active":
		return ModeEnforcing, nil
	case "observing", "observe", "monitor", "monitoring", "dryrun", "dry-run":
		return ModeObserving, nil
	default:
		return "", fmt.Errorf("unknown enforcement mode %q (want %q or %q)", raw, ModeEnforcing, ModeObserving)
	}
}

// Enforces reports whether the mode installs rules of the given origin. Static
// rules are the operator's own policy, so they hold in every mode; dynamic
// rules are the controller's output and are withheld while observing.
func (m Mode) Enforces(o Origin) bool {
	if o == OriginStatic {
		return true
	}
	return m == ModeEnforcing
}

// Installable filters a rule set down to what should be installed as schedd
// startup limits at time now under this mode: enabled, unexpired, and of an
// origin the mode enforces. The result is sorted by name so reconciliation and
// logging are stable across polls.
func (m Mode) Installable(rules []Rule, now time.Time) []Rule {
	out := make([]Rule, 0, len(rules))
	for _, r := range rules {
		if !r.Active(now) || !m.Enforces(r.Origin) {
			continue
		}
		out = append(out, r)
	}
	SortRules(out)
	return out
}
