// Package ratelimit models the rate rules pelican-man installs as HTCondor
// schedd startup limits.
//
// A rule is a persisted, named limit on how fast jobs matching a (user, site,
// input-source) selector may start. Rules come from two places:
//
//   - Static rules, declared by the operator (configuration or an API write).
//     They express policy the operator wants applied unconditionally, so they
//     are installed in every enforcement mode -- including "observing", where
//     the control loop's own conclusions are computed and published but not
//     acted on.
//   - Dynamic rules, derived by the control loop from observed transfer
//     performance. They are installed only when the daemon is enforcing.
//
// The rule set is persisted (see internal/store) so a restart re-adopts the
// operator's policy without waiting for the control loop to re-derive anything,
// and so an operator can inspect and edit what is actually installed.
package ratelimit

import (
	"fmt"
	"sort"
	"strconv"
	"strings"
	"time"
)

// Origin distinguishes operator-declared policy from control-loop output. It
// decides whether a rule survives observing mode.
type Origin string

const (
	// OriginStatic marks a rule the operator declared. Applied in every mode.
	OriginStatic Origin = "static"
	// OriginDynamic marks a rule the control loop derived. Applied only when
	// the daemon is enforcing.
	OriginDynamic Origin = "dynamic"
)

// Wildcard matches any value in a rule selector. An empty selector field means
// the same thing.
const Wildcard = "*"

// Rule is one persisted rate rule.
type Rule struct {
	// Name is the rule's stable identity: the storage key, and the basis for
	// the schedd limit's display name. Operator-chosen for static rules,
	// derived from the (user,site) pair for dynamic ones.
	Name string `json:"name"`

	// Origin decides whether observing mode installs this rule.
	Origin Origin `json:"origin"`

	// User and Site select which jobs the rule covers. Empty or "*" matches
	// any. Sources, when non-empty, further narrows to jobs whose
	// PelicanInputPrefixes name one of the listed origins.
	User    string   `json:"user,omitempty"`
	Site    string   `json:"site,omitempty"`
	Sources []string `json:"sources,omitempty"`

	// Expression, when set, replaces the selector-derived ClassAd expression
	// verbatim. It is the escape hatch for policy the (user, site, sources)
	// triple cannot express. Operator-supplied and spliced in unmodified, so it
	// carries the same trust as the rest of the daemon's configuration.
	Expression string `json:"expression,omitempty"`

	// RateCount jobs may start per RateWindow. A RateCount of 0 installs a
	// monitor-only limit: the schedd counts matching starts without blocking
	// any, which is how a static rule can observe a selector before an operator
	// commits to a number.
	RateCount  int           `json:"rate_count"`
	RateWindow time.Duration `json:"rate_window"`

	// Disabled keeps a rule in the store without installing it, so an operator
	// can park policy instead of deleting it.
	Disabled bool `json:"disabled,omitempty"`

	// ExpiresAt drops the rule after a deadline. Zero means it never expires.
	ExpiresAt time.Time `json:"expires_at,omitempty"`

	// ConfigManaged marks a rule that came from the HTCondor configuration
	// (PELICAN_MANAGER_RATE_RULE_<NAME>) rather than from a direct write to the
	// store. The daemon owns these: it rewrites them on every reconfigure and
	// deletes the ones that have disappeared from the configuration, so the
	// config file stays the single source of truth for the rules it declares. A
	// rule written directly to the store is left alone.
	ConfigManaged bool `json:"config_managed,omitempty"`

	// Note is free-form operator commentary, carried through to the store and
	// the daemon's ads so the reason for a rule survives its author.
	Note string `json:"note,omitempty"`

	// UpdatedAt is when the rule was last written.
	UpdatedAt time.Time `json:"updated_at,omitempty"`
}

// DefaultRateWindow is the rate window used when a rule does not name one. It
// matches the schedd's negotiation cadence, which is the granularity at which
// start decisions actually happen.
const DefaultRateWindow = 60 * time.Second

// Window returns the rule's rate window, substituting the default when unset.
func (r Rule) Window() time.Duration {
	if r.RateWindow <= 0 {
		return DefaultRateWindow
	}
	return r.RateWindow
}

// Active reports whether the rule should be installed at time now: it must be
// enabled and unexpired.
func (r Rule) Active(now time.Time) bool {
	if r.Disabled {
		return false
	}
	return r.ExpiresAt.IsZero() || now.Before(r.ExpiresAt)
}

// Matches reports whether the rule's selector covers a (user, site) pair. A
// rule carrying a raw Expression matches nothing here -- its scope is whatever
// the expression says, which this package does not interpret -- so callers must
// not use Matches to decide whether such a rule shadows another.
func (r Rule) Matches(user, site string) bool {
	if r.Expression != "" {
		return false
	}
	return selectorMatches(r.User, user) && selectorMatches(r.Site, site)
}

func selectorMatches(selector, value string) bool {
	return selector == "" || selector == Wildcard || selector == value
}

// Validate reports why a rule cannot be installed, or nil.
func (r Rule) Validate() error {
	if strings.TrimSpace(r.Name) == "" {
		return fmt.Errorf("rule name is required")
	}
	if r.Origin != OriginStatic && r.Origin != OriginDynamic {
		return fmt.Errorf("rule %q: origin must be %q or %q, got %q", r.Name, OriginStatic, OriginDynamic, r.Origin)
	}
	if r.RateCount < 0 {
		return fmt.Errorf("rule %q: rate must not be negative", r.Name)
	}
	if r.RateWindow < 0 {
		return fmt.Errorf("rule %q: window must not be negative", r.Name)
	}
	// A rule with neither a selector nor an expression would match every job in
	// the queue. That is a plausible thing to want, but never by accident, so
	// require the operator to say so with an explicit wildcard.
	if r.Expression == "" && r.User == "" && r.Site == "" && len(r.Sources) == 0 {
		return fmt.Errorf("rule %q: needs at least one of user, site, sources, or expr "+
			"(use user=* or site=* to match everything deliberately)", r.Name)
	}
	return nil
}

// ParseRule parses one rule from an HTCondor configuration value of the form
//
//	user=alice site=UCSD rate=20 window=60s sources=osdf://ospool note="burst guard"
//
// Keys may appear in any order; unknown keys are an error rather than a silent
// no-op, because a typo in a rate limit should not quietly widen it. name is
// supplied by the caller (it comes from the macro name, not the value).
func ParseRule(name, spec string) (Rule, error) {
	r := Rule{Name: name, Origin: OriginStatic}

	fields, err := splitFields(spec)
	if err != nil {
		return Rule{}, fmt.Errorf("rule %q: %w", name, err)
	}
	for _, f := range fields {
		key, value, ok := strings.Cut(f, "=")
		if !ok {
			return Rule{}, fmt.Errorf("rule %q: expected key=value, got %q", name, f)
		}
		key = strings.ToLower(strings.TrimSpace(key))
		value = strings.TrimSpace(value)

		switch key {
		case "user", "owner":
			r.User = value
		case "site":
			r.Site = value
		case "sources", "source":
			for _, s := range strings.Split(value, ",") {
				if s = strings.TrimSpace(s); s != "" {
					r.Sources = append(r.Sources, s)
				}
			}
		case "expr", "expression":
			r.Expression = value
		case "rate":
			n, err := strconv.Atoi(value)
			if err != nil {
				return Rule{}, fmt.Errorf("rule %q: invalid rate %q: %w", name, value, err)
			}
			r.RateCount = n
		case "window":
			d, err := time.ParseDuration(value)
			if err != nil {
				return Rule{}, fmt.Errorf("rule %q: invalid window %q: %w", name, value, err)
			}
			r.RateWindow = d
		case "expires":
			d, err := time.ParseDuration(value)
			if err != nil {
				return Rule{}, fmt.Errorf("rule %q: invalid expires %q: %w", name, value, err)
			}
			r.ExpiresAt = time.Now().Add(d)
		case "disabled":
			b, err := strconv.ParseBool(value)
			if err != nil {
				return Rule{}, fmt.Errorf("rule %q: invalid disabled %q: %w", name, value, err)
			}
			r.Disabled = b
		case "note":
			r.Note = value
		default:
			return Rule{}, fmt.Errorf("rule %q: unknown key %q", name, key)
		}
	}

	if err := r.Validate(); err != nil {
		return Rule{}, err
	}
	return r, nil
}

// splitFields splits a rule spec on whitespace, keeping double-quoted runs
// together so a note (or an expression) can contain spaces.
func splitFields(spec string) ([]string, error) {
	var (
		fields  []string
		cur     strings.Builder
		inQuote bool
	)
	for _, r := range spec {
		switch {
		case r == '"':
			inQuote = !inQuote
		case !inQuote && (r == ' ' || r == '\t' || r == '\n' || r == '\r'):
			if cur.Len() > 0 {
				fields = append(fields, cur.String())
				cur.Reset()
			}
		default:
			cur.WriteRune(r)
		}
	}
	if inQuote {
		return nil, fmt.Errorf("unterminated quote in %q", spec)
	}
	if cur.Len() > 0 {
		fields = append(fields, cur.String())
	}
	return fields, nil
}

// SortRules orders rules by name so the daemon's reconciliation, logging, and
// published ads are stable across polls.
func SortRules(rules []Rule) {
	sort.Slice(rules, func(i, j int) bool { return rules[i].Name < rules[j].Name })
}
