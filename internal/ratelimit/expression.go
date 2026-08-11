package ratelimit

import (
	"fmt"
	"strings"
)

// ClassAdExpression renders the rule's selector as the ClassAd expression the
// schedd evaluates for each start decision, or returns the operator's raw
// Expression verbatim when one is set.
//
// siteAttribute names the machine attribute carrying the execution site (the
// PELICAN_MANAGER_SITE_ATTRIBUTE macro); it varies by pool, so it is a
// parameter rather than a constant.
//
// A wildcard (or empty) selector contributes no clause, so a rule scoped only
// by user matches that user everywhere. A rule with no clauses at all matches
// every job; Validate rejects that unless the operator asked for it explicitly.
func (r Rule) ClassAdExpression(siteAttribute string) string {
	if r.Expression != "" {
		return r.Expression
	}

	var clauses []string
	if isSelector(r.User) {
		clauses = append(clauses, fmt.Sprintf("JOB.Owner =?= %q", r.User))
	}
	if len(r.Sources) > 0 {
		conditions := make([]string, len(r.Sources))
		for i, source := range r.Sources {
			conditions[i] = fmt.Sprintf("stringListMember(%q, JOB.PelicanInputPrefixes ?: \"\")", source)
		}
		if len(conditions) == 1 {
			clauses = append(clauses, conditions[0])
		} else {
			clauses = append(clauses, "("+strings.Join(conditions, " || ")+")")
		}
	}
	if isSelector(r.Site) && siteAttribute != "" {
		clauses = append(clauses, fmt.Sprintf("TARGET.%s =?= %q", siteAttribute, r.Site))
	}

	if len(clauses) == 0 {
		// Deliberate match-everything (all selectors were wildcards).
		return "true"
	}
	return "(" + strings.Join(clauses, " && ") + ")"
}

// isSelector reports whether a selector field constrains anything (i.e. is
// neither empty nor the wildcard).
func isSelector(v string) bool {
	return v != "" && v != Wildcard
}

// LimitName renders the schedd startup-limit display name for a rule. The
// origin is part of the name so an operator reading `condor_q`-adjacent limit
// listings can tell hand-written policy from control-loop output.
func (r Rule) LimitName() string {
	return "pelican_" + string(r.Origin) + "_" + sanitizeLabel(r.Name)
}

// sanitizeLabel reduces a name to characters safe in a limit label.
func sanitizeLabel(s string) string {
	if len(s) > 40 {
		s = s[:40]
	}
	var b strings.Builder
	for _, r := range s {
		if (r >= 'a' && r <= 'z') || (r >= 'A' && r <= 'Z') || (r >= '0' && r <= '9') || r == '_' {
			b.WriteRune(r)
		} else {
			b.WriteRune('_')
		}
	}
	return b.String()
}
