package store

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/bbockelm/pelican-ap-manager/internal/ratelimit"
)

// SyncConfigRules makes the config-declared rules in the store match the
// configuration exactly: it writes every declared rule and deletes the
// config-managed rules that are no longer declared.
//
// Rules written directly to the store (ConfigManaged false) are left alone --
// including dynamic rules the control loop persisted -- so an operator can mix
// configuration-driven policy with rules created out of band without one
// clobbering the other.
//
// Called at startup and on every reconfigure, so removing a rule from the
// configuration and reconfiguring is enough to retire it. Errors are collected
// rather than returned on the first failure: one unwritable rule should not
// prevent the rest of the policy from landing.
func SyncConfigRules(ctx context.Context, rs RuleStore, declared []ratelimit.Rule) error {
	if rs == nil {
		return nil
	}

	declaredByName := make(map[string]ratelimit.Rule, len(declared))
	for _, r := range declared {
		declaredByName[r.Name] = r
	}

	var errs []error

	existing, err := rs.ListRules(ctx)
	if err != nil {
		// Without the current contents we cannot tell which config-managed
		// rules to retire, but we can still write the declared ones -- and
		// writing them is the half that matters for policy being in force.
		errs = append(errs, fmt.Errorf("listing existing rules: %w", err))
	} else {
		for _, r := range existing {
			if !r.ConfigManaged {
				continue
			}
			if _, still := declaredByName[r.Name]; still {
				continue
			}
			if derr := rs.DeleteRule(ctx, r.Name); derr != nil {
				errs = append(errs, fmt.Errorf("retiring rule %q: %w", r.Name, derr))
			}
		}
	}

	now := time.Now()
	for _, r := range declared {
		r.ConfigManaged = true
		r.Origin = ratelimit.OriginStatic
		r.UpdatedAt = now
		if perr := rs.PutRule(ctx, r); perr != nil {
			errs = append(errs, fmt.Errorf("writing rule %q: %w", r.Name, perr))
		}
	}

	return errors.Join(errs...)
}
