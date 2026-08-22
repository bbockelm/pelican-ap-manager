// Package store persists the pelican-man state that must survive a restart.
//
// Historically all of it lived in one JSON blob under SPOOL. That is fine for
// a single daemon's private scratch, but rate rules are different: an operator
// writes them, wants to read them back, and expects them to be the same rules
// after a restart or a failover. So rules get a real store with two backends:
//
//   - File: a JSON document beside the legacy state file. The default; needs
//     nothing beyond a writable SPOOL.
//   - DB: a table in an htcondordb daemon, reached over an authenticated CEDAR
//     dbrpc session. Use it when the rules should be visible to (and editable
//     from) outside this daemon, or shared across a pair of APs.
//
// Only the rule set is covered here. The transfer summaries, epoch cursor and
// control state still live in internal/state; moving those over is the next
// step, and this interface is the seam they will move through.
package store

import (
	"context"

	"github.com/bbockelm/pelican-ap-manager/internal/ratelimit"
)

// RuleStore persists rate rules.
//
// Implementations must be safe for concurrent use: the poll loop reads the rule
// set on every cycle while a reconfigure may be rewriting the config-declared
// rules.
type RuleStore interface {
	// ListRules returns every stored rule, including disabled and expired ones,
	// sorted by name. Filtering is the caller's job -- an operator listing
	// rules wants to see the ones that are parked or lapsed.
	ListRules(ctx context.Context) ([]ratelimit.Rule, error)

	// PutRule writes a rule, replacing any existing rule with the same name.
	PutRule(ctx context.Context, rule ratelimit.Rule) error

	// DeleteRule removes a rule by name. Removing a rule that does not exist is
	// not an error: reconciliation deletes optimistically.
	DeleteRule(ctx context.Context, name string) error

	// Close releases the store's resources.
	Close() error
}
