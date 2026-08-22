package store

import (
	"context"
	"fmt"
	"strings"
	"sync"
	"time"

	"github.com/PelicanPlatform/classad/classad"
	"github.com/PelicanPlatform/classad/dbrpc"
	cedarclient "github.com/bbockelm/cedar/client"
	"github.com/bbockelm/cedar/security"
	htcondor "github.com/bbockelm/golang-htcondor"
	"github.com/bbockelm/golang-htcondor/config"
	"github.com/bbockelm/htcondordb/command"
	"github.com/bbockelm/pelican-ap-manager/internal/dbaddr"
	"github.com/bbockelm/pelican-ap-manager/internal/ratelimit"
)

// DefaultRuleTable is the htcondordb table holding pelican_man's rate rules.
const DefaultRuleTable = "pelican_rate_rules"

// DBStore keeps the rule set in an htcondordb table, reached over an
// authenticated CEDAR dbrpc session. One rule is one ClassAd, keyed by rule
// name.
//
// Why a remote table rather than a local file: the rules are policy, not
// scratch. Putting them in the pool's database makes them readable by the same
// tooling that reads everything else (htcondordb-cli, the Grafana datasource,
// the MCP server), editable without shelling into the AP, and durable
// independently of this daemon's spool.
//
// The connection is opened lazily and re-opened on failure, so an htcondordb
// that is down at pelican_man's start (or restarts under it) costs a poll
// cycle's worth of rules, not the daemon.
type DBStore struct {
	addr  string
	table string
	cfg   *config.Config

	mu     sync.Mutex
	client *dbrpc.Client
	conn   *cedarclient.HTCondorClient
	cancel context.CancelFunc
}

// DBConfig configures a DBStore.
type DBConfig struct {
	// Address is the htcondordb daemon's command address: a sinful string or
	// host:port.
	Address string
	// Table names the rule table. Defaults to DefaultRuleTable.
	Table string
	// Config supplies the client security policy (which authentication methods
	// to offer, whether to encrypt) exactly as any other HTCondor client reads
	// it. Required.
	Config *config.Config
}

// OpenDBStore prepares a store against an htcondordb daemon. It does not
// connect: the first operation does, so a database that is not up yet cannot
// stop the daemon from starting.
func OpenDBStore(cfg DBConfig) (*DBStore, error) {
	if strings.TrimSpace(cfg.Address) == "" {
		return nil, fmt.Errorf("store: htcondordb address is required")
	}
	if cfg.Config == nil {
		return nil, fmt.Errorf("store: HTCondor configuration is required for the htcondordb client security policy")
	}
	table := cfg.Table
	if table == "" {
		table = DefaultRuleTable
	}
	return &DBStore{addr: cfg.Address, table: table, cfg: cfg.Config}, nil
}

// connect returns a live dbrpc client, dialing if necessary. The caller holds
// s.mu.
func (s *DBStore) connectLocked(ctx context.Context) (*dbrpc.Client, error) {
	if s.client != nil {
		return s.client, nil
	}

	sec, err := htcondor.GetSecurityConfig(s.cfg, command.DBSession, "CLIENT")
	if err != nil {
		return nil, fmt.Errorf("store: building client security config: %w", err)
	}
	sec.Command = command.DBSession
	// Prefer, rather than require, authentication: PREFERRED maps us to our
	// user (so the server grants WRITE) whenever a mutually-supported method
	// exists, and still connects -- read-only -- when none does. This mirrors
	// htcondordb-cli, so pelican_man and the CLI see the same access level.
	if sec.Authentication == security.SecurityOptional {
		sec.Authentication = security.SecurityPreferred
	}

	// The session outlives this call, so it gets its own cancellable context
	// rather than the per-operation one.
	sessCtx, cancel := context.WithCancel(context.Background())
	dialCtx, dialCancel := context.WithTimeout(ctx, 30*time.Second)
	defer dialCancel()

	// Resolved per dial; see internal/dbaddr.
	addr, err := dbaddr.Resolve(s.addr, s.cfg)
	if err != nil {
		cancel()
		return nil, err
	}

	conn, err := cedarclient.ConnectAndAuthenticate(dialCtx, addr, sec)
	if err != nil {
		cancel()
		return nil, fmt.Errorf("store: connecting to htcondordb at %s: %w", addr, err)
	}

	client := dbrpc.NewClient(dbrpc.NewCedarConn(sessCtx, conn.GetStream()))

	// The table is ours to own; creating it is a no-op once it exists. A
	// permission failure here is not fatal -- a read-only client can still list
	// rules an operator created -- so it is reported by the first write that
	// actually needs it.
	_ = client.CreateTable(ctx, s.table)

	s.client, s.conn, s.cancel = client, conn, cancel
	return client, nil
}

// dropLocked tears down a session that has failed, so the next operation
// redials. The caller holds s.mu.
func (s *DBStore) dropLocked() {
	if s.client != nil {
		_ = s.client.Close()
	}
	if s.cancel != nil {
		s.cancel()
	}
	s.client, s.conn, s.cancel = nil, nil, nil
}

// ListRules implements RuleStore.
func (s *DBStore) ListRules(ctx context.Context) ([]ratelimit.Rule, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	client, err := s.connectLocked(ctx)
	if err != nil {
		return nil, err
	}
	rows, err := client.QueryTable(ctx, s.table, "true", 0)
	if err != nil {
		s.dropLocked()
		return nil, fmt.Errorf("store: listing rules: %w", err)
	}

	rules := make([]ratelimit.Rule, 0, len(rows))
	for _, row := range rows {
		// The server streams ads in the bracketed new-ClassAd form.
		ad, perr := classad.Parse(row)
		if perr != nil {
			return nil, fmt.Errorf("store: parsing a stored rule: %w", perr)
		}
		rule, rerr := ruleFromAd(ad)
		if rerr != nil {
			return nil, fmt.Errorf("store: decoding a stored rule: %w", rerr)
		}
		rules = append(rules, rule)
	}
	ratelimit.SortRules(rules)
	return rules, nil
}

// PutRule implements RuleStore.
func (s *DBStore) PutRule(ctx context.Context, rule ratelimit.Rule) error {
	if err := rule.Validate(); err != nil {
		return err
	}
	s.mu.Lock()
	defer s.mu.Unlock()

	client, err := s.connectLocked(ctx)
	if err != nil {
		return err
	}
	tx, err := client.BeginTable(ctx, s.table)
	if err != nil {
		s.dropLocked()
		return fmt.Errorf("store: beginning a rule write: %w", err)
	}
	// NewClassAd replaces any ad already under the key, which is exactly the
	// upsert semantics PutRule promises.
	if err := tx.NewClassAd(ctx, rule.Name, adFromRule(rule).String()); err != nil {
		_ = tx.Abort(ctx)
		return fmt.Errorf("store: writing rule %q: %w", rule.Name, err)
	}
	if err := tx.Commit(ctx); err != nil {
		return fmt.Errorf("store: committing rule %q: %w", rule.Name, err)
	}
	return nil
}

// DeleteRule implements RuleStore.
func (s *DBStore) DeleteRule(ctx context.Context, name string) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	client, err := s.connectLocked(ctx)
	if err != nil {
		return err
	}
	tx, err := client.BeginTable(ctx, s.table)
	if err != nil {
		s.dropLocked()
		return fmt.Errorf("store: beginning a rule delete: %w", err)
	}
	if err := tx.DestroyClassAd(ctx, name); err != nil {
		_ = tx.Abort(ctx)
		return fmt.Errorf("store: deleting rule %q: %w", name, err)
	}
	if err := tx.Commit(ctx); err != nil {
		return fmt.Errorf("store: committing delete of rule %q: %w", name, err)
	}
	return nil
}

// Close implements RuleStore.
func (s *DBStore) Close() error {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.dropLocked()
	return nil
}

// Attribute names for the stored rule ad. They are spelled out rather than
// derived so a rename in Go cannot silently orphan already-stored rules.
const (
	attrName       = "RuleName"
	attrOrigin     = "Origin"
	attrUser       = "RuleUser"
	attrSite       = "RuleSite"
	attrSources    = "RuleSources"
	attrExpression = "RuleExpression"
	attrRateCount  = "RateCount"
	attrRateWindow = "RateWindowSeconds"
	attrDisabled   = "Disabled"
	attrExpiresAt  = "ExpiresAt"
	attrManaged    = "ConfigManaged"
	attrNote       = "Note"
	attrUpdatedAt  = "UpdatedAt"
)

// adFromRule renders a rule as the ClassAd stored under its name. Sources are
// stored as an HTCondor string list (comma-separated) rather than a ClassAd
// list, so `SELECT ... WHERE stringListMember(...)` works against the table
// from the REPL.
func adFromRule(r ratelimit.Rule) *classad.ClassAd {
	ad := classad.New()
	ad.InsertAttrString(attrName, r.Name)
	ad.InsertAttrString(attrOrigin, string(r.Origin))
	ad.InsertAttrString(attrUser, r.User)
	ad.InsertAttrString(attrSite, r.Site)
	ad.InsertAttrString(attrSources, strings.Join(r.Sources, ","))
	ad.InsertAttrString(attrExpression, r.Expression)
	ad.InsertAttr(attrRateCount, int64(r.RateCount))
	ad.InsertAttr(attrRateWindow, int64(r.Window()/time.Second))
	ad.InsertAttrBool(attrDisabled, r.Disabled)
	ad.InsertAttrBool(attrManaged, r.ConfigManaged)
	ad.InsertAttr(attrExpiresAt, unixOrZero(r.ExpiresAt))
	ad.InsertAttrString(attrNote, r.Note)
	ad.InsertAttr(attrUpdatedAt, unixOrZero(r.UpdatedAt))
	return ad
}

// ruleFromAd is adFromRule's inverse.
func ruleFromAd(ad *classad.ClassAd) (ratelimit.Rule, error) {
	name, ok := ad.EvaluateAttrString(attrName)
	if !ok || name == "" {
		return ratelimit.Rule{}, fmt.Errorf("rule ad has no %s", attrName)
	}
	r := ratelimit.Rule{Name: name}

	origin, _ := ad.EvaluateAttrString(attrOrigin)
	r.Origin = ratelimit.Origin(origin)
	// A rule stored without an origin predates the field (or was written by
	// hand). Treat it as operator policy: that is what a hand-written rule is,
	// and it is the reading that keeps it in force rather than silently
	// dropping it in observing mode.
	if r.Origin != ratelimit.OriginStatic && r.Origin != ratelimit.OriginDynamic {
		r.Origin = ratelimit.OriginStatic
	}

	r.User, _ = ad.EvaluateAttrString(attrUser)
	r.Site, _ = ad.EvaluateAttrString(attrSite)
	if raw, _ := ad.EvaluateAttrString(attrSources); raw != "" {
		for _, s := range strings.Split(raw, ",") {
			if s = strings.TrimSpace(s); s != "" {
				r.Sources = append(r.Sources, s)
			}
		}
	}
	r.Expression, _ = ad.EvaluateAttrString(attrExpression)
	if n, ok := ad.EvaluateAttrInt(attrRateCount); ok {
		r.RateCount = int(n)
	}
	if n, ok := ad.EvaluateAttrInt(attrRateWindow); ok && n > 0 {
		r.RateWindow = time.Duration(n) * time.Second
	}
	r.Disabled, _ = ad.EvaluateAttrBool(attrDisabled)
	r.ConfigManaged, _ = ad.EvaluateAttrBool(attrManaged)
	if n, ok := ad.EvaluateAttrInt(attrExpiresAt); ok && n > 0 {
		r.ExpiresAt = time.Unix(n, 0)
	}
	r.Note, _ = ad.EvaluateAttrString(attrNote)
	if n, ok := ad.EvaluateAttrInt(attrUpdatedAt); ok && n > 0 {
		r.UpdatedAt = time.Unix(n, 0)
	}
	return r, nil
}

func unixOrZero(t time.Time) int64 {
	if t.IsZero() {
		return 0
	}
	return t.Unix()
}
