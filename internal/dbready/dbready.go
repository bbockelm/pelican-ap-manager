// Package dbready decides whether htcondordb is current enough to read from
// instead of the schedd.
//
// The mirror is only worth using while it is keeping up. htcondordb advertises
// per-source sync health -- how far behind its tailer is for job_queue.log,
// HISTORY and JOB_EPOCH_HISTORY, and whether it hit a durability gap -- and
// golang-htcondor's dbmirror package already turns that into a routing decision
// for the REST API. This reuses that policy rather than growing a second
// opinion about what "caught up" means, which is how two components end up
// disagreeing about the same mirror.
//
// Health reaches this daemon two ways. htcondordb advertises it to the
// collector, which is how a pool-wide consumer finds a database and judges it.
// But an access point commonly points pelican-man at a database by address file
// with no collector in the picture, and then there is no advertisement to read.
// For that case htcondordb answers the same ClassAd over its command port, so
// the gate works from either source and reaches the same decision from the same
// parser.
//
// What this adds is the pelican-man-specific part: which source backs which
// read, a per-source fallback so a gap in one table does not cost the others,
// and reporting that says why a read went to the schedd without repeating
// itself once per poll.
package dbready

import (
	"context"
	"fmt"
	"strings"
	"sync"
	"time"

	"github.com/PelicanPlatform/classad/classad"

	htcondor "github.com/bbockelm/golang-htcondor"
	"github.com/bbockelm/golang-htcondor/config"
	"github.com/bbockelm/golang-htcondor/webapi/dbmirror"
	"github.com/bbockelm/htcondordb/dbstatus"

	"github.com/bbockelm/pelican-ap-manager/internal/dbaddr"
)

// Source names a thing pelican-man reads, and therefore a sync source it
// depends on.
type Source string

const (
	// SourceJobs is the live queue: job_queue.log mirrored into "jobs".
	SourceJobs Source = "jobs"
	// SourceHistory is completed jobs: HISTORY mirrored into "history".
	SourceHistory Source = "history"
	// SourceEpoch is per-run-instance records, which is where transfers live:
	// JOB_EPOCH_HISTORY mirrored into "epoch_history".
	SourceEpoch Source = "epoch_history"
)

// Defaults for how far behind a source may be and still be read from.
//
// Both are leeway on "caught up" rather than a tolerance for genuinely stale
// data: a tailer following a busy job_queue.log is a few kilobytes behind most
// of the time, and refusing to read until it reaches exactly EOF would mean
// almost never reading. Ten seconds and ten kilobytes are small next to the
// poll interval and to what an access point writes in that time.
const (
	DefaultMaxLag      = 10 * time.Second
	DefaultMaxLagBytes = 10 << 10
)

// DefaultStatusTTL bounds how often the command-port path asks the database for
// its health. The gate is consulted once per source per poll, so without a cache
// a one-second poll would open three connections a second to ask a question
// whose answer moves on the order of the tolerance. Well under DefaultMaxLag, so
// caching cannot by itself let a stale mirror through.
const DefaultStatusTTL = 2 * time.Second

// Checker answers whether a source is current enough to read.
//
// The zero value, and a nil Checker, report every source as not ready with a
// reason saying so -- a checker with no way to reach the database cannot know,
// and guessing yes would send reads at a mirror that may be hours behind.
type Checker struct {
	// locator reads the health htcondordb advertises to the collector. Nil when
	// no collector is configured.
	locator *dbmirror.Locator

	// dbAddr asks the database directly, for deployments with no collector. It
	// is the same address the mirror client reads from, so a gate exists
	// wherever a mirror read is possible.
	dbAddr string
	cfg    *config.Config
	ttl    time.Duration

	// queryStatus is dbstatus.Query and discover is the locator's, both
	// indirected so a test can drive either source without a pool.
	queryStatus func(context.Context, *config.Config, string) (*classad.ClassAd, error)
	discover    func(context.Context) (*dbmirror.Info, error)

	mu     sync.Mutex
	last   map[string]string
	info   *dbmirror.Info
	infoAt time.Time
	infoRr error
}

// Options configures a Checker.
type Options struct {
	// Collector is the pool collector, where htcondordb advertises its sync
	// health. Optional: with no collector the health is read from DBAddress
	// instead.
	Collector *htcondor.Collector
	// DBAddress is the htcondordb command address (or address file). Used when
	// no collector is configured, which is the address-file deployment an access
	// point typically runs. Optional, but with neither this nor Collector the
	// checker can learn nothing and declines every source.
	DBAddress string
	// Config supplies the security policy for the collector query and the
	// command-port request.
	Config *config.Config

	// StatusTTL caches the command-port answer. Zero means the package default.
	StatusTTL time.Duration

	// MaxLag and MaxLagBytes are the leeway on caught-up. Zero means the
	// package default.
	MaxLag      time.Duration
	MaxLagBytes int64
}

// New returns a Checker. It is safe to call with a nil collector or config, in
// which case every source reports not ready rather than erroring at startup:
// the mirror is an accelerator, and a daemon that cannot evaluate it should
// keep reading the schedd rather than refuse to run.
//
// The tolerances are dbmirror's process-wide policy, so New sets them for the
// whole process. That is the intended use -- set once at startup -- and this is
// the only component in pelican-man that reads from a mirror.
func New(opts Options) *Checker {
	maxLag := opts.MaxLag
	if maxLag <= 0 {
		maxLag = DefaultMaxLag
	}
	maxBytes := opts.MaxLagBytes
	if maxBytes <= 0 {
		maxBytes = DefaultMaxLagBytes
	}

	secs := int64(maxLag.Seconds())
	if secs < 1 {
		secs = 1
	}
	dbmirror.JobsToleranceSecs = secs
	dbmirror.HistoryToleranceSecs = secs
	dbmirror.EpochToleranceSecs = secs
	dbmirror.CaughtUpLagBytes = maxBytes

	ttl := opts.StatusTTL
	if ttl <= 0 {
		ttl = DefaultStatusTTL
	}

	c := &Checker{last: map[string]string{}, cfg: opts.Config, ttl: ttl, queryStatus: dbstatus.Query}
	if opts.Collector != nil && opts.Config != nil {
		c.locator = dbmirror.NewLocator(opts.Collector, opts.Config)
		c.discover = c.locator.Discover
	}
	if opts.Config != nil {
		c.dbAddr = strings.TrimSpace(opts.DBAddress)
	}
	return c
}

// lookup returns the database's current sync health.
//
// The collector is tried first: its advertisement is already cached by the
// locator, and it is the same ad every other consumer in the pool judges this
// database by. When that turns up nothing the database is asked directly.
//
// The fallback is on the collector FAILING, not on one being unconfigured,
// because COLLECTOR_HOST has a default -- an access point that runs no collector
// still names one. Treating "configured" as "available" would leave the command
// port unreachable in exactly the deployment it was added for.
//
// Falling back rather than treating a missing collector as "assume current" is
// the point: an ungated read is the failure this package exists to prevent, and
// it fails silently, because a mirror that is hours behind still answers.
func (c *Checker) lookup(ctx context.Context) (*dbmirror.Info, error) {
	var advertised error
	if c.discover != nil {
		info, err := c.discover(ctx)
		if err == nil {
			return info, nil
		}
		advertised = err
	}
	if c.dbAddr == "" {
		if advertised != nil {
			return nil, advertised
		}
		return nil, fmt.Errorf("no collector and no database address, so htcondordb's sync health cannot be read")
	}

	c.mu.Lock()
	defer c.mu.Unlock()
	if c.info != nil || c.infoRr != nil {
		if time.Since(c.infoAt) < c.ttl {
			return c.info, c.infoRr
		}
	}

	// Resolved per request, like every other dial at this database: the
	// configured value is commonly an address file, and the address inside it
	// changes when the daemon restarts.
	addr, rerr := dbaddr.Resolve(c.dbAddr, c.cfg)
	if rerr != nil {
		c.info, c.infoRr = nil, rerr
		c.infoAt = time.Now()
		return nil, rerr
	}

	ad, err := c.queryStatus(ctx, c.cfg, addr)
	c.infoAt = time.Now()
	if err != nil {
		c.info, c.infoRr = nil, err
		return nil, err
	}
	c.info, c.infoRr = dbmirror.ParseAd(ad), nil
	return c.info, nil
}

// Ready reports whether src may be read from the mirror, and why not when it
// may not. The reason is prose meant for a log line; callers that need to
// branch should branch on the boolean.
//
// Discovery is cached by the locator, so calling this per poll costs a collector
// query only when the cached advertisement has aged out.
func (c *Checker) Ready(ctx context.Context, src string) (bool, string) {
	if c == nil || (c.discover == nil && c.dbAddr == "") {
		return false, "htcondordb's sync health cannot be read: no collector and no database address"
	}

	info, err := c.lookup(ctx)
	if err != nil {
		return false, fmt.Sprintf("cannot read htcondordb's sync health: %v", err)
	}

	var d dbmirror.Decision
	switch Source(src) {
	case SourceJobs:
		d = dbmirror.JobsDecision(info, "")
	case SourceHistory:
		d = dbmirror.HistoryDecision(info, nil)
	case SourceEpoch:
		d = dbmirror.EpochDecision(info)
	default:
		return false, fmt.Sprintf("unknown source %q", src)
	}
	if d.Use {
		return true, ""
	}
	return false, fmt.Sprintf("%s (%s)", d.Note, d.Reason)
}

// Report logs a source's readiness transition, once per distinct reason rather
// than once per poll.
//
// A mirror that is behind is behind for as long as it takes to catch up, which
// at a one-second poll is thousands of identical lines. Reporting the change
// instead keeps the log readable while still saying when reads moved and why --
// the thing an operator actually needs, and the thing a silent fallback denies
// them.
func (c *Checker) Report(src string, ready bool, reason string, logf func(string, ...any)) {
	if c == nil || logf == nil {
		return
	}
	state := reason
	if ready {
		state = ""
	}

	c.mu.Lock()
	prev, seen := c.last[src]
	changed := !seen || prev != state
	c.last[src] = state
	c.mu.Unlock()

	if !changed {
		return
	}
	if ready {
		logf("reading %s from htcondordb", src)
		return
	}
	logf("reading %s from the schedd: %s", src, strings.TrimSpace(reason))
}

// Health exposes the discovered mirror for status reporting. Nil when nothing
// has been discovered.
func (c *Checker) Health() *dbmirror.Health {
	if c == nil || c.locator == nil {
		return nil
	}
	h := c.locator.Health()
	return &h
}
