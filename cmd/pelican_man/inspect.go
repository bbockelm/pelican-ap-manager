package main

import (
	"context"
	"fmt"
	"io"
	"log"
	"log/slog"
	"os"
	"sort"
	"strings"
	"text/tabwriter"
	"time"

	condorconfig "github.com/bbockelm/golang-htcondor/config"
	"github.com/bbockelm/pelican-ap-manager/internal/condor"
	"github.com/bbockelm/pelican-ap-manager/internal/config"
	"github.com/bbockelm/pelican-ap-manager/internal/ratelimit"
	"github.com/bbockelm/pelican-ap-manager/internal/store"
)

// The inspection commands exist because HTCondor ships no way to see this.
// Startup limits live inside the schedd and are reachable only over its
// qmgmt command socket -- there is no condor_q or condor_status option that
// lists them -- so without these an admin has no way to answer "is my rule
// actually in force?" short of reading the daemon log.
//
// Both are read-only, take the same configuration the daemon does, and are
// meant to be run on the AP.

// quietLogging silences the library logging that would otherwise bury a
// one-screen report under CEDAR's per-connection security chatter. Warnings and
// errors still reach stderr, so a real problem is not hidden -- only the
// successful handshake narration is dropped.
//
// -debug brings the full stream back, for when the report itself is the thing
// misbehaving. Deliberately a flag rather than PELICAN_MANAGER_DEBUG: that macro
// configures the daemon's log, and a site that has turned it up should not find
// its command-line tools unusable as a side effect.
func quietLogging(debug bool) {
	if debug {
		return
	}
	slog.SetDefault(slog.New(slog.NewTextHandler(os.Stderr, &slog.HandlerOptions{Level: slog.LevelWarn})))
	// golang-htcondor's libraries also write through the standard logger when no
	// daemon logger is configured, which is the case for a command-line tool.
	log.SetOutput(io.Discard)
}

// report is a writer that remembers its first failure, so a command can print a
// table without checking every call and still notice a closed stdout (`| head`)
// once, at the end.
type report struct {
	w   io.Writer
	err error
}

func newReport(w io.Writer) *report { return &report{w: w} }

func (r *report) Write(p []byte) (int, error) {
	if r.err != nil {
		return len(p), nil
	}
	n, err := r.w.Write(p)
	r.err = err
	return n, err
}

func (r *report) printf(format string, a ...any) {
	if r.err != nil {
		return
	}
	_, r.err = fmt.Fprintf(r.w, format, a...)
}

func (r *report) println(a ...any) {
	if r.err != nil {
		return
	}
	_, r.err = fmt.Fprintln(r.w, a...)
}

// reportLimits prints the startup limits the schedd currently holds: what is
// actually throttling jobs right now, whoever installed it.
func reportLimits(ctx context.Context, condorCfg *condorconfig.Config, w io.Writer, allTags, debug bool) error {
	quietLogging(debug)
	out := newReport(w)

	cfg, err := config.LoadFrom(condorCfg)
	if err != nil {
		return fmt.Errorf("loading configuration: %w", err)
	}

	client, err := condor.NewClient(cfg.CollectorHost, cfg.ScheddName, cfg.SiteAttribute)
	if err != nil {
		return fmt.Errorf("building condor client: %w", err)
	}

	ctx, cancel := context.WithTimeout(ctx, 30*time.Second)
	defer cancel()

	schedd, err := client.LocateSchedd(ctx)
	if err != nil {
		return fmt.Errorf("%w\n\nCheck PELICAN_MANAGER_COLLECTOR_HOST (%s) and PELICAN_MANAGER_SCHEDD_NAME (%q)",
			err, cfg.CollectorHost, cfg.ScheddName)
	}

	// An empty tag returns every limit in the schedd, including any installed by
	// something other than this daemon -- which is what an admin asking "what is
	// throttling my jobs" wants to see. --mine narrows it to ours.
	tag := ""
	if !allTags {
		tag = limitTagFor(cfg)
	}

	limits, err := schedd.QueryStartupLimits(ctx, "", tag)
	if err != nil {
		return fmt.Errorf("querying startup limits from schedd %s: %w", schedd.Name(), err)
	}

	if len(limits) == 0 {
		out.printf("No startup limits installed in schedd %s.\n", schedd.Name())
		if !allTags {
			out.printf("(Showing only limits tagged %q; pass -limits-all to see every limit.)\n", tag)
		}
		return out.err
	}

	sort.Slice(limits, func(i, j int) bool { return limits[i].Name < limits[j].Name })

	out.printf("Startup limits in schedd %s:\n\n", schedd.Name())
	tw := tabwriter.NewWriter(out, 0, 0, 2, ' ', 0)
	_, _ = fmt.Fprintln(tw, "NAME\tRATE\tALLOWED\tSKIPPED\tLAST HIT\tEXPRESSION")
	for _, l := range limits {
		window := time.Duration(l.RateWindow) * time.Second
		rate := fmt.Sprintf("%d/%s", l.RateCount, window)
		if l.RateCount == 0 {
			// A zero rate counts matching starts without blocking any.
			rate = "monitor only"
		}
		_, _ = fmt.Fprintf(tw, "%s\t%s\t%d\t%d\t%s\t%s\n",
			nameOrUUID(l.Name, l.UUID), rate, l.JobsAllowed, l.JobsSkipped,
			relativeTime(l.LastIgnored), truncate(l.Expression, 60))
	}
	if err := tw.Flush(); err != nil {
		return err
	}

	out.printf("\nSKIPPED counts jobs this limit held back. LAST HIT is when it last did so.\n")
	return out.err
}

// reportRules prints the rule set from the store: what this daemon intends,
// including rules that observing mode is deliberately not enforcing. Reading it
// next to -limits is how an admin tells "the daemon has not decided that yet"
// apart from "the daemon decided but is not enforcing it".
func reportRules(ctx context.Context, condorCfg *condorconfig.Config, w io.Writer, debug bool) error {
	quietLogging(debug)
	out := newReport(w)

	cfg, err := config.LoadFrom(condorCfg)
	if err != nil {
		return fmt.Errorf("loading configuration: %w", err)
	}

	rs, desc, err := store.Open(store.Options{
		DBAddress: cfg.RuleDBAddress,
		DBTable:   cfg.RuleDBTable,
		FilePath:  cfg.RuleStorePath,
		Config:    condorCfg,
	})
	if err != nil {
		return fmt.Errorf("opening the rate rule store: %w", err)
	}
	defer func() { _ = rs.Close() }()

	ctx, cancel := context.WithTimeout(ctx, 30*time.Second)
	defer cancel()

	rules, err := rs.ListRules(ctx)
	if err != nil {
		return fmt.Errorf("reading the rate rule store (%s): %w", desc, err)
	}

	mode := cfg.EnforcementMode
	out.printf("Rate rules in %s\nEnforcement mode: %s\n\n", desc, mode)

	if len(rules) == 0 {
		out.println("No rules stored.")
		return out.err
	}

	now := time.Now()
	tw := tabwriter.NewWriter(out, 0, 0, 2, ' ', 0)
	_, _ = fmt.Fprintln(tw, "NAME\tORIGIN\tRATE\tSELECTOR\tSTATUS\tNOTE")
	for _, r := range rules {
		rate := fmt.Sprintf("%d/%s", r.RateCount, r.Window())
		if r.RateCount == 0 {
			rate = "monitor only"
		}
		_, _ = fmt.Fprintf(tw, "%s\t%s\t%s\t%s\t%s\t%s\n",
			r.Name, r.Origin, rate, selectorOf(r), ruleStatus(r, mode, now), r.Note)
	}
	if err := tw.Flush(); err != nil {
		return err
	}

	out.printf("\n%q means the rule is installed in the schedd; run -limits to confirm.\n", statusEnforced)
	if mode == ratelimit.ModeObserving {
		out.printf("Observing mode withholds dynamic rules; set PELICAN_MANAGER_ENFORCEMENT_MODE = enforcing to apply them.\n")
	}
	return out.err
}

// Status words used by reportRules.
const (
	statusEnforced = "enforced"
	statusObserved = "observed"
	statusDisabled = "disabled"
	statusExpired  = "expired"
)

func ruleStatus(r ratelimit.Rule, mode ratelimit.Mode, now time.Time) string {
	switch {
	case r.Disabled:
		return statusDisabled
	case !r.ExpiresAt.IsZero() && !now.Before(r.ExpiresAt):
		return statusExpired
	case !mode.Enforces(r.Origin):
		return statusObserved
	default:
		return statusEnforced
	}
}

// selectorOf renders a rule's scope in the same vocabulary the configuration
// uses, so what is printed can be pasted back into a rule body.
func selectorOf(r ratelimit.Rule) string {
	if r.Expression != "" {
		return "expr=" + truncate(r.Expression, 40)
	}
	var parts []string
	if r.User != "" {
		parts = append(parts, "user="+r.User)
	}
	if r.Site != "" {
		parts = append(parts, "site="+r.Site)
	}
	if len(r.Sources) > 0 {
		parts = append(parts, "sources="+strings.Join(r.Sources, ","))
	}
	if len(parts) == 0 {
		return "(any job)"
	}
	return strings.Join(parts, " ")
}

// limitTagFor mirrors how the running daemon tags the limits it installs (see
// Service.ensureLimitManager): the schedd name, else the hostname.
func limitTagFor(cfg *config.Config) string {
	if cfg.ScheddName != "" {
		return cfg.ScheddName
	}
	if hostname, err := os.Hostname(); err == nil {
		return hostname
	}
	return "pelican_man"
}

func nameOrUUID(name, uuid string) string {
	if name != "" {
		return name
	}
	return uuid
}

func relativeTime(unix int64) string {
	if unix <= 0 {
		return "never"
	}
	d := time.Since(time.Unix(unix, 0)).Truncate(time.Second)
	if d < 0 {
		return "just now"
	}
	return d.String() + " ago"
}

func truncate(s string, max int) string {
	s = strings.Join(strings.Fields(s), " ")
	if len(s) <= max {
		return s
	}
	return s[:max-1] + "…"
}
