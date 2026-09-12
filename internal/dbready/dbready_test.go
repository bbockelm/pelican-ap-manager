package dbready

import (
	"context"
	"fmt"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/bbockelm/golang-htcondor/webapi/dbmirror"
)

// TestNilCheckerIsNotReady: a daemon with no collector cannot evaluate the
// mirror. Reporting ready would send reads at something that may be hours
// behind; reporting not-ready costs only the schedd load we already had.
func TestNilCheckerIsNotReady(t *testing.T) {
	var c *Checker
	for _, src := range []Source{SourceJobs, SourceHistory, SourceEpoch} {
		ready, why := c.Ready(context.Background(), string(src))
		if ready {
			t.Errorf("%s reported ready with no collector", src)
		}
		if why == "" {
			t.Errorf("%s gave no reason", src)
		}
	}

	// And New with nothing configured behaves the same, rather than erroring at
	// startup: the mirror is an accelerator, not a prerequisite.
	c = New(Options{})
	if ready, why := c.Ready(context.Background(), string(SourceHistory)); ready || why == "" {
		t.Errorf("New(nothing): ready=%v why=%q", ready, why)
	}
}

// TestTolerancesAreApplied: the leeway is the whole point of the knob. dbmirror
// defaults to requiring a syncer at exactly EOF and minutes of staleness; this
// daemon wants a little byte leeway and a much tighter clock.
func TestTolerancesAreApplied(t *testing.T) {
	jobs, hist, epoch, bytes := dbmirror.JobsToleranceSecs, dbmirror.HistoryToleranceSecs, dbmirror.EpochToleranceSecs, dbmirror.CaughtUpLagBytes
	t.Cleanup(func() {
		dbmirror.JobsToleranceSecs, dbmirror.HistoryToleranceSecs = jobs, hist
		dbmirror.EpochToleranceSecs, dbmirror.CaughtUpLagBytes = epoch, bytes
	})

	New(Options{})
	if dbmirror.CaughtUpLagBytes != DefaultMaxLagBytes {
		t.Errorf("CaughtUpLagBytes = %d, want the %d default", dbmirror.CaughtUpLagBytes, DefaultMaxLagBytes)
	}
	for name, got := range map[string]int64{
		"jobs":    dbmirror.JobsToleranceSecs,
		"history": dbmirror.HistoryToleranceSecs,
		"epoch":   dbmirror.EpochToleranceSecs,
	} {
		if want := int64(DefaultMaxLag.Seconds()); got != want {
			t.Errorf("%s tolerance = %ds, want %ds", name, got, want)
		}
	}

	// Explicit options win.
	New(Options{MaxLag: 45 * time.Second, MaxLagBytes: 4096})
	if dbmirror.JobsToleranceSecs != 45 || dbmirror.CaughtUpLagBytes != 4096 {
		t.Errorf("explicit options ignored: %ds / %d bytes", dbmirror.JobsToleranceSecs, dbmirror.CaughtUpLagBytes)
	}

	// A sub-second tolerance floors at one second rather than zero, which would
	// reject every mirror that had ever taken a moment to poll.
	New(Options{MaxLag: 100 * time.Millisecond})
	if dbmirror.JobsToleranceSecs < 1 {
		t.Errorf("tolerance = %ds, want at least 1", dbmirror.JobsToleranceSecs)
	}
}

// TestReportSaysWhatChangedNotWhatIsTrue: a mirror that is behind stays behind
// for as long as it takes to catch up. At a one-second poll, reporting the state
// would be thousands of identical lines; reporting the transition is what an
// operator can actually read.
func TestReportSaysWhatChangedNotWhatIsTrue(t *testing.T) {
	c := New(Options{})
	var lines []string
	logf := func(f string, a ...any) { lines = append(lines, strings.TrimSpace(sprintf(f, a...))) }

	for i := 0; i < 5; i++ {
		c.Report(string(SourceEpoch), false, "mirror is not caught up", logf)
	}
	if len(lines) != 1 {
		t.Fatalf("%d lines for the same reason five times, want 1: %v", len(lines), lines)
	}
	if !strings.Contains(lines[0], "schedd") || !strings.Contains(lines[0], "not caught up") {
		t.Errorf("line does not say where reads went or why: %q", lines[0])
	}

	// Recovery is a change, and worth a line -- otherwise the log says reads
	// moved to the schedd and never says they came back.
	lines = nil
	c.Report(string(SourceEpoch), true, "", logf)
	if len(lines) != 1 || !strings.Contains(lines[0], "htcondordb") {
		t.Errorf("recovery not reported: %v", lines)
	}

	// A different reason is a different line.
	lines = nil
	c.Report(string(SourceEpoch), false, "durability gap", logf)
	c.Report(string(SourceEpoch), false, "durability gap", logf)
	if len(lines) != 1 || !strings.Contains(lines[0], "durability gap") {
		t.Errorf("new reason not reported once: %v", lines)
	}
}

// TestReportTracksSourcesSeparately: a gap in epoch history must not silence or
// trigger reporting for the live queue. They fall back independently, so they
// report independently.
func TestReportTracksSourcesSeparately(t *testing.T) {
	c := New(Options{})
	var lines []string
	logf := func(f string, a ...any) { lines = append(lines, sprintf(f, a...)) }

	c.Report(string(SourceEpoch), false, "durability gap", logf)
	c.Report(string(SourceJobs), false, "durability gap", logf)
	if len(lines) != 2 {
		t.Errorf("%d lines for two sources with the same reason, want 2: %v", len(lines), lines)
	}
}

// TestReportIsSafeConcurrently: Report is called from the poll loop while the
// renewal timer and command handlers run.
func TestReportIsSafeConcurrently(t *testing.T) {
	c := New(Options{})
	var mu sync.Mutex
	logf := func(string, ...any) { mu.Lock(); mu.Unlock() } //nolint:staticcheck // exercising the lock

	var wg sync.WaitGroup
	for i := 0; i < 32; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			c.Report(string(SourceJobs), i%2 == 0, "behind", logf)
		}(i)
	}
	wg.Wait()
}

func sprintf(f string, a ...any) string { return fmt.Sprintf(f, a...) }
