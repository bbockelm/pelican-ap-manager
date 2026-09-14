package condor

import (
	"context"
	"testing"
	"time"

	"github.com/PelicanPlatform/classad/dbrpc"
)

func quietWatcher(table string) *archiveWatcher {
	return newArchiveWatcher(table, func(string, ...any) {}, nil)
}

// TestTailIsNeverTrustedAcrossAGap is the whole safety argument for reading from
// a tail instead of querying.
//
// The tail is an accelerator: its rows may be used only when it can account for
// every record the table gained since the previous read. After any gap it must
// send the read back to the query, which is cursor-based and therefore covers
// whatever the gap swallowed. Get this wrong in the permissive direction and the
// daemon silently loses records -- transfers that never reach a summary, with no
// error anywhere, which is exactly the failure the query path cannot produce.
func TestTailIsNeverTrustedAcrossAGap(t *testing.T) {
	t.Run("a fresh tail is not trusted", func(t *testing.T) {
		w := quietWatcher("epoch_history")
		// Subscribed and receiving, but the subscription began at the current
		// head: everything committed before it is the query's business.
		w.running, w.live = true, true
		w.append(`[ ClusterId = 1 ]`)
		rows, complete := w.drain()
		if complete {
			t.Error("a first drain was treated as complete; the backlog before the head would be lost")
		}
		if len(rows) != 1 {
			t.Errorf("%d rows buffered, want 1: the rows are still returned, just not trusted", len(rows))
		}
	})

	t.Run("an unbroken tail is trusted", func(t *testing.T) {
		w := quietWatcher("epoch_history")
		w.running, w.live = true, true
		if _, complete := w.drain(); complete {
			t.Fatal("first drain complete")
		}
		w.append(`[ ClusterId = 2 ]`)
		rows, complete := w.drain()
		if !complete {
			t.Error("an unbroken tail was not trusted, so every read would still query")
		}
		if len(rows) != 1 {
			t.Errorf("%d rows, want 1", len(rows))
		}
	})

	t.Run("a break un-trusts the next drain only", func(t *testing.T) {
		w := quietWatcher("epoch_history")
		w.running, w.live = true, true
		w.drain() // clear the initial gap

		w.markBroken()
		if _, complete := w.drain(); complete {
			t.Error("a drain after a break was treated as complete")
		}
		// ...and the one after recovers, or a single blip would send every
		// later read to the query forever.
		w.append(`[ ClusterId = 3 ]`)
		if _, complete := w.drain(); !complete {
			t.Error("the tail never recovered after a break")
		}
	})

	t.Run("overflow drops the buffer and un-trusts it", func(t *testing.T) {
		w := quietWatcher("epoch_history")
		w.running, w.live = true, true
		w.drain()

		for i := 0; i < maxBufferedRows+10; i++ {
			w.append(`[ ClusterId = 4 ]`)
		}
		rows, complete := w.drain()
		if complete {
			t.Error("an overflowed buffer was treated as complete: the dropped rows would never be read")
		}
		// The buffer may be non-empty: events that arrive after the cap is hit
		// are still collected. That is deliberate rather than sloppy. Refusing to
		// buffer while broken would drop events arriving between the query this
		// drain triggers and the next drain, which no query would then cover.
		// What matters is that complete is false, so these rows are discarded and
		// the query supplies the interval instead.
		if len(rows) > maxBufferedRows {
			t.Errorf("%d rows buffered, more than the %d cap", len(rows), maxBufferedRows)
		}
	})

	t.Run("a stopped tail is never trusted", func(t *testing.T) {
		w := quietWatcher("epoch_history")
		w.running, w.live = true, true
		w.drain()
		w.append(`[ ClusterId = 5 ]`)
		w.close()
		if _, complete := w.drain(); complete {
			t.Error("a closed tail reported a complete drain")
		}
	})

	t.Run("a nil watcher is not trusted", func(t *testing.T) {
		var w *archiveWatcher
		rows, complete := w.drain()
		if complete || rows != nil {
			t.Errorf("nil watcher: rows=%v complete=%v, want nil/false", rows, complete)
		}
	})
}

// TestResetAndResyncDropWhatWasBuffered drives the real event loop, because the
// decision being tested lives in it: which event kinds leave the tail
// trustworthy. Calling the helpers directly would assert that markBroken marks
// things broken, and pass just as well with the loop ignoring these kinds
// entirely.
func TestResetAndResyncDropWhatWasBuffered(t *testing.T) {
	for _, tc := range []struct {
		name string
		kind uint8
	}{{"reset", watchReset}, {"resync", watchResync}, {"delete", watchDelete}} {
		t.Run(tc.name, func(t *testing.T) {
			w := quietWatcher("epoch_history")
			w.running, w.live = true, true
			w.drain() // past the initial gap

			ch := make(chan dbrpc.WatchEvent, 3)
			ch <- dbrpc.WatchEvent{Kind: watchUpsert, AdText: `[ ClusterId = 6 ]`}
			ch <- dbrpc.WatchEvent{Kind: tc.kind}
			close(ch)
			if n, _ := w.consume(ch); n == 0 {
				t.Fatal("consume reported no delivery for a stream that had events")
			}

			rows, complete := w.drain()
			if complete {
				t.Errorf("%s left the tail trusted: the read would take a buffer that is missing records", tc.name)
			}
			if len(rows) != 0 {
				t.Errorf("%s kept %d buffered rows", tc.name, len(rows))
			}
		})
	}
}

// TestUpsertsAreBufferedAndSyncedIsHarmless: the ordinary path through the same
// loop, so a change that made every kind distrust the stream would not pass.
func TestUpsertsAreBufferedAndSyncedIsHarmless(t *testing.T) {
	w := quietWatcher("epoch_history")
	w.running, w.live = true, true
	w.drain()

	ch := make(chan dbrpc.WatchEvent, 3)
	ch <- dbrpc.WatchEvent{Kind: watchUpsert, AdText: `[ ClusterId = 7 ]`}
	ch <- dbrpc.WatchEvent{Kind: watchSynced}
	ch <- dbrpc.WatchEvent{Kind: watchUpsert, AdText: `[ ClusterId = 8 ]`}
	close(ch)
	w.consume(ch) //nolint:errcheck // counts unused here

	rows, complete := w.drain()
	if !complete {
		t.Error("an unbroken stream of upserts left the tail untrusted")
	}
	if len(rows) != 2 {
		t.Errorf("%d rows buffered, want 2", len(rows))
	}
}

// TestArchiveRowsQueriesUnlessTheTailIsComplete checks the seam rather than the
// watcher: that a read takes the tail's rows only when the tail is trusted, and
// otherwise issues the query.
//
// The two failure directions are not symmetric. Querying when the tail would
// have done costs a query. Taking the tail when it cannot account for the
// interval loses records silently, so that is the one the test pins.
func TestArchiveRowsQueriesUnlessTheTailIsComplete(t *testing.T) {
	newClient := func(t *testing.T) (*mirrorClient, *int) {
		t.Helper()
		c, err := NewMirrorClient(&stubClient{}, MirrorConfig{
			Address: "127.0.0.1:1",
			Config:  testCondorConfig(t),
			Watch:   true,
		})
		if err != nil {
			t.Fatalf("NewMirrorClient: %v", err)
		}
		m := c.(*mirrorClient)
		queries := 0
		m.query = func(context.Context, string, string) ([]string, error) {
			queries++
			return []string{`[ ClusterId = 99 ]`}, nil
		}
		return m, &queries
	}

	t.Run("broken tail: queries", func(t *testing.T) {
		m, queries := newClient(t)
		m.transferWatch.running, m.transferWatch.live = true, true
		m.transferWatch.append(`[ ClusterId = 1 ]`) // buffered, but not trusted yet

		m.mu.Lock()
		rows, err := m.archiveRows(context.Background(), m.transferWatch, m.transferTable, "true")
		m.mu.Unlock()
		if err != nil {
			t.Fatalf("archiveRows: %v", err)
		}
		if *queries != 1 {
			t.Errorf("%d queries with an untrusted tail, want 1", *queries)
		}
		if len(rows) != 1 || rows[0] != `[ ClusterId = 99 ]` {
			t.Errorf("rows = %v, want the query's answer -- the tail's rows must not be used here", rows)
		}
	})

	t.Run("complete tail: no query", func(t *testing.T) {
		m, queries := newClient(t)
		w := m.transferWatch
		w.running, w.live = true, true
		w.drain() // clear the initial gap, as a first read would

		w.append(`[ ClusterId = 2 ]`)
		m.mu.Lock()
		rows, err := m.archiveRows(context.Background(), w, m.transferTable, "true")
		m.mu.Unlock()
		if err != nil {
			t.Fatalf("archiveRows: %v", err)
		}
		if *queries != 0 {
			t.Errorf("%d queries with a complete tail, want 0 -- the tail bought nothing", *queries)
		}
		if len(rows) != 1 || rows[0] != `[ ClusterId = 2 ]` {
			t.Errorf("rows = %v, want the tail's rows", rows)
		}
	})

	t.Run("watching disabled: always queries", func(t *testing.T) {
		c, err := NewMirrorClient(&stubClient{}, MirrorConfig{
			Address: "127.0.0.1:1",
			Config:  testCondorConfig(t),
		})
		if err != nil {
			t.Fatalf("NewMirrorClient: %v", err)
		}
		m := c.(*mirrorClient)
		if m.transferWatch != nil || m.jobWatch != nil {
			t.Fatal("watchers were created without Watch being set")
		}
		queries := 0
		m.query = func(context.Context, string, string) ([]string, error) {
			queries++
			return nil, nil
		}
		m.mu.Lock()
		_, err = m.archiveRows(context.Background(), m.transferWatch, m.transferTable, "true")
		m.mu.Unlock()
		if err != nil {
			t.Fatalf("archiveRows: %v", err)
		}
		if queries != 1 {
			t.Errorf("%d queries with watching off, want 1", queries)
		}
	})
}

// TestAPumpThatCannotSubscribeIsNeverTrusted is the regression test for a tail
// that reported complete intervals while delivering nothing.
//
// ensure() marks the watcher running before the pump has subscribed to anything.
// When the subscription then fails -- the database is down, or the table cannot
// be resolved, which is what happened against a real htcondordb whose archives
// could be watched but whose change-log head could not be read -- the pump loops
// and retries. Keying the trust decision on "a pump exists" meant a drain
// between two failed attempts returned an empty buffer marked complete, so the
// read skipped the query and silently saw no records at all.
func TestAPumpThatCannotSubscribeIsNeverTrusted(t *testing.T) {
	w := quietWatcher("epoch_history")

	// What ensure() does before any subscription exists.
	w.running = true

	// A retry loop: each failure marks the tail broken, and drains happen in
	// between, as polls do.
	for range 3 {
		w.markBroken()
		if rows, complete := w.drain(); complete {
			t.Fatalf("a pump with no live subscription reported a complete interval (%d rows): "+
				"the read would skip the query and see nothing", len(rows))
		}
	}

	// Two drains in a row with no failure between them: still not trusted,
	// because nothing is streaming.
	if _, complete := w.drain(); complete {
		t.Error("a watcher that never subscribed became trusted after a quiet interval")
	}
	if _, complete := w.drain(); complete {
		t.Error("a watcher that never subscribed became trusted on a later drain")
	}
}

// TestArrivalsCoalesceAndNeverBlock covers the two properties the signal has to
// have, both of which matter more than the signal itself.
//
// It must coalesce: the reader drains everything buffered when it wakes, so a
// thousand records arriving want one read, not a thousand. And it must never
// block: the sender is the watch pump, and a pump stalled on a busy consumer
// stops reading the stream, which eventually demotes the subscription
// server-side -- the feature would then degrade the very thing it exists to
// speed up.
func TestArrivalsCoalesceAndNeverBlock(t *testing.T) {
	notify := make(chan struct{}, 1)
	w := newArchiveWatcher("epoch_history", func(string, ...any) {}, notify)
	w.running, w.live = true, true

	// Nobody is reading the channel. A thousand arrivals must still not block.
	done := make(chan struct{})
	go func() {
		for i := 0; i < 1000; i++ {
			w.append(`[ ClusterId = 1 ]`)
		}
		close(done)
	}()
	select {
	case <-done:
	case <-time.After(5 * time.Second):
		t.Fatal("append blocked on the arrival signal: a stalled pump stops reading the " +
			"stream, which gets the subscription demoted")
	}

	// And they coalesced into exactly one pending signal.
	got := 0
	for {
		select {
		case <-notify:
			got++
			continue
		default:
		}
		break
	}
	if got != 1 {
		t.Errorf("%d pending signals after 1000 arrivals, want 1", got)
	}
}

// TestNoSignalWithoutWatching: a watcher with no channel is the watching-off
// case, and must not panic on a send.
func TestNoSignalWithoutWatching(t *testing.T) {
	w := quietWatcher("epoch_history")
	w.running, w.live = true, true
	w.append(`[ ClusterId = 1 ]`) // nil notify channel
	if rows, _ := w.drain(); len(rows) != 1 {
		t.Errorf("%d rows, want 1: the row must still be buffered without a signal channel", len(rows))
	}
}
