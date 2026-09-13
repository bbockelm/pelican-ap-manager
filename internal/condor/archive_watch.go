package condor

import (
	"context"
	"sync"
	"time"

	"github.com/PelicanPlatform/classad/dbrpc"
)

// Watch event kinds, mirroring db.WatchKind. Declared here rather than imported
// because dbrpc reports the kind as a bare uint8.
const (
	watchUpsert uint8 = iota
	watchDelete
	watchReset
	watchSynced
	watchResync
)

// maxBufferedRows bounds what a watcher holds between drains. The poll that
// drains it runs every PELICAN_MANAGER_POLL_INTERVAL, so this only fills if the
// database is producing records far faster than the daemon consumes them -- at
// which point the right answer is to stop buffering and let the next read go
// back to a query, which gets the same records in one pass.
const maxBufferedRows = 20000

// archiveWatcher tails one archive table, buffering the rows it sees so a
// subsequent read can take them instead of issuing a query.
//
// It is deliberately an accelerator, not a source of truth. The buffer is used
// only when the subscription has been unbroken since the previous read; any gap
// -- a first subscribe, a reconnect, a reset, an overflow -- sends that read
// back to the query path, which is cursor-based and therefore covers whatever
// the gap swallowed. That way a watch that misbehaves costs a query, never a
// missing record.
type archiveWatcher struct {
	table string
	logf  func(string, ...any)

	mu sync.Mutex
	// rows holds the ad text of upserts seen since the last drain. Ad text
	// rather than decoded records, so the decode stays the one that the query
	// path uses and the two cannot read an ad differently.
	rows []string
	// broken records that something between the last drain and now could have
	// lost events. It starts true: a fresh subscription tails from the current
	// head, so everything committed before it is the query's business.
	broken  bool
	running bool
	stop    func()
}

func newArchiveWatcher(table string, logf func(string, ...any)) *archiveWatcher {
	return &archiveWatcher{table: table, logf: logf, broken: true}
}

// drain returns the rows buffered since the last call, and whether they can be
// trusted as the complete set of what the table gained in that interval.
//
// A false second return is not an error: it means this read should query. The
// rows are returned either way and the caller may ignore them.
func (w *archiveWatcher) drain() ([]string, bool) {
	if w == nil {
		return nil, false
	}
	w.mu.Lock()
	defer w.mu.Unlock()

	rows := w.rows
	w.rows = nil
	complete := w.running && !w.broken
	if w.running {
		w.broken = false
	}
	return rows, complete
}

// ensure starts the tail if it is not already running. Safe to call on every
// read; it returns immediately once a pump is up.
func (w *archiveWatcher) ensure(client *dbrpc.Client, connect func(context.Context) (*dbrpc.Client, error)) {
	if w == nil {
		return
	}
	w.mu.Lock()
	if w.running {
		w.mu.Unlock()
		return
	}
	w.running = true
	w.broken = true
	w.mu.Unlock()

	go w.pump(client, connect)
}

// close stops the tail.
func (w *archiveWatcher) close() {
	if w == nil {
		return
	}
	w.mu.Lock()
	stop, running := w.stop, w.running
	w.running, w.stop, w.rows, w.broken = false, nil, nil, true
	w.mu.Unlock()
	if running && stop != nil {
		stop()
	}
}

// pump keeps a subscription up, reconnecting with backoff. It runs until close.
func (w *archiveWatcher) pump(client *dbrpc.Client, connect func(context.Context) (*dbrpc.Client, error)) {
	backoff := time.Second
	for {
		w.mu.Lock()
		running := w.running
		w.mu.Unlock()
		if !running {
			return
		}

		if client == nil {
			var err error
			ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
			client, err = connect(ctx)
			cancel()
			if err != nil {
				w.markBroken()
				if !w.sleep(backoff) {
					return
				}
				backoff = nextBackoff(backoff)
				continue
			}
		}

		if w.session(client) {
			backoff = time.Second // a session that ran resets the backoff
		}
		// The session ended: the stream closed, the connection failed, or the
		// server ended it. Whatever the cause, events may have been missed, so
		// the next read goes back to the query.
		w.markBroken()
		client = nil
		if !w.sleep(backoff) {
			return
		}
		backoff = nextBackoff(backoff)
	}
}

// session runs one subscription to completion, returning whether it delivered
// anything at all (used only to decide whether to reset the backoff).
func (w *archiveWatcher) session(client *dbrpc.Client) bool {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// Tail from the current head rather than replaying: the backlog belongs to
	// the cursor-based query, which already knows where it left off, and a
	// replay of a retained archive could be millions of records the daemon
	// would only discard.
	head, err := client.WatchHead(ctx, w.table)
	if err != nil {
		w.logf("watch %s: cannot read the change-log head: %v", w.table, err)
		return false
	}
	ch, stop, err := client.WatchTable(ctx, w.table, head)
	if err != nil {
		w.logf("watch %s: subscribe failed: %v", w.table, err)
		return false
	}

	w.mu.Lock()
	if !w.running {
		w.mu.Unlock()
		stop()
		return false
	}
	w.stop = stop
	w.mu.Unlock()

	w.logf("watch %s: tailing", w.table)
	return w.consume(ch)
}

// consume folds a subscription's events into the buffer, returning whether the
// stream delivered anything. Separated from session so it can be driven from a
// channel in a test: the decision this makes -- which kinds leave the tail
// trustworthy -- is the whole safety property, and it would otherwise only be
// reachable through a live database.
func (w *archiveWatcher) consume(ch <-chan dbrpc.WatchEvent) bool {
	delivered := false
	for ev := range ch {
		delivered = true
		switch ev.Kind {
		case watchUpsert:
			w.append(ev.AdText)
		case watchSynced:
			// Nothing to do: this subscription started at the head, so there was
			// no catch-up phase to mark the end of.
		case watchReset, watchResync, watchDelete:
			// Reset and resync both mean the stream cannot be treated as a
			// continuous record of what changed. A delete is not something an
			// append-only archive should produce at all; if one appears,
			// distrust the stream rather than guess what it meant.
			w.markBroken()
			w.dropBuffer()
		}
	}
	return delivered
}

func (w *archiveWatcher) append(row string) {
	if row == "" {
		return
	}
	w.mu.Lock()
	defer w.mu.Unlock()
	if len(w.rows) >= maxBufferedRows {
		// Past the cap the buffer is no longer cheaper than a query, and holding
		// more of it only costs memory.
		w.rows = nil
		w.broken = true
		return
	}
	w.rows = append(w.rows, row)
}

func (w *archiveWatcher) markBroken() {
	w.mu.Lock()
	w.broken = true
	w.mu.Unlock()
}

func (w *archiveWatcher) dropBuffer() {
	w.mu.Lock()
	w.rows = nil
	w.mu.Unlock()
}

// sleep waits, returning false if the watcher was closed while waiting.
func (w *archiveWatcher) sleep(d time.Duration) bool {
	t := time.NewTimer(d)
	defer t.Stop()
	<-t.C
	w.mu.Lock()
	defer w.mu.Unlock()
	return w.running
}

func nextBackoff(d time.Duration) time.Duration {
	d *= 2
	if d > 30*time.Second {
		d = 30 * time.Second
	}
	return d
}
