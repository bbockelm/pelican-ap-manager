# Watching htcondordb instead of polling it

## Where we are

`pollOnce` runs every `PELICAN_MANAGER_POLL_INTERVAL` (default 30s) and reads three
sources:

| Source | Table | Kind | How it is read today |
|---|---|---|---|
| live queue | `jobs` | mutable | `Mirror.Sync` — whole-queue query |
| completed jobs | `history` | archive | `FetchJobEpochs(cursor, cutoff)` |
| transfers | `epoch_history` | archive | `FetchTransferEpochs(cursor, cutoff)` |

Two of the three are already incremental: they carry a cursor (the last epoch id seen)
and ask only for what is newer. So the question watch answers is not "how do we avoid
re-reading everything" — that is already solved — but "how do we stop asking".

## What the database offers

`dbrpc` has a remote watch (`opWatch`/`opWatchHead`, `Client.Watch(ctx, cursor)`), and
the server resolves it uniformly across a mutable table, a materialized-view backing and
an append-only archive. All three of our sources are watchable through one API.

The contract, from `collections/docs/WATCH.md`:

- **Kinds**: `Upsert`, `Delete`, `Reset`, `Synced`, `Resync`.
- **Cursor**: opaque, carried on live events. Catch-up events carry none — the client
  persists at `Synced`. The client advances the cursor only after durably processing an
  event, which is what makes at-least-once hold.
- **Empty cursor** replays the retained records, then goes live.
- **A cursor older than retention** yields `Reset`: discard derived state, an
  authoritative snapshot follows, ending at `Synced`.
- **`Resync`** means the live stream fell behind; reconnect with the last persisted
  cursor. A slow client is *demoted* rather than being allowed to block writers.
- **At-least-once, not exactly-once. Duplicates are expected.**
- **No global order across shards** — per-shard ordered, interleaved.

## The blocker: our ingest is not idempotent

This is the finding that shapes everything below, and it is not visible from the watch
API — it is a property of our own code that polling has been hiding.

A cursor-based poll delivers each record exactly once: the cursor moves past a record
only once it has been read, and the next poll asks for strictly newer ones. Ingest was
written against that guarantee and takes full advantage of it:

- `State.Update` accumulates — `Successes++`, `SuccessBytes += bytes`. A record applied
  twice **doubles that transfer's bytes**.
- `State.AppendEpochBuckets` appends unconditionally. It writes `EpochIndex` but never
  reads it, so a repeated epoch lands in the bucket twice.
- `State.AppendJobEpoch` assigns into a map keyed by epoch, so it is already idempotent.

Under watch, duplicates are not an edge case to be defended against — they are the
documented contract, produced routinely by any reconnect or demotion. The failure mode is
the bad kind: no error, no log line, just transfer rates that read high. Those rates are
what the control loop throttles on, so the daemon would be reacting to inflated numbers
and enforcing limits nobody earned.

**Making ingest idempotent is a prerequisite, not a follow-up.** It is also worth doing on
its own merits: `EpochIndex` already exists and is already maintained, so the dedupe is a
lookup we are paying to build and then not using.

## The second constraint: watch cannot be the only path

Reads are gated on htcondordb's sync health (`internal/dbready`). When the mirror falls
behind, or reports a durability gap, or cannot be reached, reads fall back to the schedd.
That fallback is polled and has to stay polled — the schedd offers no watch.

So watch does not replace the poll loop. It accelerates one of two paths, and the daemon
must be able to move between them at any time. Concretely, a watch design has to answer:

- The gate closes while a watch is running. Stop the watch, or keep it running and ignore
  it? (Keep it: a watch that is behind is exactly what `Resync`/catch-up is for, and
  tearing it down means re-entering catch-up when the gate reopens.)
- The gate reopens after a period of schedd reads. The persisted cursor is now older than
  what was ingested from the schedd, so replaying from it produces records we already
  have. This is the duplicate case again, and it is routine rather than exceptional —
  another reason the dedupe must be real rather than best-effort.

## Is it worth it?

Honestly weighed, the win is latency, not cost.

**Cost.** The polls are already incremental. `FetchTransferEpochs` asks for epochs newer
than a cursor, and the archive prunes by zone map, so an idle pool costs one cheap query
per source per 30s. Watch would make that approximately zero — real, but small.

**Latency.** This is the case. Today a transfer can be up to 30s old before the daemon
sees it, and the control loop reacts on the poll after that. For a rate limiter whose
windows are 60s, a 30s blind spot is half a window: the difference between throttling
a burst while it is happening and throttling it after it is over.

**What it costs us.** A long-lived subscription per source, cursor persistence, reset and
resync handling, and idempotent ingest. The first four are mechanical. The fifth is a
correctness change to the hottest path in the daemon.

The recommendation is to do it, but to sequence it so the risky part lands first and
independently, where it can be verified on its own.

## Transport

No new connection is needed. `mirrorClient` already caches a `dbrpc.Client` over a
long-lived authenticated CEDAR stream and re-dials on failure, resolving the address each
time (htcondordb comes back under a different shared-port socket name after a restart, so
a cached address is stale exactly when reconnecting matters). dbrpc multiplexes by request
id and `opWatch` is explicitly a long-lived stream under one id, so the subscription and
the ordinary queries share the connection.

The consequence to design for: when that connection drops, the cached client is torn down
and the stream dies with it. The watcher has to notice and re-subscribe from its persisted
cursor. That is the same recovery path as `Resync`, so there is one reconnect story rather
than two — worth building it that way deliberately, because the two failures otherwise
look different and only one of them is exercised in testing.

## Proposed shape

### Stage 1 — idempotent ingest (no watch)

Dedupe by epoch id at the point of application, using the `EpochIndex` we already
maintain. Land and verify this with polling still in place, where duplicates can be
injected deliberately in a test rather than arriving from a reconnect.

The test that matters: apply the same batch twice and assert the summaries are unchanged.
Today that test fails, which makes it a real guard rather than a formality.

### Stage 2 — a watch source behind the same interface

`FetchTransferEpochs`/`FetchJobEpochs` keep their signatures. A watcher runs per archive
source, maintains an in-memory queue of records delivered since the last fetch, and the
fetch drains it. The poll loop still runs; it simply finds records already waiting rather
than issuing a query.

This keeps the change inside the mirror client. The service, the control loop and the
state store do not learn that watch exists, which means the fallback path needs no
parallel implementation — the thing that would otherwise produce two ingest paths that
drift apart.

### Stage 3 — cursors and restart (not done, and not needed)

The plan was to persist each source's cursor and resume from it. Having built stages 2
and 4, the gap it was meant to close turns out to be covered already.

A tail subscribes at the current head with `broken` set, so the first drain after any
subscription falls back to the cursor-based query -- and that query, keyed on the epoch
id the daemon already persists, covers the window between the query and the subscription
going live. A restart is the same case: resubscribe at head, one query, no gap.

So a cursor would save one query per break, not close a hole. Breaks are rare, and the
query is the thing that makes every other gap safe. Left undone deliberately.

### Stage 4 — drive the loop from the stream (done)

An arriving record wakes the read instead of the clock. The signal carries no count and
coalesces, and the send is non-blocking: the sender is the watch pump, and a pump stalled
on a busy consumer stops reading the stream and gets its subscription demoted, which would
make this degrade the thing it speeds up. Early reads are floored at a second, bounding
both the cost on a busy pool and the latency it buys.

One consequence to know: the tail starts on the first read, since that is where the
subscription is established. At a 30s poll nothing is faster for the first 30 seconds
after startup.

**Not yet verified end to end.** The unit tests cover coalescing and non-blocking; the
claim that a record is seen in about a second rather than up to thirty is argued, not
measured. Demonstrating it needs a completed transfer arriving after the tail is live,
because the tailed sources are the two histories -- the live queue is deliberately not
watched.

## Deliberately not doing

- **Watching `jobs`.** The live queue is read whole rather than incrementally, and the
  mirror already serves it in one query. A watch would have to rebuild the queue from
  upserts and deletes, which is a materialised view with its own consistency problems, for
  a read that is not expensive today.
- **The HTTP/SSE changefeed.** It exists (`HTCONDORDB_CHANGEFEED_ADDRESS`) and is aimed at
  non-CEDAR sinks. We already hold an authenticated dbrpc connection to this database;
  opening a second, separately-authenticated transport to the same daemon adds a security
  surface and a second failure mode for no gain.
- **Dropping the poll loop.** It is the schedd fallback path and the backstop if a watch
  wedges. The goal is for polling to find nothing new, not for it to stop.
