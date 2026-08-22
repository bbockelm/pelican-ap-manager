# pelican-ap-manager

Two HTCondor daemons that watch Pelican data movement on an access point and keep it from overwhelming itself.

- **`pelican_man`** reads the schedd's transfer and job history, aggregates it per user / endpoint / site / direction, advertises the result to the collector, and installs **schedd startup limits** so jobs do not start faster than the data can be staged.
- **`pelican_web`** serves the HTTP surface: the sandbox API the Pelican transfer plugin calls, plus the golang-htcondor REST API at `/api/`.

They are separate binaries so `pelican_man` does not link the web stack (OAuth2/OIDC, OpenTelemetry, sqlite) — roughly half its size. Run one or both.

---

## Install

Download the archive for your platform from the [releases page](https://github.com/bbockelm/pelican-ap-manager/releases) and unpack it over `/usr`:

```bash
tar -xzf pelican-ap-manager_<version>_linux_amd64.tar.gz --strip-components=1 -C /usr
```

That puts `pelican_man` and `pelican_web` in `/usr/sbin` — where `condor_master` expects to find daemons — and the docs plus a ready-made drop-in configuration under `/usr/share/doc/pelican-ap-manager/`.

Verify, and check the checksums against `SHA256SUMS.txt` from the same release:

```bash
pelican_man -version
pelican_web -version
```

The archive deliberately does **not** install anything into `/etc/condor/config.d`: dropping a file there would start both daemons on your next `condor_reconfig`, which is your decision rather than the tarball's. Copy the example when you are ready:

```bash
cp /usr/share/doc/pelican-ap-manager/99-pelican-manager.conf /etc/condor/config.d/
```

## Quick start: observe everything, enforce only what you wrote

This is the configuration most sites should start from. The control loop runs, classifies every (user, site) pair, and publishes what it *would* do — but the only limits actually installed are the ones you wrote by hand.

```
# /etc/condor/config.d/99-pelican-manager.conf

PELICAN_MANAGER = /usr/sbin/pelican_man
PELICAN_WEB     = /usr/sbin/pelican_web

# Start them...
DAEMON_LIST = $(DAEMON_LIST) PELICAN_MANAGER PELICAN_WEB

# ...and mark them as DaemonCore daemons. This puts them under the master's
# liveness supervision: it expects a DC_CHILDALIVE heartbeat and kills a daemon
# that stops sending one (see NOT_RESPONDING_TIMEOUT). Both daemons send it.
# The built-in list covers only HTCondor's own daemons, so a third-party one has
# to be added; the leading + appends rather than replacing.
DC_DAEMON_LIST = +PELICAN_MANAGER PELICAN_WEB

# Watch, but do not act on the controller's own conclusions.
PELICAN_MANAGER_ENFORCEMENT_MODE = observing

# ... except these, which are yours and always apply.
PELICAN_MANAGER_RATE_RULES          = ligo_ucsd, psu_all
PELICAN_MANAGER_RATE_RULE_LIGO_UCSD = user=ligo site=UCSD rate=20 window=60s
PELICAN_MANAGER_RATE_RULE_PSU_ALL   = site=PSU-LIGO rate=5 window=2m note="incident 4471"

# Which machine attribute names the execution site in your pool.
PELICAN_MANAGER_SITE_ATTRIBUTE = MachineAttrGLIDEIN_ResourceName0
```

Then:

```bash
condor_reconfig            # picks up the new daemons
condor_restart -daemon MASTER   # if PELICAN_MANAGER/PELICAN_WEB are new to DAEMON_LIST
```

Confirm it took:

```bash
# The daemons are alive and answer DaemonCore commands.
condor_ping -type PELICAN_MANAGER DC_NOP
condor_ping -type PELICAN_WEB     DC_NOP

# What the daemon has decided, and whether each rule is being enforced.
pelican_man -rules

# What is actually installed in the schedd right now.
pelican_man -limits
```

```
$ pelican_man -limits
Startup limits in schedd submit-1@ap.example.org:

NAME                       RATE     ALLOWED  SKIPPED  LAST HIT   EXPRESSION
pelican_static_ligo_ucsd   20/1m0s  431      12       3m14s ago  ((MY.Owner =?= "ligo") && (TARGET.MachineAttrGLIDE…
pelican_static_psu_all     5/2m0s   88       0        never      (TARGET.MachineAttrGLIDEIN_ResourceName0 =?= "PSU-…

SKIPPED counts jobs this limit held back. LAST HIT is when it last did so.
```

You should see one limit per static rule. If you see limits named `pelican_dynamic_*`, you are in `enforcing` mode, not `observing`.

HTCondor itself has no command for this — startup limits live inside the schedd and no `condor_q` or `condor_status` option lists them — which is why `pelican_man` provides it.

When you are ready to let the controller act:

```
PELICAN_MANAGER_ENFORCEMENT_MODE = enforcing
```

---

## Rate rules

A **rule** caps how fast jobs matching a selector may start. Rules have an **origin**, and the origin decides whether the enforcement mode applies to them:

| Origin | Comes from | `observing` | `enforcing` |
|---|---|:---:|:---:|
| `static` | you, in configuration or the rule store | installed | installed |
| `dynamic` | the control loop, from observed transfer performance | withheld | installed |

That asymmetry is the feature: you can evaluate the controller against a real workload for weeks without giving it the throttle, while a handful of hand-written limits stay in force the whole time.

### Declaring a static rule

Name your rules in a list macro, then give each one a body. The body macro is the rule name upper-cased:

```
PELICAN_MANAGER_RATE_RULES            = ligo_ucsd, gpu_burst
PELICAN_MANAGER_RATE_RULE_LIGO_UCSD   = user=ligo site=UCSD rate=20 window=60s
PELICAN_MANAGER_RATE_RULE_GPU_BURST   = expr=(JOB.RequestGpus > 0) rate=2 window=30s note="staging is the bottleneck"
```

One macro per rule rather than one macro holding all of them, so a rule can be dropped in (or pulled out) from its own `config.d` file.

### Rule body syntax

| Key | Meaning |
|---|---|
| `user=<owner>` | Job owner. `*` or omitted matches any. |
| `site=<site>` | Execution site, matched against `PELICAN_MANAGER_SITE_ATTRIBUTE`. `*` or omitted matches any. |
| `sources=<a,b>` | Only jobs whose `PelicanInputPrefixes` names one of these origins. |
| `rate=<n>` | Jobs allowed per window. **`rate=0` counts matching starts without blocking any** — useful for sizing a limit before committing to a number. |
| `window=<dur>` | Rate window; default `60s`, which matches the negotiation cadence. |
| `expires=<dur>` | Retire the rule this long from now. Default: never. |
| `disabled=<bool>` | Keep the rule on file without installing it. |
| `expr=<classad>` | Raw ClassAd expression, replacing `user`/`site`/`sources` entirely. |
| `note="..."` | Free-form commentary, carried into the store, the logs and the ads. |

Values with spaces must be quoted: `note="incident 4471"`.

**A malformed rule stops the daemon.** That is deliberate: an admin who believes a limit is in force when a typo silently dropped it is worse off than one whose daemon refuses to start. Check the log if `pelican_man` will not come up.

### Examples

```
# One user at one site.
user=ligo site=UCSD rate=20 window=60s

# Every user at a site that is having a bad day; expires on its own.
site=PSU-LIGO rate=5 window=2m expires=48h note="ticket 4471"

# One user everywhere.
user=cms rate=100 window=60s

# Only jobs pulling from a specific origin.
user=ligo sources=osdf://ospool/ligo rate=10

# Count without throttling, to size a future limit.
site=UNL rate=0 note="measuring before we pick a number"

# Anything the selectors cannot express.
expr=(JOB.RequestMemory > 32768 && TARGET.HasSingularity) rate=4

# Match everything, deliberately. A rule with no selector at all is rejected;
# you have to say "*" so it cannot happen by accident.
user=* rate=500 window=60s
```

### Limits are leases, not settings

A rule you declare is permanent; the schedd limit derived from it is not. Every limit `pelican_man` installs carries a short lease — 60 seconds by default — and the daemon renews it on every poll cycle.

That is deliberate. These limits exist only because `pelican_man` decided they should, and it is the only thing that can decide they should not. If it dies, a lease means whatever it was throttling returns to full rate within about a minute, rather than staying throttled until somebody notices an unexplained limit in the schedd.

This pairs with `DC_DAEMON_LIST`. A daemon that *hangs* rather than crashes stops sending `DC_CHILDALIVE`, so `condor_master` kills it — and a killed daemon stops renewing, so its limits lapse on schedule. Without that supervision a wedged daemon would sit there indefinitely; with it, the two mechanisms together bound how long a stuck manager can keep throttling an access point.

```
PELICAN_MANAGER_LIMIT_LEASE = 60s
```

Renewal runs on its own timer, at a third of the lease, so two consecutive failures — a schedd restart, a dropped connection — still leave a third of the lease to recover in. It is deliberately independent of the poll and advertise intervals: whatever you set those to, a healthy daemon's limits do not lapse.

One constraint: the schedd clamps any requested lease to `STARTUP_LIMIT_MAX_EXPIRATION` (5 minutes by default) and does so silently, so raising this above that gets you the schedd's maximum rather than what you asked for. The daemon reads that knob at startup and warns if the two disagree.

A lapsed limit is not fatal — the next cycle reinstalls it — but the gap is real, and nothing reports it, which is why the renewal interval is derived from the lease rather than configured separately.

### Where rules live

Rules are persisted so they survive a restart and can be inspected while running. By default that is a JSON document under `SPOOL`:

```
PELICAN_MANAGER_RULE_STORE_PATH = $(SPOOL)/pelican_rate_rules.json
```

Point it at an [htcondordb](https://github.com/bbockelm/htcondordb) instead to keep rules in the pool database, where `htcondordb-cli` and friends can read and edit them:

```
PELICAN_MANAGER_RULE_DB_ADDRESS = htcondordb.example.org:9618
PELICAN_MANAGER_RULE_DB_TABLE   = pelican_rate_rules
```

The store also holds the control loop's own conclusions (as `dynamic` rules), so you can see what it decided even in `observing` mode, and a restart re-adopts them instead of starting cold.

Configuration-declared rules are reconciled on every startup and `condor_reconfig`: declared rules are written, and rules that disappear from the configuration are retired. Rules written directly into the store by other means are left alone.

### Reading history from htcondordb instead of the schedd

Every poll cycle, `pelican_man` reads recent history twice: the completed-job history, and the transfer records. By default it asks the schedd, and neither read is free — the schedd walks its history files backwards, in its own process, while it is also trying to run jobs. On a busy access point this is the most expensive thing the manager does.

If the pool already runs [htcondordb](https://github.com/bbockelm/htcondordb) with `scheddsync` mirroring this schedd, point the manager at the mirror and both reads move off the access point entirely:

```
PELICAN_MANAGER_EPOCH_DB_ADDRESS = htcondordb.example.org:9618
```

The address defaults to `PELICAN_MANAGER_RULE_DB_ADDRESS`, so a site already keeping its rules in htcondordb gets this by setting nothing. Set it to a different address to split the two; leave both unset to read everything from the schedd.

This needs `scheddsync` tailing **both** files — `HISTORY` and `JOB_EPOCH_HISTORY` — because the two reads come from two different places:

| Read | Schedd file | Archive table | Override |
|---|---|---|---|
| Completed jobs | `HISTORY` | `history` | `PELICAN_MANAGER_EPOCH_DB_JOB_TABLE` |
| Transfers | `JOB_EPOCH_HISTORY` | `epoch_history` | `PELICAN_MANAGER_EPOCH_DB_TRANSFER_TABLE` |

There is no separate transfer-history file. `condor_history -transfer-history` reads `JOB_EPOCH_HISTORY` and filters on `EpochAdType` — `INPUT`, `OUTPUT`, `CHECKPOINT` — so the mirrored read applies the same filter. The other record types in that file (`SPAWN`, `EPOCH`) are job-lifecycle records, not transfers, and are excluded.

**It degrades, it does not fail.** If the database is unreachable or returns an error, that cycle falls back to the schedd for whichever read failed, and logs it. The consequence of an outage is the load you were trying to avoid, not a blind control loop.

A mirror is behind the schedd by however long the sync lags. That is fine here — the manager reacts to rolling windows measured in hours — but a mirror that has fallen far behind will make it react to stale data without saying so. If you run one, monitor the sync.

### Where the daemon's own state lives

Separate from the rules, `pelican_man` keeps working state: how far it has read into transfer and job history, the per-bucket transfer summaries, and the capacity the control loop has settled on for each (user, site) pair. Losing it is not fatal but it is not free either — the daemon comes back having forgotten every pair it had classified, and re-reads a whole lookback window of history to rebuild the summaries.

By default it is a JSON document under `SPOOL`:

```
PELICAN_MANAGER_STATE_PATH = $(SPOOL)/pelican_state.json
```

It can go in htcondordb instead, which is what lets a replacement access point pick up where the last one left off:

```
PELICAN_MANAGER_STATE_DB_ADDRESS = htcondordb.example.org:9618
```

As with the history mirror, this defaults to `PELICAN_MANAGER_RULE_DB_ADDRESS`, so one setting covers both.

The state is not stored as one document. It is written on every poll cycle and most of it does not change between cycles, so it is split across rows — one for the read cursors, one per (user, site) pair, one per summary bucket, and one each for the rolling working sets — and only the rows whose contents actually moved are written. That keeps a steady-state save proportional to what changed rather than to how much history the daemon is holding, and it makes the interesting part queryable:

```sql
SELECT PairKey, CapacityGBPerMin FROM pelican_manager_state WHERE Kind == "pair"
```

The rolling working sets stay as JSON payloads. They are the daemon's own scratch, nothing queries them, and giving them attributes would only add a way to drop a field silently.

**A state load failure is fatal.** If the store is configured and cannot be read, `pelican_man` exits rather than starting with an empty state — coming up blank would silently reset the cursors and discard every classification, and it would look exactly like a healthy first start.

---

## Configuration reference

All settings come from HTCondor configuration macros, resolved the same way `condor_config_val` resolves them. `PELICAN_MANAGER.<KEY>` and `-local-name` scoping both work.

### Rate limiting

| Macro | Default | Meaning |
|---|---|---|
| `PELICAN_MANAGER_ENFORCEMENT_MODE` | `enforcing` | `enforcing` installs static and dynamic rules; `observing` installs only static. |
| `PELICAN_MANAGER_RATE_RULES` | — | Comma-separated list of static rule names. |
| `PELICAN_MANAGER_RATE_RULE_<NAME>` | — | The body of one rule (see syntax above). |
| `PELICAN_MANAGER_RULE_STORE_PATH` | `$(SPOOL)/pelican_rate_rules.json` | JSON rule store. |
| `PELICAN_MANAGER_RULE_DB_ADDRESS` | — | htcondordb address; overrides the JSON store. |
| `PELICAN_MANAGER_RULE_DB_TABLE` | `pelican_rate_rules` | Table name in htcondordb. |
| `PELICAN_MANAGER_LIMIT_LEASE` | `60s` | How long an installed limit survives without renewal. Must exceed the poll interval; capped by the schedd's `STARTUP_LIMIT_MAX_EXPIRATION`. |

### Polling and aggregation

| Macro | Default | Meaning |
|---|---|---|
| `PELICAN_MANAGER_POLL_INTERVAL` | `30s` | How often to read new transfer epoch history. |
| `PELICAN_MANAGER_ADVERTISE_INTERVAL` | `1m` | How often to advertise summary ClassAds. |
| `PELICAN_MANAGER_EPOCH_LOOKBACK` | `24h` | How far back to read history at startup. |
| `PELICAN_MANAGER_STATS_WINDOW` | `1h` | Rolling window for in-memory statistics. |
| `PELICAN_MANAGER_DIRECTOR_CACHE_TTL` | `15m` | Cache duration for Pelican director lookups. |

### HTCondor integration

| Macro | Default | Meaning |
|---|---|---|
| `PELICAN_MANAGER_COLLECTOR_HOST` | `COLLECTOR_HOST`, else `localhost:9618` | Collector to advertise to and to locate the schedd through. |
| `PELICAN_MANAGER_SCHEDD_NAME` | `SCHEDD_NAME` | Which schedd to manage. The bare name is fine — the schedd advertises it as `<name>@<fullhostname>` and both forms match. Empty means "the only schedd in the pool". |
| `PELICAN_MANAGER_SITE_ATTRIBUTE` | `MachineAttrGLIDEIN_ResourceName0` | Machine attribute naming the execution site. **Set this to whatever your pool actually uses**, or `site=` selectors will never match. |
| `PELICAN_MANAGER_EPOCH_DB_ADDRESS` | `PELICAN_MANAGER_RULE_DB_ADDRESS` | Read history from an htcondordb mirror instead of the schedd. Falls back to the schedd on any error. |
| `PELICAN_MANAGER_EPOCH_DB_JOB_TABLE` | `history` | Archive table `scheddsync` mirrors the schedd's `HISTORY` file to. |
| `PELICAN_MANAGER_EPOCH_DB_TRANSFER_TABLE` | `epoch_history` | Archive table `scheddsync` mirrors `JOB_EPOCH_HISTORY` to; the transfer records are here. |
| `PELICAN_MANAGER_STATE_DB_ADDRESS` | `PELICAN_MANAGER_RULE_DB_ADDRESS` | Keep the daemon's working state in htcondordb rather than the `SPOOL` JSON file. A load failure is fatal. |
| `PELICAN_MANAGER_STATE_DB_TABLE` | `pelican_manager_state` | Table holding the state rows. |
| `PELICAN_MANAGER_ADDRESS_FILE` | `$(LOG)/.pelican_manager_address` | Where the command address is published. |

### State and logging

| Macro | Default | Meaning |
|---|---|---|
| `PELICAN_MANAGER_STATE_PATH` | `$(SPOOL)/pelican_state.json` | Persisted aggregates and control state. |
| `PELICAN_MANAGER_INFO_PATH` | `$(SPOOL)/pelican_info.json` | The current ClassAds, for local inspection. |
| `PELICAN_MANAGER_LOG` | `$(LOG)/PelicanManagerLog` | Log file. |
| `PELICAN_MANAGER_DEBUG` | — | Log verbosity, e.g. `D_FULLDEBUG` or `cedar:debug`. |

`pelican_web` has its own subsystem scope — `PELICAN_WEB_LOG`, `PELICAN_WEB_DEBUG`, `PELICAN_WEB_ADDRESS_FILE` — so you can turn up its logging without touching the manager. Its other settings keep the `PELICAN_MANAGER_WEB_*` names they have always had; see [docs/WEBSERVER.md](docs/WEBSERVER.md).

---

## Running under `condor_master`

Both daemons are DaemonCore-style HTCondor daemons. Started as root by `condor_master`, they **drop to the condor user** before opening any file they own, so nothing they write is root-owned. `DROP_PRIVILEGES = false` opts out; `CONDOR_IDS` / `CONDOR_USER` select the target identity.

They also participate in the usual lifecycle:

```bash
condor_reconfig -daemon PELICAN_MANAGER   # reload configuration, including rate rules
condor_off      -daemon PELICAN_MANAGER   # graceful shutdown
condor_ping     -type   PELICAN_MANAGER DC_NOP
```

`condor_reconfig` is enough to add, change or retire a static rule — no restart needed.

Running standalone (no `condor_master`) works too, for a quick look:

```bash
pelican_man -oneshot     # one poll/summarize cycle, then exit
```

---

## Monitoring

```bash
# Aggregated transfer statistics.
condor_status -any -constraint 'MyType == "PelicanSummary"'

# Per-(user,site) limit state, including what the controller would do in
# observing mode.
condor_status -any -constraint 'MyType == "PelicanLimit"'

# What is actually installed in the schedd right now, from any source.
pelican_man -limits-all
```

Attribute references: [summary ads](docs/pelican-summary-ad-attributes.md), [limit ads](docs/pelican-limit-ad-attributes.md).

`$(SPOOL)/pelican_info.json` carries the same ClassAd content for local inspection, and `$(SPOOL)/pelican_rate_rules.json` shows the rule set as stored.

---

## Troubleshooting

**No limits appear in `pelican_man -limits`.**
Check `PelicanManagerLog` for `limit manager init error`. The manager locates the schedd through `PELICAN_MANAGER_COLLECTOR_HOST`; if the collector is unreachable or the schedd name matches nothing, rate limiting is disabled and the daemon otherwise runs normally. `PELICAN_MANAGER_SCHEDD_NAME` may be set to a name no schedd advertises.

**A `site=` rule never matches.**
`PELICAN_MANAGER_SITE_ATTRIBUTE` almost certainly does not name the attribute your pool uses. Check a real machine ad: `condor_status -l | grep -i site`.

**`pelican_man` will not start.**
A malformed rate rule is fatal by design. The log names the offending macro and what it could not parse.

**A daemon is killed and restarted for no apparent reason.**
`condor_master` kills a DaemonCore daemon that stops sending `DC_CHILDALIVE`, after `NOT_RESPONDING_TIMEOUT` (an hour by default; `PELICAN_MANAGER_NOT_RESPONDING_TIMEOUT` overrides it per daemon). That is the supervision `DC_DAEMON_LIST` buys, and it is working as intended — something wedged the daemon. Look for what blocked it just before the kill.

**`condor_ping -type PELICAN_MANAGER` cannot reach the daemon.**
Check `DC_DAEMON_LIST` includes it, and read the daemon's own account of how it got its command socket — it logs one of *"accepting shared-port forwarded connections"* (inherited from the master), *"self-registered shared-port endpoint"* (its own entry in `DAEMON_SOCKET_DIR`, which is still reachable), or a plain bind. The address file (`$(LOG)/.pelican_manager_address`) holds whichever address it settled on.

**Transfer plugins cannot register sandboxes.**
`pelican_web` must be in `DAEMON_LIST` and `DC_DAEMON_LIST`; `pelican_man` serves no HTTP. Check `PelicanWebLog` and that `PELICAN_REGISTRATION_SOCKET` is on a short path — a Unix socket path is capped at ~104 bytes.

**Limits appear and disappear.**
The lease is not being renewed often enough. `PELICAN_MANAGER_POLL_INTERVAL` must be shorter than `PELICAN_MANAGER_LIMIT_LEASE`; the daemon logs a warning at startup when it is not. A limit that lapses is reinstalled on the next cycle, so the symptom is intermittent enforcement rather than none.

**Limits exist but nothing is throttled.**
A rule with `rate=0` counts without blocking, by design — `pelican_man -limits` shows those as `monitor only`. Otherwise check the `SKIPPED` column: if it is 0 and `LAST HIT` is `never`, the rule's expression is not matching the jobs you expect. The expression is printed alongside it.

---

## Control algorithm

The dynamic half of the system — how the controller classifies (user, site) pairs green/yellow/red and derives capacity — is described in [docs/control-algorithm.md](docs/control-algorithm.md), along with its tunables.

---

## Building and development

```bash
make build      # both binaries into bin/
make manager    # just pelican_man
make web        # just pelican_web
make test       # unit tests
make vet        # static checks, including build-tagged sources
```

Both binaries take `-version`, stamped from `git describe`.

### Tests

```bash
make test                 # unit tests; no HTCondor needed

# End-to-end against a real HTCondor (condor_master must be on PATH):
go test -tags integration ./integration/...

# The privileged end-to-end test: condor_master starts both daemons as root,
# they drop to the condor user, static rules are installed, and the sandbox
# API serves a registration. Requires root and a condor account.
sudo PELICAN_REQUIRE_ROOT_TEST=1 \
  go test -tags integration -run TestRootStaticRuleEnforcement ./integration/...
```

`PELICAN_REQUIRE_ROOT_TEST=1` turns "cannot run as root" from a skip into a failure, so CI cannot quietly stop covering the privileged path. It runs on every PR in [`.github/workflows/root-integration.yml`](.github/workflows/root-integration.yml).

`TestStaticRuleEnforcement` covers the same rule behavior without root, for a laptop loop.

### Updating test data

Test data is collected from production and sanitized; the sanitized output is excluded from git and must be regenerated:

```bash
make fetch-ap40-sanitized   # collect from ap40.uw.osg-htc.org
make regenerate-golden      # regenerate golden reference files
make update-testdata        # both
```

The redaction process tracks usernames from `Owner`, `AcctGroupUser` and `OsUser`; preserves project names in `AcctGroup` and `ProjectName`; rewrites usernames inside `AccountingGroup` and paths; and keeps a stable `redaction_dict.json` so anonymization is consistent across runs.
