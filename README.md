# pelican-ap-manager

Two HTCondor daemons that watch Pelican data movement on an access point and keep it from overwhelming itself.

- **`pelican_man`** reads the schedd's transfer and job history, aggregates it per user / endpoint / site / direction, advertises the result to the collector, and installs **schedd startup limits** so jobs do not start faster than the data can be staged.
- **`pelican_web`** serves the HTTP surface: the sandbox API the Pelican transfer plugin calls, plus the golang-htcondor REST API at `/api/`.

They are separate binaries so `pelican_man` does not link the web stack (OAuth2/OIDC, OpenTelemetry, sqlite) — roughly half its size. Run one or both.

---

## Quick start: observe everything, enforce only what you wrote

This is the configuration most sites should start from. The control loop runs, classifies every (user, site) pair, and publishes what it *would* do — but the only limits actually installed are the ones you wrote by hand.

```
# /etc/condor/config.d/50-pelican-manager.conf

PELICAN_MANAGER = /usr/sbin/pelican_man
PELICAN_WEB     = /usr/sbin/pelican_web
DAEMON_LIST     = $(DAEMON_LIST) PELICAN_MANAGER PELICAN_WEB

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

# Your rules are installed in the schedd.
condor_q -limits
```

You should see two limits, `pelican_static_ligo_ucsd` and `pelican_static_psu_all`. If you see limits named `pelican_dynamic_*`, you are in `enforcing` mode, not `observing`.

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

# What is actually installed in the schedd right now.
condor_q -limits
```

Attribute references: [summary ads](docs/pelican-summary-ad-attributes.md), [limit ads](docs/pelican-limit-ad-attributes.md).

`$(SPOOL)/pelican_info.json` carries the same ClassAd content for local inspection, and `$(SPOOL)/pelican_rate_rules.json` shows the rule set as stored.

---

## Troubleshooting

**No limits appear in `condor_q -limits`.**
Check `PelicanManagerLog` for `limit manager init error`. The manager locates the schedd through `PELICAN_MANAGER_COLLECTOR_HOST`; if the collector is unreachable or the schedd name matches nothing, rate limiting is disabled and the daemon otherwise runs normally. `PELICAN_MANAGER_SCHEDD_NAME` may be set to a name no schedd advertises.

**A `site=` rule never matches.**
`PELICAN_MANAGER_SITE_ATTRIBUTE` almost certainly does not name the attribute your pool uses. Check a real machine ad: `condor_status -l | grep -i site`.

**`pelican_man` will not start.**
A malformed rate rule is fatal by design. The log names the offending macro and what it could not parse.

**Transfer plugins cannot register sandboxes.**
`pelican_web` must be in `DAEMON_LIST`; `pelican_man` serves no HTTP. Check `PelicanWebLog` and that `PELICAN_REGISTRATION_SOCKET` is on a short path — a Unix socket path is capped at ~104 bytes.

**Limits exist but nothing is throttled.**
A rule with `rate=0` counts without blocking, by design. Also confirm the rule's expression matches the jobs you expect: `condor_q -limits -long` shows it.

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
