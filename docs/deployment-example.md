# A worked deployment: `pelican-man` + htcondordb on an access point

One complete setup, start to finish, with the check to run after each step so a
failure is caught where it happens rather than three steps later.

No web interface. `pelican-web` serves the sandbox API for Pelican transfer
plugins; if you are not using that yet, leave it out entirely — `pelican-man`
does not need it.

Assumptions: an existing HTCondor access point (schedd + collector reachable),
`condor` as the daemon user, and `USE_SHARED_PORT = True` (the default). Paths
below are the RHEL-family defaults; substitute your own.

---

## 1. Install

```bash
# From the release page, matching your architecture.
tar -tzf pelican-ap-manager_v0.1.0_linux_amd64.tar.gz | head
sha256sum -c SHA256SUMS.txt --ignore-missing

# The archive mirrors an install prefix, so this is the whole install.
sudo tar -xzf pelican-ap-manager_v0.1.0_linux_amd64.tar.gz \
  --strip-components=1 -C /usr/local
```

That gives you:

```
/usr/local/sbin/pelican-man
/usr/local/sbin/pelican-web                       # unused here
/usr/local/share/doc/pelican-ap-manager/...       # this file, README, example config
```

htcondordb is a separate project; put its binaries alongside:

```
/usr/local/sbin/htcondordb
/usr/local/bin/htcondordb-cli
```

**Check:**

```bash
/usr/local/sbin/pelican-man -version
/usr/local/sbin/htcondordb -version
```

---

## 2. Let the schedd write epoch history

Transfer records live in `JOB_EPOCH_HISTORY` — there is no separate
transfer-history file — and that is what both the schedd's own
`condor_history -transfer-history` and htcondordb read. If your schedd is not
already writing it, nothing will land in the `epoch_history` table.

Skip this step if you only want completed-job history from the database;
`pelican-man` will keep asking the schedd for transfers.

It is bounded by default: `MAX_EPOCH_HISTORY_LOG` is 20 MB across
`MAX_EPOCH_HISTORY_ROTATIONS = 2`, so about 60 MB. It is still new write traffic
on the AP, which is why it is a deliberate step rather than something the manager
turns on for you.

The setting itself (`JOB_EPOCH_HISTORY`) is in the config file in step 3.
Check whether your schedd already writes it:

```bash
condor_config_val JOB_EPOCH_HISTORY
condor_history -transfer-history -limit 1
```

---

## 3. The configuration

`/etc/condor/config.d/99-pelican-manager.conf`. Nothing here restates a default —
every line is either a path, a decision, or something this pool actually needs:

```conf
# ---------------------------------------------------------------------------
# Daemons
# ---------------------------------------------------------------------------

PELICAN_MANAGER = /usr/local/sbin/pelican-man
HTCONDORDB      = /usr/local/sbin/htcondordb

DAEMON_LIST    = $(DAEMON_LIST), HTCONDORDB, PELICAN_MANAGER
DC_DAEMON_LIST = $(DC_DAEMON_LIST) +HTCONDORDB +PELICAN_MANAGER

# ---------------------------------------------------------------------------
# Keep condor_preen from deleting what these daemons put in SPOOL
# ---------------------------------------------------------------------------
#
# VALID_SPOOL_FILES is the list of files condor_preen leaves alone; anything in
# SPOOL that is not named here is eventually removed. Three things land there:
# htcondordb's database directory, the persisted session caches, and
# pelican-man's info file. Omitting this looks fine for days and then quietly
# destroys the database.
VALID_SPOOL_FILES = $(VALID_SPOOL_FILES) htcondordb sessions_* pelican_info.json

# ---------------------------------------------------------------------------
# htcondordb: mirror this schedd into the database
# ---------------------------------------------------------------------------

# Tails job_queue.log -> "jobs", history -> "history", JOB_EPOCH_HISTORY ->
# "epoch_history".
HTCONDORDB_SYNC_SCHEDD = true

# The file paths default to the schedd's own $(JOB_QUEUE_LOG), $(HISTORY) and
# $(JOB_EPOCH_HISTORY), which is usually all you need. If yours are not where
# HTCondor's defaults say -- a separate job_queue directory is common -- scope
# the override to this daemon with the HTCONDORDB. prefix so the schedd's own
# setting is untouched:
#
#   HTCONDORDB.JOB_QUEUE_LOG = /var/lib/condor/job_queue/job_queue.log

# Transfer records live in JOB_EPOCH_HISTORY -- there is no separate
# transfer-history file -- so this is what fills the epoch_history table that
# pelican-man reads transfers from. See step 2 before enabling it.
JOB_EPOCH_HISTORY = $(SPOOL)/epoch_history

# ---------------------------------------------------------------------------
# pelican-man
# ---------------------------------------------------------------------------

# The default is enforcing. Start by observing: run the control loop and publish
# what it concludes, while installing only the static rules below.
PELICAN_MANAGER_ENFORCEMENT_MODE = observing

# Read history from htcondordb instead of the schedd, and keep the rules and the
# daemon's working state there too.
#
# One line covers all three: the epoch and state addresses default to the rule
# address. "auto" locates the local htcondordb the way its own clients do -- via
# the address file it publishes -- and is re-resolved on every connection, so
# neither daemon restarting strands the other. Nothing to write down, and no
# dependence on the port or the socket name.
PELICAN_MANAGER_RULE_DB_ADDRESS = auto

# Your static rules. These apply in BOTH modes, so this is how to throttle
# something without handing the control loop the keys.
PELICAN_MANAGER_RATE_RULES          = ligo_ucsd
PELICAN_MANAGER_RATE_RULE_LIGO_UCSD = user=ligo site=UCSD rate=20 window=60s note="ticket 4471"

# Which machine attribute names the execution site in YOUR pool. This is the
# default, and it is here anyway because it is the setting most likely to be
# wrong for a given pool -- and a site= selector naming the wrong attribute
# matches nothing and says nothing about it. Check it (step 4).
PELICAN_MANAGER_SITE_ATTRIBUTE = MachineAttrGLIDEIN_ResourceName0

# ---------------------------------------------------------------------------
# Worth having
# ---------------------------------------------------------------------------

# Persist the CEDAR session cache so clients resume across a daemon restart
# instead of all re-authenticating at once. Off by default. It needs SPOOL and
# the pool signing keys (it encrypts the cache at rest), and it is fatal rather
# than quietly skipped if either is missing -- which is why the sessions_* entry
# above matters.
SEC_PERSIST_SESSIONS = True

# htcondordb can serve Prometheus metrics, and pprof for when it is the thing
# misbehaving. Scoped to the daemon, and bound to localhost.
# HTCONDORDB.HTCONDORDB_METRICS_ADDRESS = localhost:9721
# HTCONDORDB.HTCONDORDB_ENABLE_PPROF    = true

# Log paths are not set: each daemon defaults to $(LOG)/<CamelCase subsystem>Log
# -- PelicanManagerLog and HtcondordbLog. Turn either up when needed:
# PELICAN_MANAGER_DEBUG = D_FULLDEBUG
# HTCONDORDB_DEBUG      = general:debug cedar:debug

# USE_SHARED_PORT is not set either. It has defaulted to true since HTCondor
# 7.5.0, and asserting it in a 99- drop-in would override a site that turned it
# off deliberately. Both daemons work either way, and "auto" above does not care.
```

**Check** — that it parses and the values are what you meant, before restarting
anything:

```bash
condor_config_val -verbose PELICAN_MANAGER_RULE_DB_ADDRESS
condor_config_val DAEMON_LIST DC_DAEMON_LIST VALID_SPOOL_FILES
```

Do not expect `condor_config_val PELICAN_MANAGER_EPOCH_DB_ADDRESS` to show
anything — it will say *Not defined*, and that is correct. The epoch and state
addresses fall back to the rule address inside `pelican-man`, not in the
HTCondor macro table. The daemon's startup log is where you confirm all three
resolved (step 6).

Everything else has a default that fits: the poll and advertise intervals, the
lookback and stats windows, the state and info paths, the limit lease. Setting
them to their own values only makes the file longer.

---

## 4. Find your site attribute

Getting this wrong is the most common way to end up with rules that silently
never match:

```bash
condor_status -l | grep -i site
```

Pick the attribute that actually carries the site name and set
`PELICAN_MANAGER_SITE_ATTRIBUTE` to it. Common values are
`MachineAttrGLIDEIN_ResourceName0` and `MachineAttrGLIDEIN_Site0`.

---

## 5. Start, and confirm the address

A `condor_reconfig` is **not** enough — `DC_DAEMON_LIST` is only read when
daemons start:

```bash
sudo condor_restart -master
```

**Check** — both daemons are up and htcondordb has published an address:

```bash
# Both running, and as condor rather than root -- they drop privileges at startup.
ps -o user,args -C htcondordb -C pelican-man

# The master's account of starting them, if either is missing.
grep -iE "pelican_manager|htcondordb" /var/log/condor/MasterLog | tail

cat /var/log/condor/.htcondordb_address
```

The address file is what `auto` resolves through, so its contents matter but its
*form* does not — whatever socket name is in there, `pelican-man` will use it,
and will re-read it if htcondordb restarts under a different one.

By default that name carries the *master's* pid — `htcondordb_<masterpid>_<hex>`
— so it changes whenever `condor_master` restarts. That is why `auto` reads the
file rather than a literal address.

If you would rather write the address down, HTCondor will use a name you choose
verbatim:

```conf
HTCONDORDB_ARGS = -sock htcondordb
```

which makes the socket exactly `htcondordb` and the address constructible as
`<$(FULL_HOSTNAME):9618?sock=htcondordb>` — the same mechanism behind HTCondor's
own collector socket being just `collector`. Both forms work; `auto` is one line
and does not care about the port, so it is what this example uses.

```bash
condor_ping -type PELICAN_MANAGER DC_NOP
```

---

## 6. Confirm each layer

**htcondordb is syncing.** The tables fill as the tailers catch up:

```bash
htcondordb-cli -e "SELECT COUNT(*) FROM jobs"
htcondordb-cli -e "SELECT COUNT(*) FROM history"
htcondordb-cli -e "SELECT COUNT(*) FROM epoch_history"
```

If a query cannot find the daemon, read `$(LOG)/HtcondordbLog` — that is the
default log name, derived from the subsystem `HTCONDORDB`.

`epoch_history` stays at 0 until jobs finish transfers *after* you enabled
`JOB_EPOCH_HISTORY` — it is not backfilled.

**`pelican-man` found the schedd and the database.** In
`/var/log/condor/PelicanManagerLog`, expect:

```
reading history from htcondordb auto (jobs: history, transfers: epoch_history)
rate rule store: htcondordb <...> table pelican_rate_rules
loading state from htcondordb <...> table pelican_manager_state
initialized limit manager for schedd <name>
renewing schedd limit leases every 20s
```

If instead you see *"history mirror unavailable"*, it fell back to the schedd —
which works, but is exactly the load you were trying to move off the AP. That
line is logged once per distinct cause rather than once per cycle, and recovery
is logged too (*"mirror is answering again"*), so one line does not mean one
transient blip: check whether a later recovery line follows it.

**The rules and the state are really in the database.** Ask the database, not the
daemon — this is the check that distinguishes "stored" from "the daemon thinks it
stored":

```bash
htcondordb-cli -e 'SELECT RuleName, RuleUser, RateCount FROM pelican_rate_rules'
htcondordb-cli -e 'SELECT Kind, COUNT(*) FROM pelican_manager_state GROUP BY Kind'

# Once the control loop has classified anything, its conclusions are rows:
htcondordb-cli -e 'SELECT PairKey, CapacityGBPerMin FROM pelican_manager_state WHERE Kind == "pair"'
```

**The daemon is writing its local snapshot.** `$(SPOOL)/pelican_info.json` is
written every advertise cycle even when there is nothing to report, so its
presence and mtime are a liveness signal independent of the collector:

```bash
ls -l /var/lib/condor/spool/pelican_info.json
```

**Summaries are being published**, once a poll cycle or two has run:

```bash
condor_status -any -constraint 'MyType == "PelicanSummary"'
condor_status -any -constraint 'MyType == "PelicanLimit"'
```

**Nothing is being enforced yet.** No static rules are declared yet and
observing mode withholds the dynamic ones, so both lists are empty — that is the
expected state at this point, not a failure:

```bash
/usr/local/sbin/pelican-man -rules      # what the daemon intends
/usr/local/sbin/pelican-man -limits     # what the schedd is enforcing
```

(Static rules are installed in *both* modes. Observing mode only withholds the
control loop's own conclusions.)

---

## 7. Add a static rule

Static rules apply in **both** modes, so this is how you throttle something
without handing the control loop the keys:

```conf
PELICAN_MANAGER_RATE_RULES          = ligo_ucsd
PELICAN_MANAGER_RATE_RULE_LIGO_UCSD = user=ligo site=UCSD rate=20 window=60s note="ticket 4471"
```

A rule change needs only a reconfigure:

```bash
sudo condor_reconfig -daemon PELICAN_MANAGER
```

**Check:**

```bash
/usr/local/sbin/pelican-man -rules      # ligo_ucsd, origin=static, status=enforced
/usr/local/sbin/pelican-man -limits     # pelican_static_ligo_ucsd, 20/1m
```

`-rules` shows what the daemon decided; `-limits` shows what the schedd is
actually applying. Reading them together is how you tell "not decided yet" from
"decided but not enforced".

---

## 8. Turn on enforcement

Only when the `PelicanLimit` ads look like decisions you would have made:

```conf
PELICAN_MANAGER_ENFORCEMENT_MODE = enforcing
```

```bash
sudo condor_reconfig -daemon PELICAN_MANAGER
/usr/local/sbin/pelican-man -limits
```

You should now also see `pelican_dynamic_*` limits for the (user, site) pairs
the controller has classified.

---

## 9. Backing out

Rate limits are **leases**, renewed every ~20 seconds and expiring within a
minute. So the fastest way to lift everything the manager installed is to stop
the manager:

```bash
sudo condor_off -daemon PELICAN_MANAGER
```

Within about a minute the schedd stops applying its limits. Nothing is left
behind that needs cleaning up — that is what the lease is for.

To go back to publishing without enforcing, set
`PELICAN_MANAGER_ENFORCEMENT_MODE = observing` and reconfigure; the dynamic
limits lapse on their own while the static ones stay.

To remove it entirely, drop `PELICAN_MANAGER` from `DAEMON_LIST` and
`DC_DAEMON_LIST` and `condor_restart -master`.

---

## If something is wrong

See the [Troubleshooting](../README.md#troubleshooting) section. The two
failures specific to this setup:

**`pelican-man` logs a mirror fallback on every cycle.** The address is wrong or
htcondordb is not reachable. Compare your configured value against
`cat $(LOG)/.htcondordb_address`. Reads fall back to the schedd, so the daemon
keeps working — the only symptom is the schedd load you meant to avoid.

**`epoch_history` stays empty.** Either the schedd is not writing
`JOB_EPOCH_HISTORY` (step 2), or no job has finished a transfer since you turned
it on. Confirm with `condor_history -transfer-history -limit 1`.

**The database disappeared, or sessions stop resuming.** `condor_preen` removes
anything in `SPOOL` that `VALID_SPOOL_FILES` does not name, and it runs on a
timer — so this presents as everything working for a day or more and then the
database being gone. Check that `condor_config_val VALID_SPOOL_FILES` includes
`htcondordb` and `sessions_*`.

**`pelican-man` will not start, with a session-cache error.**
`SEC_PERSIST_SESSIONS` is deliberately fatal rather than quietly skipped when its
prerequisites are missing: it needs `SPOOL` set and the pool signing keys
readable, because it encrypts the cache at rest and there is no plaintext
fallback. Remove the setting or fix the prerequisite; it is an optimization, not
a requirement.
