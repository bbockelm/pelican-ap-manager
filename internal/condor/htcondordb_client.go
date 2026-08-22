package condor

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
	"github.com/bbockelm/pelican-ap-manager/internal/state"
)

// The archive tables htcondordb's schedd-sync writes into (see its scheddsync
// manager). Two, because the daemon's two history reads come from two different
// schedd files:
//
//   - FetchJobEpochs reads the schedd's HISTORY file (completed jobs), mirrored
//     to "history".
//   - FetchTransferEpochs reads JOB_EPOCH_HISTORY filtered to the transfer ad
//     types, mirrored to "epoch_history".
//
// There is no separate TRANSFER_HISTORY file: `condor_history
// -transfer-history` sets the record source to JOB_EPOCH and filters on
// EpochAdType, and golang-htcondor's HistorySourceTransfer does the same. Since
// scheddsync's epoch dedup key already includes EpochAdType, the transfer
// records are in the mirror and individually addressable.
const (
	DefaultJobEpochTable = "history"
	DefaultTransferTable = "epoch_history"
)

// transferAdTypes are the EpochAdType values that mark a transfer record. This
// is the mirror-side spelling of the HistoryAdTypeFilter that
// HistorySourceTransfer sends to the schedd; the two must name the same set or
// the mirrored and direct reads see different data.
var transferAdTypes = []string{"INPUT", "OUTPUT", "CHECKPOINT"}

// mirrorClient reads history from an htcondordb mirror of the schedd's files
// instead of from the schedd itself.
//
// Why: every poll, the daemon walks the schedd's history backwards until it
// reaches records it has already seen -- twice, once for completed jobs and once
// for transfers. That work happens inside the schedd, on the access point,
// competing with the thing the access point exists to do. htcondordb's
// schedd-sync already tails the same files into archive tables with zone maps on
// the event time, so the same questions can be asked of a database built to
// answer them -- and, if the operator wants, on a different machine entirely.
//
// Advertising and job queries still go to the schedd: they are about what is
// happening now, which is not what a history mirror holds.
type mirrorClient struct {
	// schedd is the direct client. It serves everything the mirror cannot, and
	// is the fallback when the mirror is unreachable.
	schedd CondorClient

	addr          string
	jobTable      string
	transferTable string
	cfg           *config.Config

	// Ad conversion borrowed from the wrapped schedd client, so the mirrored and
	// direct paths cannot drift in how they read an ad. Nil when the wrapped
	// client is not the schedd client, in which case the mirror declines and the
	// read falls back.
	convertJob      func(*classad.ClassAd) (*JobEpochRecord, state.EpochID)
	convertTransfer func(*classad.ClassAd) ([]TransferRecord, state.EpochID)

	// query runs one archive query. Indirected so a test can see which table
	// each read goes to: the two reads pull different kinds of record out of
	// different tables, and crossing them returns the wrong records rather than
	// an error. Set to queryLocked; the caller holds m.mu.
	query func(ctx context.Context, table, constraint string) ([]string, error)

	mu     sync.Mutex
	client *dbrpc.Client
	conn   *cedarclient.HTCondorClient
	cancel context.CancelFunc
}

// MirrorConfig configures the htcondordb-backed history source.
type MirrorConfig struct {
	// Address is the htcondordb daemon's command address (sinful or host:port).
	Address string
	// JobTable names the archive table holding the mirrored completed-job
	// history. Defaults to DefaultJobEpochTable.
	JobTable string
	// TransferTable names the archive table holding the mirrored epoch history,
	// which is where the transfer records are. Defaults to
	// DefaultTransferTable.
	TransferTable string
	// Config supplies the client security policy, as for any HTCondor client.
	Config *config.Config
}

// NewMirrorClient wraps a schedd-backed client so job-epoch reads go to an
// htcondordb mirror. It does not connect; the first query does, so a database
// that is not up yet cannot stop the daemon from starting.
func NewMirrorClient(direct CondorClient, cfg MirrorConfig) (CondorClient, error) {
	if direct == nil {
		return nil, fmt.Errorf("condor: mirror client needs a direct client to delegate to")
	}
	if strings.TrimSpace(cfg.Address) == "" {
		return nil, fmt.Errorf("condor: htcondordb address is required")
	}
	if cfg.Config == nil {
		return nil, fmt.Errorf("condor: HTCondor configuration is required for the htcondordb client security policy")
	}
	m := &mirrorClient{
		schedd:        direct,
		addr:          cfg.Address,
		jobTable:      orDefault(cfg.JobTable, DefaultJobEpochTable),
		transferTable: orDefault(cfg.TransferTable, DefaultTransferTable),
		cfg:           cfg.Config,
	}
	// Reuse the schedd client's ad conversion, so the mirrored and direct paths
	// cannot drift in how they read an ad. If the wrapped client is something
	// else, there is nothing to reuse; the mirror read then errors and falls
	// back, rather than silently returning no records.
	m.query = m.queryLocked
	if h, ok := direct.(*htcClient); ok {
		m.convertJob = h.convertJobEpochAd
		m.convertTransfer = h.convertTransferAd
	}
	return m, nil
}

func orDefault(v, fallback string) string {
	if v == "" {
		return fallback
	}
	return v
}

// FetchTransferEpochs reads the mirrored epoch history instead of the schedd's,
// filtered to the transfer ad types -- which is exactly what the schedd does for
// a HistorySourceTransfer query, so the two see the same records.
//
// Falls back to the schedd on any error, for the same reason FetchJobEpochs
// does: an outage should cost extra schedd load, not blind the control loop.
func (m *mirrorClient) FetchTransferEpochs(since state.EpochID, cutoff time.Time) ([]TransferRecord, state.EpochID, error) {
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()

	records, newest, err := m.fetchTransfersFromMirror(ctx, since, cutoff)
	if err == nil {
		return records, newest, nil
	}
	return m.schedd.FetchTransferEpochs(since, cutoff)
}

func (m *mirrorClient) fetchTransfersFromMirror(ctx context.Context, since state.EpochID, cutoff time.Time) ([]TransferRecord, state.EpochID, error) {
	m.mu.Lock()
	defer m.mu.Unlock()

	if m.convertTransfer == nil {
		return nil, since, fmt.Errorf("condor: no ad converter for the mirrored transfer history")
	}

	rows, err := m.query(ctx, m.transferTable, transferConstraint(cutoff, since))
	if err != nil {
		return nil, since, err
	}
	return m.decodeTransferRows(rows, since)
}

// decodeTransferRows turns the archive's rows into transfer records, dropping
// anything already counted and reporting the newest epoch seen.
//
// As with decodeRows, the ClusterId bound in the constraint is coarse, so this
// exact filter is what actually makes the read incremental -- and here a replay
// would double-count bytes, not merely repeat work.
func (m *mirrorClient) decodeTransferRows(rows []string, since state.EpochID) ([]TransferRecord, state.EpochID, error) {
	var (
		records []TransferRecord
		newest  = since
	)
	for _, row := range rows {
		ad, err := classad.ParseOld(row)
		if err != nil {
			return nil, since, fmt.Errorf("parsing a mirrored transfer ad: %w", err)
		}

		id := epochFromAd(ad)
		if !since.IsZero() && !id.After(since) {
			continue
		}
		if id.After(newest) {
			newest = id
		}
		// One ad can yield several records (an input and an output leg, say),
		// which is why this borrows the schedd client's conversion rather than
		// reimplementing it.
		recs, _ := m.convertTransfer(ad)
		records = append(records, recs...)
	}
	return records, newest, nil
}

// Delegated: the mirror holds history, and these are about the present.
func (m *mirrorClient) AdvertiseClassAds(payload []map[string]any) error {
	return m.schedd.AdvertiseClassAds(payload)
}
func (m *mirrorClient) QueryJobs(ctx context.Context, constraint string, projection []string) ([]*classad.ClassAd, error) {
	return m.schedd.QueryJobs(ctx, constraint, projection)
}
func (m *mirrorClient) LocateSchedd(ctx context.Context) (*htcondor.Schedd, error) {
	return m.schedd.LocateSchedd(ctx)
}

// FetchJobEpochs reads the mirrored history instead of the schedd's.
//
// A mirror that cannot be reached falls back to the schedd rather than
// returning nothing: a database outage should cost the pool some extra schedd
// load, not blind the control loop.
func (m *mirrorClient) FetchJobEpochs(since state.EpochID, cutoff time.Time) ([]JobEpochRecord, state.EpochID, error) {
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()

	records, newest, err := m.fetchFromMirror(ctx, since, cutoff)
	if err == nil {
		return records, newest, nil
	}
	return m.schedd.FetchJobEpochs(since, cutoff)
}

func (m *mirrorClient) fetchFromMirror(ctx context.Context, since state.EpochID, cutoff time.Time) ([]JobEpochRecord, state.EpochID, error) {
	m.mu.Lock()
	defer m.mu.Unlock()

	if m.convertJob == nil {
		return nil, since, fmt.Errorf("condor: no ad converter for the mirrored history")
	}

	rows, err := m.query(ctx, m.jobTable, mirrorConstraint(cutoff, since))
	if err != nil {
		return nil, since, err
	}
	return m.decodeRows(rows, since)
}

// queryLocked runs one archive query, dropping the session on failure so the
// next call redials. The caller holds m.mu.
func (m *mirrorClient) queryLocked(ctx context.Context, table, constraint string) ([]string, error) {
	client, err := m.connectLocked(ctx)
	if err != nil {
		return nil, err
	}
	rows, err := client.ArchiveQuery(ctx, table, constraint, 0)
	if err != nil {
		m.dropLocked()
		return nil, fmt.Errorf("querying %s in htcondordb at %s: %w", table, m.addr, err)
	}
	return rows, nil
}

// decodeRows turns the archive's rows into records, dropping anything already
// accounted for and reporting the newest epoch seen.
//
// The ClusterId bound in the constraint is coarse -- it admits every proc and
// run instance of the last-seen cluster, including ones already processed -- so
// this second, exact filter is what actually makes the read incremental. The
// schedd path gets the same guarantee from iterating backwards and stopping.
func (m *mirrorClient) decodeRows(rows []string, since state.EpochID) ([]JobEpochRecord, state.EpochID, error) {
	var (
		records []JobEpochRecord
		newest  = since
	)
	for _, row := range rows {
		// The archive streams the old-ClassAd text form.
		ad, err := classad.ParseOld(row)
		if err != nil {
			return nil, since, fmt.Errorf("parsing a mirrored history ad: %w", err)
		}

		id := epochFromAd(ad)
		if !since.IsZero() && !id.After(since) {
			continue
		}
		if id.After(newest) {
			newest = id
		}
		if rec, _ := m.convertJob(ad); rec != nil {
			records = append(records, *rec)
		}
	}
	return records, newest, nil
}

// mirrorConstraint selects the records worth transferring. It is the inverse of
// the schedd path's stop condition: iterating a file backwards stops once it
// passes the cutoff, whereas a query says what to include.
//
// Both bounds matter. EpochWriteDate is zone-mapped in the archive, so the
// cutoff prunes whole segments. The ClusterId bound is what keeps a steady-state
// poll cheap: without it every cycle would pull the whole lookback window --
// hours of history, every 30 seconds -- only to discard nearly all of it
// client-side. EpochID orders on (ClusterId, ProcId, RunInstanceID), so no ad
// below the last-seen ClusterId can be new, which makes this bound exactly
// equivalent to the filter it replaces rather than an approximation of it.
func mirrorConstraint(cutoff time.Time, since state.EpochID) string {
	var parts []string
	if !cutoff.IsZero() {
		parts = append(parts, fmt.Sprintf("EpochWriteDate >= %d", cutoff.Unix()))
	}
	if !since.IsZero() {
		parts = append(parts, fmt.Sprintf("ClusterId >= %d", since.ClusterID))
	}
	if len(parts) == 0 {
		return "true"
	}
	return strings.Join(parts, " && ")
}

// transferConstraint is mirrorConstraint plus the ad-type filter that makes an
// epoch-history query a transfer-history query.
//
// The epoch archive holds every kind of run-instance record -- SPAWN, EPOCH, and
// the transfer legs. Without the filter the daemon would feed job-lifecycle
// records to the transfer conversion, which would read them as transfers with no
// bytes and no endpoint.
func transferConstraint(cutoff time.Time, since state.EpochID) string {
	quoted := make([]string, 0, len(transferAdTypes))
	for _, t := range transferAdTypes {
		quoted = append(quoted, fmt.Sprintf("EpochAdType == %q", t))
	}
	return mirrorConstraint(cutoff, since) + " && (" + strings.Join(quoted, " || ") + ")"
}

func (m *mirrorClient) connectLocked(ctx context.Context) (*dbrpc.Client, error) {
	if m.client != nil {
		return m.client, nil
	}

	sec, err := htcondor.GetSecurityConfig(m.cfg, command.DBSession, "CLIENT")
	if err != nil {
		return nil, fmt.Errorf("building client security config: %w", err)
	}
	sec.Command = command.DBSession
	// Prefer rather than require authentication, matching htcondordb-cli: reads
	// are all this needs, and an unauthenticated peer still gets them.
	if sec.Authentication == security.SecurityOptional {
		sec.Authentication = security.SecurityPreferred
	}

	sessCtx, cancel := context.WithCancel(context.Background())
	dialCtx, dialCancel := context.WithTimeout(ctx, 30*time.Second)
	defer dialCancel()

	// Resolved per dial, not once at startup: an htcondordb that restarts comes
	// back under a different shared-port socket name, so a cached address would
	// be stale exactly when reconnecting matters.
	addr, err := dbaddr.Resolve(m.addr, m.cfg)
	if err != nil {
		cancel()
		return nil, err
	}

	conn, err := cedarclient.ConnectAndAuthenticate(dialCtx, addr, sec)
	if err != nil {
		cancel()
		return nil, fmt.Errorf("connecting to htcondordb at %s: %w", addr, err)
	}

	m.client = dbrpc.NewClient(dbrpc.NewCedarConn(sessCtx, conn.GetStream()))
	m.conn, m.cancel = conn, cancel
	return m.client, nil
}

func (m *mirrorClient) dropLocked() {
	if m.client != nil {
		_ = m.client.Close()
	}
	if m.cancel != nil {
		m.cancel()
	}
	m.client, m.conn, m.cancel = nil, nil, nil
}

// MirrorTables reports the tables a MirrorConfig resolves to, so a caller can
// log which ones are in use without duplicating the defaults.
func MirrorTables(cfg MirrorConfig) (jobTable, transferTable string) {
	return orDefault(cfg.JobTable, DefaultJobEpochTable), orDefault(cfg.TransferTable, DefaultTransferTable)
}
