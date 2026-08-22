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
	"github.com/bbockelm/pelican-ap-manager/internal/state"
)

// DefaultJobEpochTable is the archive table htcondordb's schedd-sync writes the
// completed-job history into (see its scheddsync manager).
const DefaultJobEpochTable = "history"

// mirrorClient reads job-epoch records from an htcondordb mirror of the schedd's
// history instead of from the schedd itself.
//
// Why: every poll, FetchJobEpochs walks the schedd's history file backwards
// until it reaches records it has already seen. That work happens inside the
// schedd, on the access point, competing with the thing the access point exists
// to do. htcondordb's schedd-sync already tails the same file into an archive
// table with zone maps on the epoch write time, so the same question can be
// asked of a database that is built to answer it -- and, if the operator wants,
// on a different machine entirely.
//
// Everything else still goes to the schedd. Transfer epochs in particular: they
// come from TRANSFER_HISTORY, and nothing mirrors that file today, so
// FetchTransferEpochs is delegated unchanged.
type mirrorClient struct {
	// schedd is the direct client. It serves everything the mirror cannot, and
	// is the fallback when the mirror is unreachable.
	schedd CondorClient

	addr  string
	table string
	cfg   *config.Config

	// convert turns a mirrored ad into a record. Borrowed from the wrapped
	// schedd client; nil when there is nothing to borrow.
	convert func(*classad.ClassAd) (*JobEpochRecord, state.EpochID)

	mu     sync.Mutex
	client *dbrpc.Client
	conn   *cedarclient.HTCondorClient
	cancel context.CancelFunc
}

// MirrorConfig configures the htcondordb-backed epoch source.
type MirrorConfig struct {
	// Address is the htcondordb daemon's command address (sinful or host:port).
	Address string
	// Table names the archive table holding the mirrored history. Defaults to
	// DefaultJobEpochTable.
	Table string
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
	table := cfg.Table
	if table == "" {
		table = DefaultJobEpochTable
	}
	m := &mirrorClient{schedd: direct, addr: cfg.Address, table: table, cfg: cfg.Config}
	// Reuse the schedd client's ad conversion, so the mirrored and direct paths
	// cannot drift in how they read an ad. If the wrapped client is something
	// else, there is nothing to reuse; fetchFromMirror then errors and the read
	// falls back, rather than silently returning no records.
	if h, ok := direct.(*htcClient); ok {
		m.convert = h.convertJobEpochAd
	}
	return m, nil
}

// Delegated: the mirror holds job history, nothing else.
func (m *mirrorClient) FetchTransferEpochs(since state.EpochID, cutoff time.Time) ([]TransferRecord, state.EpochID, error) {
	return m.schedd.FetchTransferEpochs(since, cutoff)
}
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

	if m.convert == nil {
		return nil, since, fmt.Errorf("condor: no ad converter for the mirrored history")
	}

	client, err := m.connectLocked(ctx)
	if err != nil {
		return nil, since, err
	}

	rows, err := client.ArchiveQuery(ctx, m.table, mirrorConstraint(cutoff, since), 0)
	if err != nil {
		m.dropLocked()
		return nil, since, fmt.Errorf("querying %s in htcondordb at %s: %w", m.table, m.addr, err)
	}

	return m.decodeRows(rows, since)
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
		if rec, _ := m.convert(ad); rec != nil {
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

	conn, err := cedarclient.ConnectAndAuthenticate(dialCtx, m.addr, sec)
	if err != nil {
		cancel()
		return nil, fmt.Errorf("connecting to htcondordb at %s: %w", m.addr, err)
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
