package store

import (
	"context"
	"encoding/json"
	"fmt"
	"sort"
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
	"github.com/bbockelm/pelican-ap-manager/internal/control"
	"github.com/bbockelm/pelican-ap-manager/internal/dbaddr"
	"github.com/bbockelm/pelican-ap-manager/internal/state"
)

// DefaultStateTable is the htcondordb table holding pelican-man's working state.
const DefaultStateTable = "pelican_manager_state"

// DBStateStore keeps the daemon's working state in an htcondordb table.
//
// Not as one document. The state is written on every poll cycle, and most of it
// does not change from one cycle to the next -- a document would mean shipping
// the whole thing, including a rolling window of transfer history, every few
// seconds. So it is stored in rows:
//
//	cursor            the two epoch cursors
//	pair:<key>        one row per (user, site) pair the control loop tracks
//	limit:<key>       one row per pair with a derived limit
//	bucket:<key>      one row per transfer summary bucket
//	scratch:<name>    the rolling working sets, one row each
//
// and Save writes only the rows whose contents actually moved. That keeps the
// steady-state write proportional to what changed rather than to how much
// history the daemon is holding, and it makes the interesting parts -- which
// pairs exist, what capacity each was given -- visible to a SELECT instead of
// buried in a blob.
//
// The rolling working sets stay JSON. They are this daemon's scratch, nobody
// queries them, and their shapes are nested; decomposing them into attributes
// would add a way to silently drop a field for no benefit.
type DBStateStore struct {
	addr  string
	table string
	cfg   *config.Config

	mu     sync.Mutex
	client *dbrpc.Client
	conn   *cedarclient.HTCondorClient
	cancel context.CancelFunc

	// written maps row key to the serialized ad last successfully committed, so
	// a Save can tell what changed and what disappeared. Empty until the first
	// Load or Save, which makes the first Save write everything -- correct,
	// since it cannot know what the table already holds.
	written map[string]string
}

// StateDBConfig configures a DBStateStore.
type StateDBConfig struct {
	// Address is the htcondordb daemon's command address.
	Address string
	// Table names the state table. Defaults to DefaultStateTable.
	Table string
	// Config supplies the client security policy, as for any HTCondor client.
	Config *config.Config
}

// OpenDBStateStore prepares a state store against an htcondordb daemon. It does
// not connect; the first operation does.
func OpenDBStateStore(cfg StateDBConfig) (*DBStateStore, error) {
	if strings.TrimSpace(cfg.Address) == "" {
		return nil, fmt.Errorf("store: htcondordb address is required")
	}
	if cfg.Config == nil {
		return nil, fmt.Errorf("store: HTCondor configuration is required for the htcondordb client security policy")
	}
	table := cfg.Table
	if table == "" {
		table = DefaultStateTable
	}
	return &DBStateStore{addr: cfg.Address, table: table, cfg: cfg.Config, written: map[string]string{}}, nil
}

// Load implements StateStore.
func (s *DBStateStore) Load(ctx context.Context) (*state.State, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	client, err := s.connectLocked(ctx)
	if err != nil {
		return nil, err
	}
	rows, err := client.QueryTable(ctx, s.table, "true", 0)
	if err != nil {
		s.dropLocked()
		return nil, fmt.Errorf("store: reading state: %w", err)
	}

	sec := state.Sections{
		Buckets:     map[string]state.SummaryStats{},
		PairStates:  map[string]control.PairState{},
		LimitStates: map[string]control.PairState{},
	}
	for _, row := range rows {
		// Query results come back in the bracketed new-ClassAd form; writes go
		// out in the old form. The asymmetry is dbrpc's -- see Save.
		ad, perr := classad.Parse(row)
		if perr != nil {
			return nil, fmt.Errorf("store: parsing a stored state row: %w", perr)
		}
		if _, kerr := applyStateRow(&sec, ad); kerr != nil {
			return nil, fmt.Errorf("store: decoding a stored state row: %w", kerr)
		}
	}

	// Seed the write cache with what Save *would* write for the state just
	// loaded, rather than with the server's rendering of it. The two are not
	// byte-identical -- a whole-number real comes back as an integer literal,
	// among other things -- and Save compares byte for byte, so seeding from the
	// server would leave those rows dirty on every cycle forever, quietly
	// undoing the reason the state is stored as rows at all.
	written, werr := stateRows(sec)
	if werr != nil {
		return nil, werr
	}

	st := state.New()
	st.RestoreSections(sec)
	s.written = written
	return st, nil
}

// Save implements StateStore.
func (s *DBStateStore) Save(ctx context.Context, st *state.State) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	rows, err := stateRows(st.Sections())
	if err != nil {
		return err
	}

	changed, gone := diffRows(s.written, rows)
	if len(changed) == 0 && len(gone) == 0 {
		return nil
	}

	client, err := s.connectLocked(ctx)
	if err != nil {
		return err
	}
	tx, err := client.BeginTable(ctx, s.table)
	if err != nil {
		s.dropLocked()
		return fmt.Errorf("store: beginning a state write: %w", err)
	}
	for key, ad := range changed {
		if err := tx.NewClassAd(ctx, key, ad); err != nil {
			_ = tx.Abort(ctx)
			return fmt.Errorf("store: writing state row %q: %w", key, err)
		}
	}
	for _, key := range gone {
		if err := tx.DestroyClassAd(ctx, key); err != nil {
			_ = tx.Abort(ctx)
			return fmt.Errorf("store: deleting state row %q: %w", key, err)
		}
	}
	if err := tx.Commit(ctx); err != nil {
		return fmt.Errorf("store: committing state: %w", err)
	}

	// Only now: a failed commit must leave the cache describing what is really
	// stored, or the next Save would skip the rows it thinks it already wrote.
	for key, ad := range changed {
		s.written[key] = ad
	}
	for _, key := range gone {
		delete(s.written, key)
	}
	return nil
}

// Close implements StateStore.
func (s *DBStateStore) Close() error {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.dropLocked()
	return nil
}

func (s *DBStateStore) connectLocked(ctx context.Context) (*dbrpc.Client, error) {
	if s.client != nil {
		return s.client, nil
	}

	sec, err := htcondor.GetSecurityConfig(s.cfg, command.DBSession, "CLIENT")
	if err != nil {
		return nil, fmt.Errorf("store: building client security config: %w", err)
	}
	sec.Command = command.DBSession
	if sec.Authentication == security.SecurityOptional {
		sec.Authentication = security.SecurityPreferred
	}

	sessCtx, cancel := context.WithCancel(context.Background())
	dialCtx, dialCancel := context.WithTimeout(ctx, 30*time.Second)
	defer dialCancel()

	// Resolved per dial; see internal/dbaddr.
	addr, err := dbaddr.Resolve(s.addr, s.cfg)
	if err != nil {
		cancel()
		return nil, err
	}

	conn, err := cedarclient.ConnectAndAuthenticate(dialCtx, addr, sec)
	if err != nil {
		cancel()
		return nil, fmt.Errorf("store: connecting to htcondordb at %s: %w", addr, err)
	}

	client := dbrpc.NewClient(dbrpc.NewCedarConn(sessCtx, conn.GetStream()))
	_ = client.CreateTable(ctx, s.table)

	s.client, s.conn, s.cancel = client, conn, cancel
	return client, nil
}

func (s *DBStateStore) dropLocked() {
	if s.client != nil {
		_ = s.client.Close()
	}
	if s.cancel != nil {
		s.cancel()
	}
	s.client, s.conn, s.cancel = nil, nil, nil
	// The cache describes a session's worth of successful writes, not the
	// table. It survives a reconnect: the rows are still there.
}

// Row keys and attribute names. Spelled out rather than derived, so renaming a
// Go identifier cannot orphan already-stored rows.
const (
	stateRowCursor = "cursor"
	pairRowPrefix  = "pair:"
	limitRowPrefix = "limit:"
	bucketPrefix   = "bucket:"
	scratchPrefix  = "scratch:"

	attrKind = "Kind"

	kindCursor  = "cursor"
	kindPair    = "pair"
	kindLimit   = "limit"
	kindBucket  = "bucket"
	kindScratch = "scratch"

	attrLastEpochCluster = "LastEpochClusterId"
	attrLastEpochProc    = "LastEpochProcId"
	attrLastEpochRun     = "LastEpochRunInstanceId"
	attrLastJobCluster   = "LastJobEpochClusterId"
	attrLastJobProc      = "LastJobEpochProcId"
	attrLastJobRun       = "LastJobEpochRunInstanceId"

	attrPairKey   = "PairKey"
	attrCapacity  = "CapacityGBPerMin"
	attrBucketKey = "BucketKey"

	attrSuccesses          = "Successes"
	attrFailures           = "Failures"
	attrSuccessBytes       = "SuccessBytes"
	attrFailureBytes       = "FailureBytes"
	attrSuccessDurationSec = "SuccessDurationSec"
	attrFailureDurationSec = "FailureDurationSec"
	attrLastUpdated        = "LastUpdated"
	attrFederations        = "FederationsJSON"

	attrSection = "Section"
	attrPayload = "PayloadJSON"
)

// Scratch section names. These are row keys, so they are stable strings rather
// than anything derived from the Go field names.
const (
	sectionRecentTransfers = "recent_transfers"
	sectionEpochBuckets    = "epoch_buckets"
	sectionEpochIndex      = "epoch_index"
	sectionJobEpochs       = "job_epochs"
	sectionEpochUsers      = "epoch_users"
	sectionBucketRuntimes  = "bucket_runtimes"
)

// diffRows works out the smallest write that makes the table match rows, given
// what was last committed.
//
// A row whose serialization is byte-identical to what is already stored is not
// worth a round trip -- which is the point of storing rows at all. A pair that
// has gone quiet, or a bucket that aged out of the window, disappears from the
// desired set and has to be deleted, or the table would accumulate state the
// daemon no longer believes.
func diffRows(written, rows map[string]string) (changed map[string]string, gone []string) {
	changed = make(map[string]string)
	for key, ad := range rows {
		if prev, ok := written[key]; !ok || prev != ad {
			changed[key] = ad
		}
	}
	for key := range written {
		if _, ok := rows[key]; !ok {
			gone = append(gone, key)
		}
	}
	sort.Strings(gone)
	return changed, gone
}

// stateRows renders the sections as the complete set of rows the table should
// hold, keyed by row key.
func stateRows(sec state.Sections) (map[string]string, error) {
	rows := make(map[string]string, len(sec.Buckets)+len(sec.PairStates)+len(sec.LimitStates)+8)

	cursor := classad.New()
	cursor.InsertAttrString(attrKind, kindCursor)
	cursor.InsertAttr(attrLastEpochCluster, sec.LastEpoch.ClusterID)
	cursor.InsertAttr(attrLastEpochProc, sec.LastEpoch.ProcID)
	cursor.InsertAttr(attrLastEpochRun, sec.LastEpoch.RunInstanceID)
	cursor.InsertAttr(attrLastJobCluster, sec.LastJobEpoch.ClusterID)
	cursor.InsertAttr(attrLastJobProc, sec.LastJobEpoch.ProcID)
	cursor.InsertAttr(attrLastJobRun, sec.LastJobEpoch.RunInstanceID)
	rows[stateRowCursor] = cursor.MarshalOld()

	for key, ps := range sec.PairStates {
		rows[pairRowPrefix+key] = pairAd(kindPair, key, ps).MarshalOld()
	}
	for key, ps := range sec.LimitStates {
		rows[limitRowPrefix+key] = pairAd(kindLimit, key, ps).MarshalOld()
	}

	for key, st := range sec.Buckets {
		ad, err := bucketAd(key, st)
		if err != nil {
			return nil, err
		}
		rows[bucketPrefix+key] = ad.MarshalOld()
	}

	scratch := map[string]any{
		sectionRecentTransfers: sec.RecentTransfers,
		sectionEpochBuckets:    sec.EpochBuckets,
		sectionEpochIndex:      sec.EpochIndex,
		sectionJobEpochs:       sec.JobEpochs,
		sectionEpochUsers:      sec.EpochUsers,
		sectionBucketRuntimes:  sec.BucketRuntimes,
	}
	for name, value := range scratch {
		payload, err := json.Marshal(value)
		if err != nil {
			return nil, fmt.Errorf("store: encoding state section %q: %w", name, err)
		}
		ad := classad.New()
		ad.InsertAttrString(attrKind, kindScratch)
		ad.InsertAttrString(attrSection, name)
		ad.InsertAttrString(attrPayload, string(payload))
		rows[scratchPrefix+name] = ad.MarshalOld()
	}

	return rows, nil
}

func pairAd(kind, key string, ps control.PairState) *classad.ClassAd {
	ad := classad.New()
	ad.InsertAttrString(attrKind, kind)
	ad.InsertAttrString(attrPairKey, key)
	ad.InsertAttrFloat(attrCapacity, ps.CapacityGBPerMin)
	ad.InsertAttr(attrLastUpdated, unixOrZero(ps.LastUpdated))
	return ad
}

func bucketAd(key string, st state.SummaryStats) (*classad.ClassAd, error) {
	ad := classad.New()
	ad.InsertAttrString(attrKind, kindBucket)
	ad.InsertAttrString(attrBucketKey, key)
	ad.InsertAttr(attrSuccesses, int64(st.Successes))
	ad.InsertAttr(attrFailures, int64(st.Failures))
	ad.InsertAttr(attrSuccessBytes, st.SuccessBytes)
	ad.InsertAttr(attrFailureBytes, st.FailureBytes)
	ad.InsertAttrFloat(attrSuccessDurationSec, st.SuccessDurationSec)
	ad.InsertAttrFloat(attrFailureDurationSec, st.FailureDurationSec)
	ad.InsertAttr(attrLastUpdated, unixOrZero(st.LastUpdated))

	// The per-federation breakdown is a map with operator-supplied keys, so it
	// stays JSON rather than becoming attributes.
	if len(st.Federations) > 0 {
		payload, err := json.Marshal(st.Federations)
		if err != nil {
			return nil, fmt.Errorf("store: encoding federation stats for bucket %q: %w", key, err)
		}
		ad.InsertAttrString(attrFederations, string(payload))
	}
	return ad, nil
}

// applyStateRow folds one stored row into the sections being rebuilt, returning
// the row key it came from.
//
// An unrecognized Kind is an error rather than a skip: it means this daemon is
// reading a table written by a newer version, and quietly dropping the row
// would present a partial state as a complete one.
func applyStateRow(sec *state.Sections, ad *classad.ClassAd) (string, error) {
	kind, _ := ad.EvaluateAttrString(attrKind)
	switch kind {
	case kindCursor:
		sec.LastEpoch = state.EpochID{
			ClusterID:     attrInt(ad, attrLastEpochCluster),
			ProcID:        attrInt(ad, attrLastEpochProc),
			RunInstanceID: attrInt(ad, attrLastEpochRun),
		}
		sec.LastJobEpoch = state.EpochID{
			ClusterID:     attrInt(ad, attrLastJobCluster),
			ProcID:        attrInt(ad, attrLastJobProc),
			RunInstanceID: attrInt(ad, attrLastJobRun),
		}
		return stateRowCursor, nil

	case kindPair, kindLimit:
		key, ok := ad.EvaluateAttrString(attrPairKey)
		if !ok || key == "" {
			return "", fmt.Errorf("%s row has no %s", kind, attrPairKey)
		}
		ps := control.PairState{
			CapacityGBPerMin: attrFloat(ad, attrCapacity),
			LastUpdated:      timeOrZero(attrInt(ad, attrLastUpdated)),
		}
		if kind == kindPair {
			sec.PairStates[key] = ps
			return pairRowPrefix + key, nil
		}
		sec.LimitStates[key] = ps
		return limitRowPrefix + key, nil

	case kindBucket:
		key, ok := ad.EvaluateAttrString(attrBucketKey)
		if !ok || key == "" {
			return "", fmt.Errorf("bucket row has no %s", attrBucketKey)
		}
		st := state.SummaryStats{
			Successes:          int(attrInt(ad, attrSuccesses)),
			Failures:           int(attrInt(ad, attrFailures)),
			SuccessBytes:       attrInt(ad, attrSuccessBytes),
			FailureBytes:       attrInt(ad, attrFailureBytes),
			SuccessDurationSec: attrFloat(ad, attrSuccessDurationSec),
			FailureDurationSec: attrFloat(ad, attrFailureDurationSec),
			LastUpdated:        timeOrZero(attrInt(ad, attrLastUpdated)),
		}
		if raw, _ := ad.EvaluateAttrString(attrFederations); raw != "" {
			if err := json.Unmarshal([]byte(raw), &st.Federations); err != nil {
				return "", fmt.Errorf("decoding federation stats for bucket %q: %w", key, err)
			}
		}
		sec.Buckets[key] = st
		return bucketPrefix + key, nil

	case kindScratch:
		name, ok := ad.EvaluateAttrString(attrSection)
		if !ok || name == "" {
			return "", fmt.Errorf("scratch row has no %s", attrSection)
		}
		payload, _ := ad.EvaluateAttrString(attrPayload)
		if err := applyScratch(sec, name, payload); err != nil {
			return "", err
		}
		return scratchPrefix + name, nil

	default:
		return "", fmt.Errorf("unknown state row kind %q (written by a newer pelican-man?)", kind)
	}
}

func applyScratch(sec *state.Sections, name, payload string) error {
	if payload == "" {
		return nil
	}
	var target any
	switch name {
	case sectionRecentTransfers:
		target = &sec.RecentTransfers
	case sectionEpochBuckets:
		target = &sec.EpochBuckets
	case sectionEpochIndex:
		target = &sec.EpochIndex
	case sectionJobEpochs:
		target = &sec.JobEpochs
	case sectionEpochUsers:
		target = &sec.EpochUsers
	case sectionBucketRuntimes:
		target = &sec.BucketRuntimes
	default:
		return fmt.Errorf("unknown state section %q (written by a newer pelican-man?)", name)
	}
	if err := json.Unmarshal([]byte(payload), target); err != nil {
		return fmt.Errorf("decoding state section %q: %w", name, err)
	}
	return nil
}

func attrInt(ad *classad.ClassAd, name string) int64 {
	n, _ := ad.EvaluateAttrInt(name)
	return n
}

func attrFloat(ad *classad.ClassAd, name string) float64 {
	if f, ok := ad.EvaluateAttrReal(name); ok {
		return f
	}
	// A whole number round-trips as an integer literal, which EvaluateAttrReal
	// may decline; falling back keeps a capacity of exactly 2 from reading as 0.
	return float64(attrInt(ad, name))
}

func timeOrZero(unix int64) time.Time {
	if unix <= 0 {
		return time.Time{}
	}
	return time.Unix(unix, 0)
}
