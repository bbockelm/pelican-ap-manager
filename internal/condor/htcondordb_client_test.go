package condor

import (
	"context"
	"fmt"
	"testing"
	"time"

	"strings"

	"github.com/PelicanPlatform/classad/classad"
	htcondor "github.com/bbockelm/golang-htcondor"
	"github.com/bbockelm/pelican-ap-manager/internal/state"
)

// stubClient stands in for the schedd-backed client so the delegation and
// fallback paths can be exercised without a pool.
type stubClient struct {
	jobEpochCalls     int
	transferCalls     int
	advertiseCalls    int
	queryJobsCalls    int
	locateScheddCalls int

	jobEpochs []JobEpochRecord
	newest    state.EpochID
}

func (s *stubClient) FetchTransferEpochs(since state.EpochID, _ time.Time) ([]TransferRecord, state.EpochID, error) {
	s.transferCalls++
	return nil, since, nil
}
func (s *stubClient) FetchJobEpochs(_ state.EpochID, _ time.Time) ([]JobEpochRecord, state.EpochID, error) {
	s.jobEpochCalls++
	return s.jobEpochs, s.newest, nil
}
func (s *stubClient) AdvertiseClassAds(_ []map[string]any) error { s.advertiseCalls++; return nil }
func (s *stubClient) QueryJobs(_ context.Context, _ string, _ []string) ([]*classad.ClassAd, error) {
	s.queryJobsCalls++
	return nil, nil
}
func (s *stubClient) LocateSchedd(_ context.Context) (*htcondor.Schedd, error) {
	s.locateScheddCalls++
	return nil, fmt.Errorf("stub has no schedd")
}

func newTestMirror(t *testing.T, direct CondorClient) *mirrorClient {
	t.Helper()
	// An address that cannot be dialled: every mirror query fails, which is the
	// state the fallback exists for.
	c, err := NewMirrorClient(direct, MirrorConfig{
		Address: "127.0.0.1:1",
		Config:  testCondorConfig(t),
	})
	if err != nil {
		t.Fatalf("NewMirrorClient: %v", err)
	}
	m := c.(*mirrorClient)
	// The stub is not an *htcClient, so the mirror borrowed no converters and
	// would fall back for that reason alone. Supply them, so a test that means to
	// exercise the unreachable-database path really does.
	m.convertJob = func(*classad.ClassAd) (*JobEpochRecord, state.EpochID) { return nil, state.EpochID{} }
	m.convertTransfer = func(*classad.ClassAd) ([]TransferRecord, state.EpochID) { return nil, state.EpochID{} }
	return m
}

// TestMirrorDelegatesWhatItDoesNotHold pins the boundary. The mirror holds
// history; advertising and job queries are about what is happening now, which is
// not something a history mirror can answer.
func TestMirrorDelegatesWhatItDoesNotHold(t *testing.T) {
	stub := &stubClient{}
	m := newTestMirror(t, stub)

	if err := m.AdvertiseClassAds(nil); err != nil {
		t.Fatalf("AdvertiseClassAds: %v", err)
	}
	if _, err := m.QueryJobs(context.Background(), "true", nil); err != nil {
		t.Fatalf("QueryJobs: %v", err)
	}
	_, _ = m.LocateSchedd(context.Background())

	if stub.advertiseCalls != 1 || stub.queryJobsCalls != 1 || stub.locateScheddCalls != 1 {
		t.Errorf("delegation missed: advertise=%d queryJobs=%d locateSchedd=%d",
			stub.advertiseCalls, stub.queryJobsCalls, stub.locateScheddCalls)
	}
}

// TestMirrorFallsBackToScheddWhenUnreachable: a database outage should cost
// extra schedd load, not blind the control loop.
func TestMirrorFallsBackToScheddWhenUnreachable(t *testing.T) {
	want := []JobEpochRecord{{User: "alice", Site: "UCSD"}}
	stub := &stubClient{jobEpochs: want, newest: state.EpochID{ClusterID: 7, ProcID: 0, RunInstanceID: 1}}
	m := newTestMirror(t, stub)

	got, newest, err := m.FetchJobEpochs(state.EpochID{}, time.Now().Add(-time.Hour))
	if err != nil {
		t.Fatalf("FetchJobEpochs: %v", err)
	}
	if stub.jobEpochCalls != 1 {
		t.Fatalf("schedd was not consulted after the mirror failed (%d calls)", stub.jobEpochCalls)
	}
	if len(got) != len(want) || newest != stub.newest {
		t.Errorf("fallback returned %d records / newest %v; want %d / %v", len(got), newest, len(want), stub.newest)
	}
}

func TestMirrorConstraint(t *testing.T) {
	cutoff := time.Unix(1_700_000_000, 0)
	zero := state.EpochID{}

	// EpochWriteDate is zone-mapped in the archive, so this form prunes whole
	// segments.
	if got, want := mirrorConstraint(cutoff, zero), "EpochWriteDate >= 1700000000"; got != want {
		t.Errorf("mirrorConstraint = %q, want %q", got, want)
	}
	// No bounds at all means "everything the archive still holds" -- the first
	// poll of a daemon configured with no lookback.
	if got := mirrorConstraint(time.Time{}, zero); got != "true" {
		t.Errorf("mirrorConstraint(unbounded) = %q, want \"true\"", got)
	}

	// The steady-state case, and the one that matters for load: a poll that
	// already knows where it left off must not drag the whole lookback window
	// across the wire again.
	since := state.EpochID{ClusterID: 42, ProcID: 3, RunInstanceID: 1}
	got := mirrorConstraint(cutoff, since)
	if want := "EpochWriteDate >= 1700000000 && ClusterId >= 42"; got != want {
		t.Errorf("mirrorConstraint = %q, want %q", got, want)
	}
	if !strings.Contains(got, "ClusterId") {
		t.Error("no ClusterId bound: every poll would refetch the entire lookback window")
	}
}

func TestNewMirrorClientRejectsIncompleteConfig(t *testing.T) {
	cfg := testCondorConfig(t)
	if _, err := NewMirrorClient(nil, MirrorConfig{Address: "h:1", Config: cfg}); err == nil {
		t.Error("accepted a nil client to delegate to")
	}
	if _, err := NewMirrorClient(&stubClient{}, MirrorConfig{Config: cfg}); err == nil {
		t.Error("accepted an empty htcondordb address")
	}
	if _, err := NewMirrorClient(&stubClient{}, MirrorConfig{Address: "h:1"}); err == nil {
		t.Error("accepted a nil HTCondor configuration")
	}
}

func TestMirrorDefaultsToTheRightTables(t *testing.T) {
	c, err := NewMirrorClient(&stubClient{}, MirrorConfig{Address: "h:1", Config: testCondorConfig(t)})
	if err != nil {
		t.Fatalf("NewMirrorClient: %v", err)
	}
	m := c.(*mirrorClient)

	// Named literally rather than compared to the constants, which would be a
	// tautology. The two reads come from two different schedd files, so they
	// come from two different archive tables: FetchJobEpochs reads the schedd's
	// HISTORY file, which scheddsync mirrors to "history", while transfer
	// records are written to JOB_EPOCH_HISTORY, mirrored to "epoch_history".
	// Pointing either at the other's table would silently return the wrong kind
	// of record.
	if m.jobTable != "history" {
		t.Errorf("jobTable = %q, want \"history\"", m.jobTable)
	}
	if m.transferTable != "epoch_history" {
		t.Errorf("transferTable = %q, want \"epoch_history\"", m.transferTable)
	}
	if m.jobTable == m.transferTable {
		t.Error("both reads point at the same table")
	}

	// And both are overridable, independently.
	c, err = NewMirrorClient(&stubClient{}, MirrorConfig{
		Address: "h:1", JobTable: "ap_jobs", TransferTable: "ap_epochs", Config: testCondorConfig(t),
	})
	if err != nil {
		t.Fatalf("NewMirrorClient: %v", err)
	}
	m = c.(*mirrorClient)
	if m.jobTable != "ap_jobs" || m.transferTable != "ap_epochs" {
		t.Errorf("tables = %q/%q, want ap_jobs/ap_epochs", m.jobTable, m.transferTable)
	}

	jt, tt := MirrorTables(MirrorConfig{Address: "h:1"})
	if jt != "history" || tt != "epoch_history" {
		t.Errorf("MirrorTables reported %q/%q, want history/epoch_history", jt, tt)
	}
}

// TestMirrorSkipsAlreadySeenEpochs covers the filter that makes the read
// incremental. The constraint's ClusterId bound is deliberately coarse -- it
// admits every proc of the last-seen cluster -- so without this the daemon would
// reprocess part of its own history on every poll and double-count the
// transfers in it.
func TestMirrorSkipsAlreadySeenEpochs(t *testing.T) {
	m := newTestMirror(t, &stubClient{})
	seen := map[state.EpochID]bool{}
	m.convertJob = func(ad *classad.ClassAd) (*JobEpochRecord, state.EpochID) {
		id := epochFromAd(ad)
		seen[id] = true
		return &JobEpochRecord{User: "alice"}, id
	}

	row := func(cluster, proc, run int) string {
		return fmt.Sprintf("ClusterId = %d\nProcId = %d\nRunInstanceID = %d\nOwner = \"alice\"\n", cluster, proc, run)
	}
	since := state.EpochID{ClusterID: 42, ProcID: 3, RunInstanceID: 1}

	records, newest, err := m.decodeRows([]string{
		row(42, 2, 1), // older proc in the same cluster: seen
		row(42, 3, 1), // exactly the cursor: seen
		row(42, 4, 0), // later proc in the same cluster: new
		row(43, 0, 0), // later cluster: new
	}, since)
	if err != nil {
		t.Fatalf("decodeRows: %v", err)
	}

	if len(records) != 2 {
		t.Errorf("%d records, want 2 (the two epochs after the cursor)", len(records))
	}
	for _, stale := range []state.EpochID{{ClusterID: 42, ProcID: 2, RunInstanceID: 1}, since} {
		if seen[stale] {
			t.Errorf("epoch %v was reprocessed; its transfers would be counted twice", stale)
		}
	}
	if want := (state.EpochID{ClusterID: 43}); newest != want {
		t.Errorf("newest = %v, want %v", newest, want)
	}
}

// TestMirrorReportsAnUnparsableRow: a row the archive cannot yield as a ClassAd
// means the mirror is not what we think it is. Erroring sends the cycle back to
// the schedd; skipping the row would quietly lose history instead.
func TestMirrorReportsAnUnparsableRow(t *testing.T) {
	m := newTestMirror(t, &stubClient{})
	if _, _, err := m.decodeRows([]string{"ClusterId = = ="}, state.EpochID{}); err == nil {
		t.Error("decodeRows accepted a row that is not a ClassAd")
	}
}

// TestMirrorWithoutAConverterFallsBack: the mirror borrows its ad conversion
// from the schedd client so the two paths cannot read an ad differently. If
// there is nothing to borrow it must fall back, not return an empty history --
// an empty history is indistinguishable from a quiet pool.
func TestMirrorWithoutAConverterFallsBack(t *testing.T) {
	stub := &stubClient{jobEpochs: []JobEpochRecord{{User: "alice"}}}
	m := newTestMirror(t, stub)
	m.convertJob = nil

	got, _, err := m.FetchJobEpochs(state.EpochID{}, time.Now())
	if err != nil {
		t.Fatalf("FetchJobEpochs: %v", err)
	}
	if len(got) != 1 || stub.jobEpochCalls != 1 {
		t.Errorf("got %d records after %d schedd calls; want 1 record from the schedd", len(got), stub.jobEpochCalls)
	}
}

// TestTransferConstraintFiltersAdTypes is the load-bearing test for reading
// transfers from the mirror.
//
// There is no TRANSFER_HISTORY file: a HistorySourceTransfer query is a
// JOB_EPOCH query with HistoryAdTypeFilter=INPUT,OUTPUT,CHECKPOINT. The epoch
// archive therefore holds every kind of run-instance record, and without the
// same filter the daemon would hand SPAWN and EPOCH ads to the transfer
// conversion -- which reads them happily, as transfers with no bytes and no
// endpoint.
func TestTransferConstraintFiltersAdTypes(t *testing.T) {
	got := transferConstraint(time.Unix(1_700_000_000, 0), state.EpochID{ClusterID: 42})

	// The set has to match what golang-htcondor sends the schedd, or the
	// mirrored and direct reads see different records.
	for _, adType := range []string{"INPUT", "OUTPUT", "CHECKPOINT"} {
		if !strings.Contains(got, `EpochAdType == "`+adType+`"`) {
			t.Errorf("constraint %q does not admit %s records", got, adType)
		}
	}
	// And it must not admit the job-lifecycle records that share the file.
	for _, adType := range []string{"SPAWN", "EPOCH"} {
		if strings.Contains(got, adType) {
			t.Errorf("constraint %q admits %s records, which are not transfers", got, adType)
		}
	}
	// The ad-type clause is an alternation, so it has to be parenthesized or the
	// && bounds would apply to the first branch only.
	if !strings.Contains(got, "&& (") {
		t.Errorf("constraint %q does not group the ad-type alternation; the cutoff and cursor bounds would apply to one branch", got)
	}
	// The bounds that make the read incremental still have to be there.
	if !strings.Contains(got, "EpochWriteDate >= 1700000000") || !strings.Contains(got, "ClusterId >= 42") {
		t.Errorf("constraint %q lost the cutoff or cursor bound", got)
	}
}

// TestTransferAdTypesMatchTheScheddFilter states the coupling in one place. If
// golang-htcondor's default transfer filter ever changes, this is the assertion
// that should be updated alongside it -- not discovered by a mirrored read that
// quietly returns a different set of records than the direct one.
func TestTransferAdTypesMatchTheScheddFilter(t *testing.T) {
	want := []string{"INPUT", "OUTPUT", "CHECKPOINT"}
	if diff := len(transferAdTypes) - len(want); diff != 0 {
		t.Fatalf("transferAdTypes = %v, want %v", transferAdTypes, want)
	}
	for i, w := range want {
		if transferAdTypes[i] != w {
			t.Errorf("transferAdTypes[%d] = %q, want %q", i, transferAdTypes[i], w)
		}
	}
}

// TestMirrorFallsBackForTransfersToo: a mirror that cannot be reached must not
// leave the control loop without the transfer data it exists to react to.
func TestMirrorFallsBackForTransfersToo(t *testing.T) {
	stub := &stubClient{}
	m := newTestMirror(t, stub)

	if _, _, err := m.FetchTransferEpochs(state.EpochID{}, time.Now().Add(-time.Hour)); err != nil {
		t.Fatalf("FetchTransferEpochs: %v", err)
	}
	if stub.transferCalls != 1 {
		t.Errorf("the schedd was not consulted after the mirror failed (%d calls)", stub.transferCalls)
	}
}

// TestMirrorWithoutATransferConverterFallsBack: as for job epochs, an empty
// result is indistinguishable from a quiet pool, so the mirror must decline
// rather than return nothing.
func TestMirrorWithoutATransferConverterFallsBack(t *testing.T) {
	stub := &stubClient{}
	m := newTestMirror(t, stub)
	m.convertTransfer = nil

	if _, _, err := m.FetchTransferEpochs(state.EpochID{}, time.Now()); err != nil {
		t.Fatalf("FetchTransferEpochs: %v", err)
	}
	if stub.transferCalls != 1 {
		t.Errorf("%d schedd calls, want 1: the mirror returned an empty history instead of falling back", stub.transferCalls)
	}
}

// TestTransferDecodeSkipsSeenEpochsAndFlattens covers what the transfer read
// does with the rows it gets: one ad can carry several transfer legs, and
// already-counted epochs must not be replayed.
func TestTransferDecodeSkipsSeenEpochsAndFlattens(t *testing.T) {
	m := newTestMirror(t, &stubClient{})

	var seen []state.EpochID
	m.convertTransfer = func(ad *classad.ClassAd) ([]TransferRecord, state.EpochID) {
		id := epochFromAd(ad)
		seen = append(seen, id)
		// Two legs per ad, as an input plus an output would be.
		return []TransferRecord{{User: "alice"}, {User: "alice"}}, id
	}

	row := func(cluster, proc, run int) string {
		return fmt.Sprintf("ClusterId = %d\nProcId = %d\nRunInstanceID = %d\nEpochAdType = \"INPUT\"\n", cluster, proc, run)
	}
	since := state.EpochID{ClusterID: 42, ProcID: 3, RunInstanceID: 1}

	records, newest, err := m.decodeTransferRows([]string{
		row(42, 3, 1), // exactly the cursor: already counted
		row(42, 4, 0), // new
		row(43, 0, 0), // new
	}, since)
	if err != nil {
		t.Fatalf("decodeTransferRows: %v", err)
	}

	if len(records) != 4 {
		t.Errorf("%d records from 2 new ads of 2 legs each, want 4", len(records))
	}
	for _, id := range seen {
		if !id.After(since) {
			t.Errorf("epoch %v was converted again; its transfers would be counted twice", id)
		}
	}
	if want := (state.EpochID{ClusterID: 43}); newest != want {
		t.Errorf("newest = %v, want %v", newest, want)
	}
}

// recordedQuery captures what a mirror read asks the database for.
type recordedQuery struct {
	table      string
	constraint string
}

// TestEachReadUsesItsOwnTable is the guard on the wiring, and the reason
// mirrorClient indirects its query at all.
//
// The two reads pull different kinds of record from different tables: completed
// jobs from the mirrored HISTORY file, transfers from the mirrored
// JOB_EPOCH_HISTORY. Crossing them returns records of the wrong kind, which the
// conversions accept without complaint -- completed-job ads read as transfers
// with no bytes, epoch ads read as jobs with no runtime. No error, just wrong
// numbers, which is why this needs asserting rather than reviewing.
func TestEachReadUsesItsOwnTable(t *testing.T) {
	m := newTestMirror(t, &stubClient{})
	var queries []recordedQuery
	m.query = func(_ context.Context, table, constraint string) ([]string, error) {
		queries = append(queries, recordedQuery{table, constraint})
		return nil, nil
	}

	cutoff := time.Unix(1_700_000_000, 0)
	if _, _, err := m.FetchJobEpochs(state.EpochID{}, cutoff); err != nil {
		t.Fatalf("FetchJobEpochs: %v", err)
	}
	if _, _, err := m.FetchTransferEpochs(state.EpochID{}, cutoff); err != nil {
		t.Fatalf("FetchTransferEpochs: %v", err)
	}

	if len(queries) != 2 {
		t.Fatalf("%d queries, want 2 (one per read)", len(queries))
	}
	jobQ, xferQ := queries[0], queries[1]

	if jobQ.table != m.jobTable {
		t.Errorf("job history read from %q, want the job table %q", jobQ.table, m.jobTable)
	}
	if xferQ.table != m.transferTable {
		t.Errorf("transfer history read from %q, want the transfer table %q", xferQ.table, m.transferTable)
	}
	if jobQ.table == xferQ.table {
		t.Error("both reads went to the same table")
	}

	// And each carries its own constraint: only the transfer read filters ad
	// types, and only it may -- the job table holds completed jobs, which have
	// no EpochAdType at all.
	if strings.Contains(jobQ.constraint, "EpochAdType") {
		t.Errorf("the job read filters on EpochAdType (%q); completed-job records do not carry it, so this matches nothing", jobQ.constraint)
	}
	if !strings.Contains(xferQ.constraint, "EpochAdType") {
		t.Errorf("the transfer read does not filter ad types (%q); it would treat SPAWN and EPOCH records as transfers", xferQ.constraint)
	}
}

// TestBothReadsFallBackWhenTheQueryFails: the seam must not have introduced a
// path where a database error propagates instead of deferring to the schedd.
func TestBothReadsFallBackWhenTheQueryFails(t *testing.T) {
	stub := &stubClient{jobEpochs: []JobEpochRecord{{User: "alice"}}}
	m := newTestMirror(t, stub)
	m.query = func(context.Context, string, string) ([]string, error) {
		return nil, fmt.Errorf("archive unavailable")
	}

	jobs, _, err := m.FetchJobEpochs(state.EpochID{}, time.Now())
	if err != nil {
		t.Fatalf("FetchJobEpochs: %v", err)
	}
	if len(jobs) != 1 || stub.jobEpochCalls != 1 {
		t.Errorf("job read did not fall back: %d records, %d schedd calls", len(jobs), stub.jobEpochCalls)
	}

	if _, _, err := m.FetchTransferEpochs(state.EpochID{}, time.Now()); err != nil {
		t.Fatalf("FetchTransferEpochs: %v", err)
	}
	if stub.transferCalls != 1 {
		t.Errorf("transfer read did not fall back: %d schedd calls", stub.transferCalls)
	}
}
