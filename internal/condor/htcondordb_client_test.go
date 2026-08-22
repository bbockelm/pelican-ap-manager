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
	// The stub is not an *htcClient, so the mirror borrowed no converter and
	// would fall back for that reason alone. Supply one, so a test that means to
	// exercise the unreachable-database path really does.
	m.convert = func(*classad.ClassAd) (*JobEpochRecord, state.EpochID) { return nil, state.EpochID{} }
	return m
}

// TestMirrorDelegatesEverythingItDoesNotServe pins the boundary. The mirror
// holds job history and nothing else -- transfer epochs come from
// TRANSFER_HISTORY, which nothing mirrors, so sending them to the database
// would silently return nothing.
func TestMirrorDelegatesEverythingItDoesNotServe(t *testing.T) {
	stub := &stubClient{}
	m := newTestMirror(t, stub)

	if _, _, err := m.FetchTransferEpochs(state.EpochID{}, time.Now()); err != nil {
		t.Fatalf("FetchTransferEpochs: %v", err)
	}
	if err := m.AdvertiseClassAds(nil); err != nil {
		t.Fatalf("AdvertiseClassAds: %v", err)
	}
	if _, err := m.QueryJobs(context.Background(), "true", nil); err != nil {
		t.Fatalf("QueryJobs: %v", err)
	}
	_, _ = m.LocateSchedd(context.Background())

	if stub.transferCalls != 1 {
		t.Errorf("transfer epochs went to the mirror; they must go to the schedd (%d delegated calls)", stub.transferCalls)
	}
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

func TestMirrorDefaultsToTheHistoryTable(t *testing.T) {
	c, err := NewMirrorClient(&stubClient{}, MirrorConfig{Address: "h:1", Config: testCondorConfig(t)})
	if err != nil {
		t.Fatalf("NewMirrorClient: %v", err)
	}
	// Named literally rather than compared to the constant, which would be a
	// tautology. htcondordb's schedd-sync writes completed jobs to "history";
	// "epoch_history" is a different table holding per-run-instance records, and
	// FetchJobEpochs reads HISTORY on the schedd side, so "history" is the table
	// that matches what the direct path returns.
	if got := c.(*mirrorClient).table; got != "history" {
		t.Errorf("table = %q, want \"history\"", got)
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
	m.convert = func(ad *classad.ClassAd) (*JobEpochRecord, state.EpochID) {
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
	m.convert = nil

	got, _, err := m.FetchJobEpochs(state.EpochID{}, time.Now())
	if err != nil {
		t.Fatalf("FetchJobEpochs: %v", err)
	}
	if len(got) != 1 || stub.jobEpochCalls != 1 {
		t.Errorf("got %d records after %d schedd calls; want 1 record from the schedd", len(got), stub.jobEpochCalls)
	}
}
