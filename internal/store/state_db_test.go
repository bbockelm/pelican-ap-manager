package store

import (
	"testing"
	"time"

	"github.com/PelicanPlatform/classad/classad"
	condorconfig "github.com/bbockelm/golang-htcondor/config"
	"github.com/bbockelm/pelican-ap-manager/internal/control"
	"github.com/bbockelm/pelican-ap-manager/internal/state"
	"github.com/google/go-cmp/cmp"
)

// populatedSections mirrors what a running daemon holds: every section
// non-empty, so a round-trip test cannot pass by preserving only what happens
// to be set.
func populatedSections() state.Sections {
	now := time.Unix(1_700_000_000, 0)
	return state.Sections{
		LastEpoch:    state.EpochID{ClusterID: 12, ProcID: 1, RunInstanceID: 3},
		LastJobEpoch: state.EpochID{ClusterID: 14, ProcID: 2, RunInstanceID: 1},
		Buckets: map[string]state.SummaryStats{
			"user=alice|endpoint=e1|site=UCSD|dir=download": {
				Successes: 7, Failures: 2,
				SuccessBytes: 1 << 30, FailureBytes: 1 << 20,
				SuccessDurationSec: 12.5, FailureDurationSec: 0.25,
				LastUpdated: now,
				Federations: map[string]state.FederationStats{
					"osdf": {Successes: 5, Failures: 1, SuccessBytes: 999, SuccessDurationSec: 1.5},
				},
			},
			// A bucket with no federation breakdown and whole-number durations:
			// the attribute is omitted, and the reals round-trip as integer
			// literals, both of which are easy to read back wrong.
			"user=bob|endpoint=e2|site=PSU|dir=upload": {
				Successes: 3, SuccessBytes: 10, SuccessDurationSec: 2, LastUpdated: now,
			},
		},
		RecentTransfers: map[string][]state.TransferHistoryEntry{
			"alice": {{
				User: "alice", Site: "UCSD", Source: "osdf:///a", Destination: "./a",
				Direction: "download", Bytes: 4096, DurationSeconds: 1.5, Success: true,
			}},
		},
		EpochBuckets:   map[string][]state.TransferEpochRef{"user=alice": {{}}},
		EpochIndex:     map[string]string{"12.1.3": "user=alice"},
		JobEpochs:      map[string]state.JobEpochSample{"12.1.3": {}},
		EpochUsers:     map[string]string{"12.1.3": "alice"},
		BucketRuntimes: map[string][]state.BucketRuntimeSample{"user=alice": {{}}},
		PairStates: map[string]control.PairState{
			"alice|UCSD": {CapacityGBPerMin: 4.5, LastUpdated: now},
			// A whole-number capacity, which serializes as an integer literal.
			"bob|PSU": {CapacityGBPerMin: 2, LastUpdated: now},
		},
		LimitStates: map[string]control.PairState{
			"alice|UCSD": {CapacityGBPerMin: 2.25, LastUpdated: now},
		},
	}
}

// decodeRowsToSections replays a rendered row set the way Load does.
func decodeRowsToSections(t *testing.T, rows map[string]string) state.Sections {
	t.Helper()
	sec := state.Sections{
		Buckets:     map[string]state.SummaryStats{},
		PairStates:  map[string]control.PairState{},
		LimitStates: map[string]control.PairState{},
	}
	for key, row := range rows {
		ad, err := classad.ParseOld(row)
		if err != nil {
			t.Fatalf("parsing row %q: %v", key, err)
		}
		got, err := applyStateRow(&sec, ad)
		if err != nil {
			t.Fatalf("applying row %q: %v", key, err)
		}
		if got != key {
			t.Errorf("row rendered under key %q decodes as key %q; a mismatch makes the dirty tracking write duplicates", key, got)
		}
	}
	return sec
}

// TestStateRowsRoundTrip is the load-bearing test for the sectioned store: what
// the daemon writes has to be exactly what it reads back. Every other property
// here is secondary to this one.
func TestStateRowsRoundTrip(t *testing.T) {
	want := populatedSections()

	rows, err := stateRows(want)
	if err != nil {
		t.Fatalf("stateRows: %v", err)
	}
	got := decodeRowsToSections(t, rows)

	if diff := cmp.Diff(want, got); diff != "" {
		t.Errorf("state did not survive the round trip:\n%s", diff)
	}
}

// TestStateRowsAreSeparatePerPairAndBucket: the row layout is the reason this
// store is not one document. If pairs and buckets shared a row, a single
// changed pair would rewrite all of them.
func TestStateRowsAreSeparatePerPairAndBucket(t *testing.T) {
	rows, err := stateRows(populatedSections())
	if err != nil {
		t.Fatalf("stateRows: %v", err)
	}

	for _, key := range []string{
		"cursor",
		"pair:alice|UCSD", "pair:bob|PSU",
		"limit:alice|UCSD",
		"bucket:user=alice|endpoint=e1|site=UCSD|dir=download",
		"bucket:user=bob|endpoint=e2|site=PSU|dir=upload",
		"scratch:recent_transfers", "scratch:epoch_buckets", "scratch:epoch_index",
		"scratch:job_epochs", "scratch:epoch_users", "scratch:bucket_runtimes",
	} {
		if _, ok := rows[key]; !ok {
			t.Errorf("no row %q", key)
		}
	}
	if len(rows) != 12 {
		t.Errorf("%d rows, want 12: %v", len(rows), rowKeys(rows))
	}
}

// TestPairRowsAreQueryable: putting the control loop's conclusions in their own
// rows is only worth it if they can be read without decoding a blob.
func TestPairRowsAreQueryable(t *testing.T) {
	rows, err := stateRows(populatedSections())
	if err != nil {
		t.Fatalf("stateRows: %v", err)
	}
	// stateRows renders in the old form, because that is what dbrpc's writes
	// parse; see DBStateStore.Save.
	ad, err := classad.ParseOld(rows["pair:alice|UCSD"])
	if err != nil {
		t.Fatalf("parse: %v", err)
	}
	if kind, _ := ad.EvaluateAttrString(attrKind); kind != kindPair {
		t.Errorf("Kind = %q, want %q", kind, kindPair)
	}
	if key, _ := ad.EvaluateAttrString(attrPairKey); key != "alice|UCSD" {
		t.Errorf("PairKey = %q, want alice|UCSD", key)
	}
	if got := attrFloat(ad, attrCapacity); got != 4.5 {
		t.Errorf("CapacityGBPerMin = %v, want 4.5", got)
	}
}

// TestDiffRowsWritesOnlyWhatMoved is the property the row layout exists for.
// The state is saved every poll cycle and most of it does not change.
func TestDiffRowsWritesOnlyWhatMoved(t *testing.T) {
	sec := populatedSections()
	rows, err := stateRows(sec)
	if err != nil {
		t.Fatalf("stateRows: %v", err)
	}

	// First save: nothing is known to be stored, so everything is written.
	changed, gone := diffRows(nil, rows)
	if len(changed) != len(rows) || len(gone) != 0 {
		t.Fatalf("first save wrote %d of %d rows and deleted %d; want all written, none deleted",
			len(changed), len(rows), len(gone))
	}

	// Unchanged state: no round trip at all.
	changed, gone = diffRows(rows, rows)
	if len(changed) != 0 || len(gone) != 0 {
		t.Errorf("an unchanged state wrote %d rows and deleted %d; want nothing", len(changed), len(gone))
	}

	// One pair's capacity moves. Exactly one row may be written.
	sec.PairStates["alice|UCSD"] = control.PairState{CapacityGBPerMin: 9, LastUpdated: time.Unix(1_700_000_500, 0)}
	next, err := stateRows(sec)
	if err != nil {
		t.Fatalf("stateRows: %v", err)
	}
	changed, gone = diffRows(rows, next)
	if len(gone) != 0 {
		t.Errorf("deleted %v on a pure update", gone)
	}
	if len(changed) != 1 {
		t.Fatalf("%d rows written for one changed pair, want 1: %v", len(changed), rowKeys(changed))
	}
	if _, ok := changed["pair:alice|UCSD"]; !ok {
		t.Errorf("wrote %v, want pair:alice|UCSD", rowKeys(changed))
	}
}

// TestDiffRowsDeletesWhatWentAway: a pair that goes quiet, or a bucket that
// ages out of the window, has to leave the table -- otherwise it accumulates
// state the daemon no longer believes, and a restart would adopt it.
func TestDiffRowsDeletesWhatWentAway(t *testing.T) {
	sec := populatedSections()
	rows, err := stateRows(sec)
	if err != nil {
		t.Fatalf("stateRows: %v", err)
	}

	delete(sec.PairStates, "bob|PSU")
	delete(sec.Buckets, "user=bob|endpoint=e2|site=PSU|dir=upload")
	next, err := stateRows(sec)
	if err != nil {
		t.Fatalf("stateRows: %v", err)
	}

	changed, gone := diffRows(rows, next)
	if len(changed) != 0 {
		t.Errorf("wrote %v on a pure deletion", rowKeys(changed))
	}
	want := []string{"bucket:user=bob|endpoint=e2|site=PSU|dir=upload", "pair:bob|PSU"}
	if diff := cmp.Diff(want, gone); diff != "" {
		t.Errorf("deleted rows:\n%s", diff)
	}
}

// TestUnknownRowKindIsAnError: a table written by a newer pelican-man must not
// be read as a partial state. Dropping the rows it does not recognize would
// present that partial state as complete, and the next save would delete the
// rest.
func TestUnknownRowKindIsAnError(t *testing.T) {
	sec := state.Sections{
		Buckets:     map[string]state.SummaryStats{},
		PairStates:  map[string]control.PairState{},
		LimitStates: map[string]control.PairState{},
	}

	ad := classad.New()
	ad.InsertAttrString(attrKind, "something_new")
	if _, err := applyStateRow(&sec, ad); err == nil {
		t.Error("an unrecognized row Kind was accepted")
	}

	ad = classad.New()
	ad.InsertAttrString(attrKind, kindScratch)
	ad.InsertAttrString(attrSection, "something_new")
	ad.InsertAttrString(attrPayload, "{}")
	if _, err := applyStateRow(&sec, ad); err == nil {
		t.Error("an unrecognized scratch section was accepted")
	}
}

// TestKeylessRowsAreRejected: the key is how a row is addressed, so a row
// without one cannot be updated or deleted later.
func TestKeylessRowsAreRejected(t *testing.T) {
	sec := state.Sections{
		Buckets:     map[string]state.SummaryStats{},
		PairStates:  map[string]control.PairState{},
		LimitStates: map[string]control.PairState{},
	}
	for _, kind := range []string{kindPair, kindLimit, kindBucket} {
		ad := classad.New()
		ad.InsertAttrString(attrKind, kind)
		if _, err := applyStateRow(&sec, ad); err == nil {
			t.Errorf("a %s row with no key was accepted", kind)
		}
	}
}

// TestEmptyStateStillWritesACursor: a daemon that has seen nothing must still
// record that, so a restart does not mistake "nothing stored" for "never ran".
func TestEmptyStateStillWritesACursor(t *testing.T) {
	rows, err := stateRows(state.Sections{})
	if err != nil {
		t.Fatalf("stateRows: %v", err)
	}
	if _, ok := rows["cursor"]; !ok {
		t.Error("no cursor row for an empty state")
	}
}

func TestOpenDBStateStoreRejectsIncompleteConfig(t *testing.T) {
	if _, err := OpenDBStateStore(StateDBConfig{Config: testCondorConfig(t)}); err == nil {
		t.Error("accepted an empty address")
	}
	if _, err := OpenDBStateStore(StateDBConfig{Address: "h:1"}); err == nil {
		t.Error("accepted a nil HTCondor configuration")
	}
	s, err := OpenDBStateStore(StateDBConfig{Address: "h:1", Config: testCondorConfig(t)})
	if err != nil {
		t.Fatalf("OpenDBStateStore: %v", err)
	}
	if s.table != "pelican_manager_state" {
		t.Errorf("table = %q, want pelican_manager_state", s.table)
	}
}

func rowKeys(rows map[string]string) []string {
	keys := make([]string, 0, len(rows))
	for k := range rows {
		keys = append(keys, k)
	}
	return keys
}

// testCondorConfig builds an in-memory HTCondor configuration, so tests do not
// need a condor_config on disk.
func testCondorConfig(t *testing.T) *condorconfig.Config {
	t.Helper()
	t.Setenv("CONDOR_CONFIG", "ONLY_ENV")
	cfg, err := condorconfig.New()
	if err != nil {
		t.Fatalf("condor config: %v", err)
	}
	return cfg
}
