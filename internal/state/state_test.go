package state

import (
	"path/filepath"
	"reflect"
	"testing"
	"time"

	"github.com/bbockelm/pelican-ap-manager/internal/control"
	"github.com/google/go-cmp/cmp"
)

func TestBucketAndPairStageInPercent(t *testing.T) {
	now := time.Now()
	s := New()

	bucket := "user=alice|endpoint=e1|site=s1|dir=download"
	s.AppendEpochBuckets(24*time.Hour, bucket, []TransferEpochRef{
		{Epoch: EpochID{ClusterID: 1, ProcID: 1, RunInstanceID: 1}, EndedAt: now.Add(-time.Minute), DurationSec: 30, JobRuntimeSec: 300, Source: "srcA", Destination: "dstA"},
		{Epoch: EpochID{ClusterID: 2, ProcID: 1, RunInstanceID: 1}, EndedAt: now.Add(-2 * time.Minute), DurationSec: 60, JobRuntimeSec: 600, Source: "srcA", Destination: "dstA"},
	})

	samples, pct := s.BucketStageInPercent(24*time.Hour, bucket)
	if samples != 2 {
		t.Fatalf("bucket samples=%d want 2", samples)
	}
	if pct <= 0 || pct >= 30 {
		t.Fatalf("bucket pct=%f out of expected range", pct)
	}

	pSamples, pPct := s.PairStageInPercent(24*time.Hour, "srcA", "dstA")
	if pSamples != 2 {
		t.Fatalf("pair samples=%d want 2", pSamples)
	}
	if pPct <= 0 || pPct >= 30 {
		t.Fatalf("pair pct=%f out of expected range", pPct)
	}
}

// TestJobEpochCursorSurvivesARestart: the job-epoch cursor used to be a plain
// field on the service, so every restart re-read the whole lookback window of
// job history -- work the schedd (or the mirror) had already done, thrown away
// again because the records were already summarized.
func TestJobEpochCursorSurvivesARestart(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "state.json")

	st := New()
	cursor := EpochID{ClusterID: 91, ProcID: 4, RunInstanceID: 2}
	st.SetLastJobEpoch(cursor)
	if err := st.Save(path); err != nil {
		t.Fatalf("Save: %v", err)
	}

	reloaded, err := Load(path)
	if err != nil {
		t.Fatalf("Load: %v", err)
	}
	if got := reloaded.LastJobEpochID(); got != cursor {
		t.Errorf("LastJobEpochID after reload = %v, want %v", got, cursor)
	}
}

// TestJobEpochCursorOnlyMovesForward: the cursor is fed the newest epoch a
// fetch saw, and a fetch that failed or returned nothing reports the value it
// was given. Letting it move backwards would replay history already
// summarized, double-counting those transfers.
func TestJobEpochCursorOnlyMovesForward(t *testing.T) {
	st := New()
	ahead := EpochID{ClusterID: 91, ProcID: 4, RunInstanceID: 2}
	st.SetLastJobEpoch(ahead)

	st.SetLastJobEpoch(EpochID{ClusterID: 90})
	st.SetLastJobEpoch(EpochID{})
	if got := st.LastJobEpochID(); got != ahead {
		t.Errorf("cursor moved backwards to %v; want %v", got, ahead)
	}

	next := EpochID{ClusterID: 91, ProcID: 5}
	st.SetLastJobEpoch(next)
	if got := st.LastJobEpochID(); got != next {
		t.Errorf("cursor = %v, want %v", got, next)
	}
}

// TestSectionsCoverEveryField is the guard on the whole sectioned-store design.
//
// A field added to State but not to Sections would be dropped on every save,
// and the daemon would look perfectly healthy until a restart lost it. This
// compares the two structs by name so that omission is a build-time-ish
// failure rather than something noticed in production months later.
func TestSectionsCoverEveryField(t *testing.T) {
	inSections := map[string]bool{}
	secT := reflect.TypeOf(Sections{})
	for i := 0; i < secT.NumField(); i++ {
		inSections[secT.Field(i).Name] = true
	}

	stT := reflect.TypeOf(State{})
	for i := 0; i < stT.NumField(); i++ {
		f := stT.Field(i)
		if !f.IsExported() {
			continue // the mutex
		}
		if !inSections[f.Name] {
			t.Errorf("State.%s has no Sections field: it would be silently dropped on every save", f.Name)
		}
	}

	// And the reverse, so a section cannot outlive the field it persists.
	for name := range inSections {
		if _, ok := stT.FieldByName(name); !ok {
			t.Errorf("Sections.%s has no matching State field", name)
		}
	}
}

// TestSectionsRoundTripPreservesEverything checks that the copy is faithful for
// a fully-populated state, not just structurally complete.
func TestSectionsRoundTripPreservesEverything(t *testing.T) {
	original := populatedState(t)

	restored := New()
	restored.RestoreSections(original.Sections())

	if diff := cmp.Diff(original.Sections(), restored.Sections()); diff != "" {
		t.Errorf("round trip changed the state:\n%s", diff)
	}
}

// TestSectionsAreCopiesNotAliases: a store holds sections while the poll loop
// keeps mutating the live state. Sharing the backing maps would let a write
// race with a save, and would let a later mutation retroactively change what
// was "saved".
func TestSectionsAreCopiesNotAliases(t *testing.T) {
	st := populatedState(t)
	snapshot := st.Sections()

	st.RestoreSections(Sections{}) // wipe the live state entirely

	if len(snapshot.Buckets) == 0 || len(snapshot.PairStates) == 0 || len(snapshot.RecentTransfers) == 0 {
		t.Fatal("the snapshot was emptied along with the live state; Sections is aliasing its maps")
	}

	// Slices inside the maps too, which a shallow map copy would still share.
	entries := snapshot.RecentTransfers["alice"]
	if len(entries) == 0 {
		t.Fatal("no recent transfers in the snapshot")
	}
	before := entries[0].Bytes
	st.RestoreSections(snapshot)
	live := st.Sections()
	live.RecentTransfers["alice"][0].Bytes = before + 1
	if snapshot.RecentTransfers["alice"][0].Bytes != before {
		t.Error("mutating the live state changed the snapshot; the slices inside the maps are shared")
	}
}

// populatedState builds a state with every section non-empty, so a round-trip
// test cannot pass by preserving only the parts that happen to be set.
func populatedState(t *testing.T) *State {
	t.Helper()
	now := time.Unix(1_700_000_000, 0)

	st := New()
	st.RestoreSections(Sections{
		LastEpoch:    EpochID{ClusterID: 12, ProcID: 1, RunInstanceID: 3},
		LastJobEpoch: EpochID{ClusterID: 14, ProcID: 2, RunInstanceID: 1},
		Buckets: map[string]SummaryStats{
			"user=alice|endpoint=e1|site=UCSD|dir=download": {
				Successes: 7, Failures: 2,
				SuccessBytes: 1 << 30, FailureBytes: 1 << 20,
				SuccessDurationSec: 12.5, FailureDurationSec: 0.25,
				LastUpdated: now,
				Federations: map[string]FederationStats{
					"osdf": {Successes: 5, Failures: 1, SuccessBytes: 999, SuccessDurationSec: 1.5},
				},
			},
		},
		RecentTransfers: map[string][]TransferHistoryEntry{
			"alice": {{
				User: "alice", Site: "UCSD", Source: "osdf:///a", Destination: "./a",
				Direction: "download", Bytes: 4096, DurationSeconds: 1.5, Success: true,
			}},
		},
		EpochBuckets:   map[string][]TransferEpochRef{"user=alice": {{}}},
		EpochIndex:     map[string]string{"12.1.3": "user=alice"},
		JobEpochs:      map[string]JobEpochSample{"12.1.3": {}},
		EpochUsers:     map[string]string{"12.1.3": "alice"},
		BucketRuntimes: map[string][]BucketRuntimeSample{"user=alice": {{}}},
		PairStates:     map[string]control.PairState{"alice|UCSD": {CapacityGBPerMin: 4.5, LastUpdated: now}},
		LimitStates:    map[string]control.PairState{"alice|UCSD": {CapacityGBPerMin: 2, LastUpdated: now}},
	})
	return st
}
