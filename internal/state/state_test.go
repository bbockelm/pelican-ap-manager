package state

import (
	"path/filepath"
	"testing"
	"time"
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
