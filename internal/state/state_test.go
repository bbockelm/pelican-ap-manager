package state

import (
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
