package daemon

import (
	"testing"
	"time"

	"github.com/bbockelm/pelican-ap-manager/internal/control"
	"github.com/bbockelm/pelican-ap-manager/internal/state"
	"github.com/bbockelm/pelican-ap-manager/internal/stats"
)

func TestPairMetricsUsesTrackerAndState(t *testing.T) {
	cfg := control.DefaultConfig()
	st := state.New()
	tracker := stats.NewTracker(time.Hour)

	// Add two successful transfers and one failure to drive error rate 1/3.
	tracker.Add("alice", []stats.ProcessedTransfer{
		{User: "alice", Source: "srcA", Destination: "dstA", Success: true, Bytes: 1000, Duration: time.Second, EndedAt: time.Now()},
		{User: "alice", Source: "srcA", Destination: "dstA", Success: true, Bytes: 1000, Duration: time.Second, EndedAt: time.Now()},
		{User: "alice", Source: "srcA", Destination: "dstA", Success: false, Bytes: 1000, Duration: time.Second, EndedAt: time.Now()},
	})

	// Stage-in percent: two epochs with 10s stage-in over 100s runtime => 10%.
	bucket := state.KeyString(state.SummaryKey{User: "alice", Endpoint: "ep", Site: "site", Direction: state.DirectionDownload})
	now := time.Now()
	st.AppendEpochBuckets(24*time.Hour, bucket, []state.TransferEpochRef{
		{Epoch: state.EpochID{ClusterID: 1, ProcID: 1, RunInstanceID: 1}, EndedAt: now, DurationSec: 10, JobRuntimeSec: 100, Source: "srcA", Destination: "dstA"},
		{Epoch: state.EpochID{ClusterID: 2, ProcID: 1, RunInstanceID: 1}, EndedAt: now, DurationSec: 10, JobRuntimeSec: 100, Source: "srcA", Destination: "dstA"},
	})

	m := pairMetrics(cfg, st, tracker, "srcA", "dstA")

	if got, want := m.ErrorRate, 1.0/3.0; got < want-1e-6 || got > want+1e-6 {
		t.Fatalf("error rate=%f want %f", got, want)
	}
	if got := m.CostPct; got < 9.9 || got > 10.1 {
		t.Fatalf("cost pct=%f want ~10", got)
	}
}
