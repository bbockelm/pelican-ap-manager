package daemon

import (
	"context"
	"fmt"
	"path/filepath"
	"testing"
	"time"

	"github.com/PelicanPlatform/classad/classad"
	htcondor "github.com/bbockelm/golang-htcondor"
	htcondorlogging "github.com/bbockelm/golang-htcondor/logging"

	"github.com/bbockelm/pelican-ap-manager/internal/condor"
	"github.com/bbockelm/pelican-ap-manager/internal/state"
)

// replayClient hands back the same records on every fetch, as a source that
// redelivers already-seen rows does: a read resumed from a cursor that has not
// advanced, a source re-scanned after a gap, or a watch stream, which is
// at-least-once by contract and so produces this routinely.
type replayClient struct {
	transfers []condor.TransferRecord
	jobs      []condor.JobEpochRecord
}

func (r *replayClient) FetchTransferEpochs(since state.EpochID, _ time.Time) ([]condor.TransferRecord, state.EpochID, error) {
	return r.transfers, since, nil
}
func (r *replayClient) FetchJobEpochs(since state.EpochID, _ time.Time) ([]condor.JobEpochRecord, state.EpochID, error) {
	return r.jobs, since, nil
}
func (r *replayClient) AdvertiseClassAds([]map[string]any) error { return nil }
func (r *replayClient) QueryJobs(context.Context, string, []string) ([]*classad.ClassAd, error) {
	return nil, nil
}
func (r *replayClient) LocateSchedd(context.Context) (*htcondor.Schedd, error) {
	return nil, fmt.Errorf("no schedd in this test")
}

// TestIngestIsIdempotent: folding the same archive record in twice must not move
// the counters.
//
// Every counter this daemon keeps is an accumulator -- SuccessBytes += bytes,
// appends to per-bucket slices -- and they were written against a cursor-based
// poll that delivers each row exactly once. Nothing downstream can tell a second
// copy of a row from a second row that looks identical, so a duplicate inflates
// the transfer rates the control loop throttles on, with no error and no log
// line to notice: the totals simply read high and someone gets throttled for
// traffic they did not send.
//
// The assertion is that the second pass changes nothing, compared against the
// state after the first. Asserting specific totals instead would pass on a
// daemon that dropped every record.
func TestIngestIsIdempotent(t *testing.T) {
	now := time.Now().Add(-time.Minute)
	epoch := state.EpochID{ClusterID: 5, ProcID: 0, RunInstanceID: 1}

	// An epoch that produced both an output transfer and a checkpoint. They are
	// distinct rows, but a checkpoint has no direction of its own and is reported
	// as an upload, so keying on the epoch and direction alone would collapse
	// them and silently lose one.
	client := &replayClient{
		transfers: []condor.TransferRecord{
			{
				EpochID: epoch, Kind: "OUTPUT", Seq: 0,
				User: "alice", Endpoint: "e1", Site: "UCSD", Direction: "upload",
				Success: true, EndedAt: now,
				Files: []condor.TransferFile{{
					URL: "osdf:///out/a", LastEndpoint: "e1", Success: true, TotalBytes: 1024,
					Bytes: 1024, Start: now.Add(-time.Second), End: now,
					Attempts: []condor.TransferAttempt{{Endpoint: "e1", Bytes: 1024, DurationSec: 1}},
				}},
			},
			{
				EpochID: epoch, Kind: "CHECKPOINT", Seq: 0,
				User: "alice", Endpoint: "e1", Site: "UCSD", Direction: "upload",
				Success: true, EndedAt: now,
				Files: []condor.TransferFile{{
					URL: "osdf:///ckpt/a", LastEndpoint: "e1", Success: true, TotalBytes: 2048,
					Bytes: 2048, Start: now.Add(-time.Second), End: now,
					Attempts: []condor.TransferAttempt{{Endpoint: "e1", Bytes: 2048, DurationSec: 1}},
				}},
			},
		},
		jobs: []condor.JobEpochRecord{
			{EpochID: epoch, User: "alice", Site: "UCSD", Runtime: 30 * time.Second, Success: true, EndedAt: now},
		},
	}

	logger, err := htcondorlogging.New(&htcondorlogging.Config{})
	if err != nil {
		t.Fatalf("logger: %v", err)
	}
	dir := t.TempDir()
	st := state.New()
	svc := NewService(client, st, filepath.Join(dir, "state.json"),
		time.Second, time.Second, time.Hour, time.Hour,
		nil, nil, "", nil, logger, filepath.Join(dir, "info.json"), "", "", true)

	ctx := context.Background()
	svc.pollOnce(ctx)
	first := snapshotCounters(t, st)

	// Both rows must have been taken the first time: if the checkpoint collapsed
	// into the output row, the rest of this test would pass while having lost
	// half the data.
	if first.buckets == 0 {
		t.Fatal("no buckets after the first pass; the test fixture never reached ingest")
	}
	if first.bucketRuntimes == 0 {
		t.Fatal("no runtime samples after the first pass: the job-epoch path never ran, " +
			"so this test would not notice its guard being removed")
	}
	if first.successBytes != 3072 {
		t.Fatalf("SuccessBytes = %d after one pass, want 3072 (1024 output + 2048 checkpoint): "+
			"a lower number means two distinct rows of one epoch collapsed into one",
			first.successBytes)
	}

	svc.pollOnce(ctx)
	second := snapshotCounters(t, st)

	if second != first {
		t.Errorf("a replayed batch moved the counters:\n  first:  %+v\n  second: %+v", first, second)
	}
}

type counters struct {
	buckets      int
	successes    int
	successBytes int64
	epochRefs    int
	jobEpochs    int
	recent       int
	// bucketRuntimes is the job path's accumulator. JobEpochs is a map keyed by
	// epoch and so is idempotent on its own; without counting this, the job-side
	// guard could be removed entirely and this test would still pass.
	bucketRuntimes int
}

func snapshotCounters(t *testing.T, st *state.State) counters {
	t.Helper()
	sec := st.Sections()
	c := counters{buckets: len(sec.Buckets), jobEpochs: len(sec.JobEpochs)}
	for _, b := range sec.Buckets {
		c.successes += b.Successes
		c.successBytes += b.SuccessBytes
	}
	for _, refs := range sec.EpochBuckets {
		c.epochRefs += len(refs)
	}
	for _, entries := range sec.RecentTransfers {
		c.recent += len(entries)
	}
	for _, samples := range sec.BucketRuntimes {
		c.bucketRuntimes += len(samples)
	}
	return c
}
