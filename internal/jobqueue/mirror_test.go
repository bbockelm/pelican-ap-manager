package jobqueue

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/PelicanPlatform/classad/classad"
	htcondor "github.com/bbockelm/golang-htcondor"
	htcondorlogging "github.com/bbockelm/golang-htcondor/logging"
	"github.com/bbockelm/pelican-ap-manager/internal/condor"
	"github.com/bbockelm/pelican-ap-manager/internal/state"
)

// fakeClient stands in for the condor client, recording what was asked of it.
type fakeClient struct {
	canServe  bool
	queryErr  error
	queries   int
	canAsked  int
	returnAds []*classad.ClassAd
}

func (f *fakeClient) FetchTransferEpochs(s state.EpochID, _ time.Time) ([]condor.TransferRecord, state.EpochID, error) {
	return nil, s, nil
}
func (f *fakeClient) FetchJobEpochs(s state.EpochID, _ time.Time) ([]condor.JobEpochRecord, state.EpochID, error) {
	return nil, s, nil
}
func (f *fakeClient) AdvertiseClassAds([]map[string]any) error { return nil }
func (f *fakeClient) LocateSchedd(context.Context) (*htcondor.Schedd, error) {
	return nil, fmt.Errorf("no schedd")
}
func (f *fakeClient) QueryJobs(context.Context, string, []string) ([]*classad.ClassAd, error) {
	f.queries++
	if f.queryErr != nil {
		return nil, f.queryErr
	}
	return f.returnAds, nil
}
func (f *fakeClient) CanServeJobs(context.Context) bool { f.canAsked++; return f.canServe }

func jobAd(t *testing.T, cluster, proc int) *classad.ClassAd {
	t.Helper()
	ad := classad.New()
	ad.InsertAttr("ClusterId", int64(cluster))
	ad.InsertAttr("ProcId", int64(proc))
	ad.InsertAttrString("Owner", "alice")
	return ad
}

func testMirror(t *testing.T, c condor.CondorClient) *Mirror {
	t.Helper()
	logger, err := htcondorlogging.New(&htcondorlogging.Config{})
	if err != nil {
		t.Fatalf("logger: %v", err)
	}
	// No log path, so there is no local reader to prefer: the source choice
	// under test is mirror-versus-schedd.
	m, err := NewMirror("", c, logger)
	if err != nil {
		t.Fatalf("NewMirror: %v", err)
	}
	return m
}

// TestSyncPrefersTheMirrorWhenItIsCurrent is the point of the change: when
// htcondordb is caught up on the live queue, this daemon should not also be
// parsing job_queue.log. One process on the access point reads that file.
func TestSyncPrefersTheMirrorWhenItIsCurrent(t *testing.T) {
	c := &fakeClient{canServe: true, returnAds: []*classad.ClassAd{jobAd(t, 7, 0)}}
	m := testMirror(t, c)

	if err := m.Sync(context.Background()); err != nil {
		t.Fatalf("Sync: %v", err)
	}
	if c.canAsked == 0 {
		t.Error("never asked whether the mirror could serve the queue")
	}
	if c.queries != 1 {
		t.Errorf("%d job queries, want 1", c.queries)
	}
	if got := len(m.Snapshot()); got != 1 {
		t.Errorf("%d jobs in the snapshot, want 1", got)
	}
}

// TestSyncUsesTheScheddPathWhenTheMirrorIsBehind: a mirror that is behind must
// not be read, and the daemon still has to learn the queue from somewhere.
func TestSyncUsesTheScheddPathWhenTheMirrorIsBehind(t *testing.T) {
	c := &fakeClient{canServe: false, returnAds: []*classad.ClassAd{jobAd(t, 9, 1)}}
	m := testMirror(t, c)

	if err := m.Sync(context.Background()); err != nil {
		t.Fatalf("Sync: %v", err)
	}
	// Readiness must actually be consulted. Without this the test cannot tell
	// "asked, was told no, took the fallback" from "never asked and read the
	// mirror anyway": with no log reader configured both paths end in the same
	// QueryJobs call and produce the same snapshot, so the outcome alone proves
	// nothing.
	if c.canAsked == 0 {
		t.Error("read the queue without asking whether the mirror was current")
	}
	// With no log reader configured, the fallback is the client's own query --
	// which the client routes to the schedd when the mirror is behind.
	if c.queries != 1 {
		t.Errorf("%d job queries, want 1", c.queries)
	}
	if got := len(m.Snapshot()); got != 1 {
		t.Errorf("%d jobs, want 1", got)
	}
}

// TestSyncSurvivesBothSourcesFailing: the client falls back to the schedd
// internally, so an error from it means both failed. Sync must report rather
// than wedge, and must not leave the snapshot half-written.
func TestSyncSurvivesBothSourcesFailing(t *testing.T) {
	c := &fakeClient{canServe: true, queryErr: fmt.Errorf("mirror and schedd both unreachable")}
	m := testMirror(t, c)

	err := m.Sync(context.Background())
	if err == nil {
		t.Fatal("Sync reported success with no source available")
	}
	if got := len(m.Snapshot()); got != 0 {
		t.Errorf("%d jobs after a failed sync, want 0", got)
	}
}

// TestSyncDoesNotAskAPlainScheddClient: a client with no mirror does not
// implement the optional interface, and must take the original path without
// anything being asked of it.
func TestSyncDoesNotAskAPlainScheddClient(t *testing.T) {
	plain := &plainClient{fakeClient{returnAds: []*classad.ClassAd{jobAd(t, 3, 0)}}}
	m := testMirror(t, plain)

	if err := m.Sync(context.Background()); err != nil {
		t.Fatalf("Sync: %v", err)
	}
	if plain.canAsked != 0 {
		t.Error("asked a plain schedd client whether it could serve the mirror")
	}
	if plain.queries != 1 {
		t.Errorf("%d queries, want 1", plain.queries)
	}
}

// plainClient is a client with no mirror: it deliberately does NOT implement
// CanServeJobs, by shadowing it away.
type plainClient struct{ fakeClient }

func (p *plainClient) CanServeJobs() bool { return false } // different signature: not the interface
