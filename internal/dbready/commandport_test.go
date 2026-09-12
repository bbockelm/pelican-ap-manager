package dbready

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/PelicanPlatform/classad/classad"
	"github.com/bbockelm/golang-htcondor/config"
	"github.com/bbockelm/golang-htcondor/webapi/dbmirror"
)

// healthAd is the ClassAd htcondordb answers a status request with: the same one
// it advertises to a collector, which is why one parser serves both paths.
func healthAd(t *testing.T, extra string) *classad.ClassAd {
	t.Helper()
	src := `[ MyType = "HTCondorDB"; Name = "db@ap"; MyAddress = "<127.0.0.1:9618>"; ` + extra + ` ]`
	ad, err := classad.Parse(src)
	if err != nil {
		t.Fatalf("parse: %v", err)
	}
	return ad
}

func currentJobQueue() string {
	return fmt.Sprintf(`JobQueueCaughtUp = true; JobQueueLagBytes = 0;
		JobQueueLastSyncTime = %d; JobQueueSecondsSinceSync = 0`, time.Now().Unix())
}

// TestCommandPortGatesWithoutACollector is the case the command-port path exists
// for: an access point points pelican-man at a database by address file and runs
// no collector, so there is no advertisement to judge the mirror by.
//
// Before, that configuration read the mirror ungated -- a query that succeeds
// against a database hours behind, returning its contents as the answer with no
// error and no log line. Asking the database directly is what makes the gate
// available everywhere a mirror read is.
func TestCommandPortGatesWithoutACollector(t *testing.T) {
	c := New(Options{DBAddress: "<127.0.0.1:9618>", Config: &config.Config{}})
	c.queryStatus = func(context.Context, *config.Config, string) (*classad.ClassAd, error) {
		return healthAd(t, currentJobQueue()), nil
	}

	if ready, why := c.Ready(context.Background(), string(SourceJobs)); !ready {
		t.Errorf("a current mirror was declined with no collector: %s", why)
	}

	// And the same path must still decline a mirror that is behind, or it is not
	// a gate -- it is a more elaborate way of always saying yes.
	stale := New(Options{DBAddress: "<127.0.0.1:9618>", Config: &config.Config{}})
	stale.queryStatus = func(context.Context, *config.Config, string) (*classad.ClassAd, error) {
		return healthAd(t, fmt.Sprintf(`JobQueueCaughtUp = true; JobQueueLagBytes = 0;
			JobQueueLastSyncTime = %d; JobQueueSecondsSinceSync = 3600`, time.Now().Unix())), nil
	}
	if ready, _ := stale.Ready(context.Background(), string(SourceJobs)); ready {
		t.Error("an hour-stale mirror was accepted over the command port")
	}
}

// TestCommandPortFailureDeclines: if the database cannot be reached or answers
// an error, the read goes to the schedd. An unreachable database is exactly when
// guessing "current" is least defensible.
func TestCommandPortFailureDeclines(t *testing.T) {
	c := New(Options{DBAddress: "<127.0.0.1:9618>", Config: &config.Config{}})
	c.queryStatus = func(context.Context, *config.Config, string) (*classad.ClassAd, error) {
		return nil, fmt.Errorf("connection refused")
	}
	ready, why := c.Ready(context.Background(), string(SourceJobs))
	if ready {
		t.Error("reported ready when the status request failed")
	}
	if why == "" {
		t.Error("gave no reason for declining")
	}
}

// TestStatusIsCachedAcrossSources: the gate is consulted once per source per
// poll. Without a cache a one-second poll opens three connections a second to
// ask a question whose answer moves on the order of the tolerance.
func TestStatusIsCachedAcrossSources(t *testing.T) {
	c := New(Options{DBAddress: "<127.0.0.1:9618>", Config: &config.Config{}})
	var calls int
	c.queryStatus = func(context.Context, *config.Config, string) (*classad.ClassAd, error) {
		calls++
		return healthAd(t, currentJobQueue()), nil
	}

	ctx := context.Background()
	for range 3 {
		for _, src := range []Source{SourceJobs, SourceHistory, SourceEpoch} {
			c.Ready(ctx, string(src))
		}
	}
	if calls != 1 {
		t.Errorf("%d status requests for 9 readiness checks, want 1 (the rest from cache)", calls)
	}

	// The cache is a rate limit, not a freeze: past the TTL it asks again, or a
	// mirror that fell behind would be trusted indefinitely.
	c.mu.Lock()
	c.infoAt = time.Now().Add(-2 * DefaultStatusTTL)
	c.mu.Unlock()
	c.Ready(ctx, string(SourceJobs))
	if calls != 2 {
		t.Errorf("%d status requests after the TTL expired, want 2", calls)
	}
}

// TestCollectorIsPreferredAndTheCommandPortIsTheFallback pins the order, and
// the reason the fallback triggers on failure rather than on configuration.
//
// COLLECTOR_HOST has a default, so an access point running no collector still
// names one. Keying the fallback on "no collector configured" would therefore
// never fire in the deployment it exists for: discovery would fail against a
// collector that is not there, and the read would fall back to the schedd
// forever with a database sitting right next to it, answering.
func TestCollectorIsPreferredAndTheCommandPortIsTheFallback(t *testing.T) {
	t.Run("collector answers: the command port is not asked", func(t *testing.T) {
		c := New(Options{DBAddress: "<127.0.0.1:9618>", Config: &config.Config{}})
		c.discover = func(context.Context) (*dbmirror.Info, error) {
			return dbmirror.ParseAd(healthAd(t, currentJobQueue())), nil
		}
		var asked bool
		c.queryStatus = func(context.Context, *config.Config, string) (*classad.ClassAd, error) {
			asked = true
			return healthAd(t, currentJobQueue()), nil
		}

		if ready, why := c.Ready(context.Background(), string(SourceJobs)); !ready {
			t.Errorf("declined a current mirror the collector advertised: %s", why)
		}
		if asked {
			t.Error("asked the command port while the collector was answering")
		}
	})

	t.Run("collector does not answer: the command port does", func(t *testing.T) {
		c := New(Options{DBAddress: "<127.0.0.1:9618>", Config: &config.Config{}})
		c.discover = func(context.Context) (*dbmirror.Info, error) {
			return nil, fmt.Errorf("no htcondordb is advertising")
		}
		var asked bool
		c.queryStatus = func(context.Context, *config.Config, string) (*classad.ClassAd, error) {
			asked = true
			return healthAd(t, currentJobQueue()), nil
		}

		if ready, why := c.Ready(context.Background(), string(SourceJobs)); !ready {
			t.Errorf("declined with a reachable database and no collector: %s", why)
		}
		if !asked {
			t.Error("did not fall back to the command port when discovery failed")
		}
	})

	t.Run("neither answers: decline", func(t *testing.T) {
		c := New(Options{Config: &config.Config{}})
		c.discover = func(context.Context) (*dbmirror.Info, error) {
			return nil, fmt.Errorf("no htcondordb is advertising")
		}
		if ready, why := c.Ready(context.Background(), string(SourceJobs)); ready || why == "" {
			t.Errorf("ready=%v why=%q with nothing reachable", ready, why)
		}
	})
}

// TestAddressFileIsResolvedPerRequest: an access point points pelican-man at
// htcondordb's address file, not at a literal address -- the config is written
// before the daemon starts and the port is not known until then. The address
// inside also changes on restart, so resolving once at startup would leave the
// gate dialling a port nothing listens on until pelican-man itself restarted.
func TestAddressFileIsResolvedPerRequest(t *testing.T) {
	dir := t.TempDir()
	addrFile := filepath.Join(dir, ".htcondordb_address")
	if err := os.WriteFile(addrFile, []byte("<127.0.0.1:7001?sock=htcondordb>\n"), 0o644); err != nil {
		t.Fatal(err)
	}

	c := New(Options{DBAddress: addrFile, Config: &config.Config{}})
	var dialled []string
	c.queryStatus = func(_ context.Context, _ *config.Config, addr string) (*classad.ClassAd, error) {
		dialled = append(dialled, addr)
		return healthAd(t, currentJobQueue()), nil
	}

	if ready, why := c.Ready(context.Background(), string(SourceJobs)); !ready {
		t.Fatalf("declined with a current mirror behind an address file: %s", why)
	}

	// The daemon restarts on a new port and rewrites the file.
	if err := os.WriteFile(addrFile, []byte("<127.0.0.1:7002?sock=htcondordb>\n"), 0o644); err != nil {
		t.Fatal(err)
	}
	c.mu.Lock()
	c.infoAt = time.Now().Add(-2 * DefaultStatusTTL) // past the cache, so it dials again
	c.mu.Unlock()
	if ready, why := c.Ready(context.Background(), string(SourceJobs)); !ready {
		t.Fatalf("declined after the database restarted on a new port: %s", why)
	}

	if len(dialled) != 2 || dialled[0] != "<127.0.0.1:7001?sock=htcondordb>" || dialled[1] != "<127.0.0.1:7002?sock=htcondordb>" {
		t.Errorf("dialled %q, want the address read fresh from the file each time", dialled)
	}
}
