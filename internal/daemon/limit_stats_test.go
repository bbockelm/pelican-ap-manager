package daemon

import (
	"context"
	"testing"

	"github.com/bbockelm/pelican-ap-manager/internal/ratelimit"
)

// TestInstalledStatsDistinguishNotMatchingFromNotThrottling is the operator's
// question: my rule is configured, so is it doing anything?
//
// Before these counters were read, the two answers looked identical. A limit
// whose expression matches no job in the queue -- the rule names the wrong user,
// or its expression fails to evaluate -- throttles nothing and is silent. A
// limit that matches every job but has never exceeded its rate also throttles
// nothing and is equally silent. Meanwhile the per-poll line kept reporting
// "installable=2" in both cases, because that counts what the daemon intends,
// not what the schedd is doing.
//
// JobsAllowed is what separates them, and the schedd has been returning it on
// every query the daemon already makes.
func TestInstalledStatsDistinguishNotMatchingFromNotThrottling(t *testing.T) {
	ctx := context.Background()

	install := func(t *testing.T) (*limitManager, *fakeSchedd, string) {
		t.Helper()
		f := newFakeSchedd()
		m := newTestLimitManager(t, f)
		if err := m.reconcile(ctx, []ratelimit.Rule{testRule(1)}); err != nil {
			t.Fatalf("reconcile: %v", err)
		}
		var uuid string
		for id := range f.installed {
			uuid = id
		}
		if uuid == "" {
			t.Fatal("nothing installed")
		}
		return m, f, uuid
	}

	t.Run("matching nothing", func(t *testing.T) {
		m, _, _ := install(t)
		if err := m.refreshLimitStats(ctx); err != nil {
			t.Fatalf("refresh: %v", err)
		}
		st := m.installedStats()
		if st.Installed != 1 {
			t.Errorf("Installed = %d, want 1", st.Installed)
		}
		if st.Allowed != 0 || st.Skipped != 0 {
			t.Errorf("allowed=%d skipped=%d, want both zero for a limit that matched nothing",
				st.Allowed, st.Skipped)
		}
	})

	t.Run("matching but under the rate", func(t *testing.T) {
		m, f, uuid := install(t)
		f.installed[uuid].JobsAllowed = 7 // the schedd let seven jobs through
		if err := m.refreshLimitStats(ctx); err != nil {
			t.Fatalf("refresh: %v", err)
		}
		st := m.installedStats()
		if st.Allowed != 7 {
			t.Errorf("Allowed = %d, want 7 -- without this the operator cannot tell this apart from a rule matching nothing", st.Allowed)
		}
		if st.Skipped != 0 {
			t.Errorf("Skipped = %d, want 0: under the rate, nothing is held back", st.Skipped)
		}
	})

	t.Run("throttling", func(t *testing.T) {
		m, f, uuid := install(t)
		f.installed[uuid].JobsAllowed = 1
		f.installed[uuid].JobsSkipped = 4
		f.installed[uuid].MatchesIgnored = 4
		f.installed[uuid].LastIgnored = 1_700_000_000
		if err := m.refreshLimitStats(ctx); err != nil {
			t.Fatalf("refresh: %v", err)
		}
		st := m.installedStats()
		if st.Allowed != 1 || st.Skipped != 4 || st.Ignored != 4 {
			t.Errorf("allowed=%d skipped=%d ignored=%d, want 1/4/4", st.Allowed, st.Skipped, st.Ignored)
		}
	})
}

// TestInstalledStatsReportTheScheddNotTheIntent: a limit the schedd has dropped
// must stop being counted as installed. The daemon still wants it -- it stays in
// the rule set and is reinstalled next cycle -- but reporting it as installed in
// the meantime is the exact reassurance an operator must not be given.
func TestInstalledStatsReportTheScheddNotTheIntent(t *testing.T) {
	ctx := context.Background()
	f := newFakeSchedd()
	m := newTestLimitManager(t, f)
	if err := m.reconcile(ctx, []ratelimit.Rule{testRule(1)}); err != nil {
		t.Fatalf("reconcile: %v", err)
	}
	if st := m.installedStats(); st.Installed != 1 {
		t.Fatalf("Installed = %d after reconcile, want 1", st.Installed)
	}

	// The lease runs out while the daemon is not looking.
	clear(f.installed)
	if err := m.refreshLimitStats(ctx); err != nil {
		t.Fatalf("refresh: %v", err)
	}
	if st := m.installedStats(); st.Installed != 0 {
		t.Errorf("Installed = %d after the schedd dropped the limit, want 0", st.Installed)
	}
}
