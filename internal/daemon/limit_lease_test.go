package daemon

import (
	"context"
	"fmt"
	"testing"
	"time"

	htcondor "github.com/bbockelm/golang-htcondor"
	condorconfig "github.com/bbockelm/golang-htcondor/config"
	htcondorlogging "github.com/bbockelm/golang-htcondor/logging"
	"github.com/bbockelm/pelican-ap-manager/internal/config"
	"github.com/bbockelm/pelican-ap-manager/internal/ratelimit"
)

// fakeSchedd records what the limit manager asks of the schedd and lets a test
// decide what the schedd holds. It is the piece that was missing: without it,
// every reconcile path needed a live HTCondor, so nothing checked the lease.
type fakeSchedd struct {
	pushes []*htcondor.StartupLimitRequest

	// installed is what a query returns, keyed by UUID. A test can empty it to
	// simulate a lease running out.
	installed map[string]*htcondor.StartupLimit

	nextUUID int
	pushErr  error
}

func newFakeSchedd() *fakeSchedd {
	return &fakeSchedd{installed: map[string]*htcondor.StartupLimit{}}
}

func (f *fakeSchedd) CreateStartupLimit(_ context.Context, req *htcondor.StartupLimitRequest) (string, error) {
	f.pushes = append(f.pushes, req)
	if f.pushErr != nil {
		return "", f.pushErr
	}
	uuid := req.UUID
	if uuid == "" {
		f.nextUUID++
		uuid = fmt.Sprintf("uuid-%d", f.nextUUID)
	}
	f.installed[uuid] = &htcondor.StartupLimit{
		UUID: uuid, Tag: req.Tag, Name: req.Name, Expression: req.Expression,
		RateCount: req.RateCount, RateWindow: req.RateWindow,
	}
	return uuid, nil
}

func (f *fakeSchedd) QueryStartupLimits(_ context.Context, uuid, _ string) ([]*htcondor.StartupLimit, error) {
	if uuid != "" {
		if l, ok := f.installed[uuid]; ok {
			return []*htcondor.StartupLimit{l}, nil
		}
		return nil, nil
	}
	out := make([]*htcondor.StartupLimit, 0, len(f.installed))
	for _, l := range f.installed {
		out = append(out, l)
	}
	return out, nil
}

func newTestLimitManager(t *testing.T, schedd scheddLimits) *limitManager {
	t.Helper()
	logger, err := htcondorlogging.New(&htcondorlogging.Config{})
	if err != nil {
		t.Fatalf("logger: %v", err)
	}
	m := &limitManager{
		schedd:        schedd,
		logger:        logger,
		activeLimits:  map[string]*limitState{},
		daemonName:    "test-schedd",
		siteAttribute: "GLIDEIN_Site",
		cfg: limitConfig{
			interval:             defaultLimitInterval,
			expirationInactivity: limitExpirationInactivity,
			lease:                defaultLimitLease,
			ewmaAlpha:            ewmaAlpha,
			enabled:              true,
		},
	}
	return m
}

func testRule(rate int) ratelimit.Rule {
	return ratelimit.Rule{
		Name: "r", Origin: ratelimit.OriginStatic,
		User: "alice", Site: "UCSD",
		RateCount: rate, RateWindow: time.Minute,
	}
}

// TestLeaseIsSentInSeconds is the direct guard on the bug that started this:
// StartupLimitRequest.Expiration is seconds from now, and a Unix timestamp was
// being sent instead. The schedd clamps to STARTUP_LIMIT_MAX_EXPIRATION, so the
// mistake was invisible in production -- every limit silently came out at the
// schedd's maximum.
func TestLeaseIsSentInSeconds(t *testing.T) {
	f := newFakeSchedd()
	m := newTestLimitManager(t, f)

	if err := m.reconcile(context.Background(), []ratelimit.Rule{testRule(5)}); err != nil {
		t.Fatalf("reconcile: %v", err)
	}
	if len(f.pushes) != 1 {
		t.Fatalf("%d pushes, want 1", len(f.pushes))
	}

	got := f.pushes[0].Expiration
	if want := int(defaultLimitLease.Seconds()); got != want {
		t.Errorf("Expiration = %d, want %d (seconds from now, not a timestamp)", got, want)
	}
	// A timestamp would be ~1.8e9. Anything on that scale means the units are
	// wrong again, whatever the configured lease.
	if got > 86400 {
		t.Errorf("Expiration = %d looks like a Unix timestamp, not a duration", got)
	}
}

// TestLeaseIsRenewedEveryCycle is the property the daemon depends on: the limits
// live only as long as it keeps saying so.
func TestLeaseIsRenewedEveryCycle(t *testing.T) {
	f := newFakeSchedd()
	m := newTestLimitManager(t, f)
	rule := testRule(5)

	for i := 0; i < 3; i++ {
		if err := m.reconcile(context.Background(), []ratelimit.Rule{rule}); err != nil {
			t.Fatalf("reconcile %d: %v", i, err)
		}
	}

	if len(f.pushes) != 3 {
		t.Fatalf("%d pushes across 3 cycles, want 3: an unchanged rule must still renew its lease", len(f.pushes))
	}
	// The first creates, the rest renew the same limit rather than making new ones.
	if f.pushes[0].UUID != "" {
		t.Errorf("first push carried UUID %q, want a create", f.pushes[0].UUID)
	}
	for i, p := range f.pushes[1:] {
		if p.UUID != "uuid-1" {
			t.Errorf("push %d renewed UUID %q, want uuid-1", i+1, p.UUID)
		}
	}
	if len(f.installed) != 1 {
		t.Errorf("%d limits installed, want 1", len(f.installed))
	}
}

// TestLapsedLimitIsReinstalled covers recovery: if a limit disappears from the
// schedd -- its lease ran out while the daemon was wedged, or the schedd
// restarted -- the daemon has to put it back.
//
// The renewal does that on its own: the schedd installs under whatever UUID it
// is handed (StartupLimits[uuid] = ...), so re-pushing an unknown UUID
// re-creates the limit. That is the path this asserts, because it is the one
// that runs in practice.
func TestLapsedLimitIsReinstalled(t *testing.T) {
	f := newFakeSchedd()
	m := newTestLimitManager(t, f)
	rule := testRule(5)

	if err := m.reconcile(context.Background(), []ratelimit.Rule{rule}); err != nil {
		t.Fatalf("reconcile: %v", err)
	}

	// The limit vanishes from the schedd between cycles.
	f.installed = map[string]*htcondor.StartupLimit{}

	if err := m.reconcile(context.Background(), []ratelimit.Rule{rule}); err != nil {
		t.Fatalf("reconcile after lapse: %v", err)
	}
	if len(f.installed) != 1 {
		t.Fatalf("%d limits in the schedd after a lapse, want 1: enforcement did not recover", len(f.installed))
	}
}

// TestLimitForgottenWhenRenewalFails covers the case renewal cannot fix: the
// schedd is unreachable long enough for the lease to run out. The daemon must
// stop believing the limit exists, so that a later cycle installs a fresh one
// rather than renewing a UUID nothing is tracking.
//
// It only forgets on a *successful* query that comes back empty. A failed query
// says nothing about whether the limit is there, and forgetting on that could
// install a duplicate alongside a live limit.
func TestLimitForgottenWhenRenewalFails(t *testing.T) {
	f := newFakeSchedd()
	m := newTestLimitManager(t, f)
	rule := testRule(5)

	if err := m.reconcile(context.Background(), []ratelimit.Rule{rule}); err != nil {
		t.Fatalf("reconcile: %v", err)
	}

	// The schedd stops accepting writes and loses the limit.
	f.pushErr = fmt.Errorf("schedd unreachable")
	f.installed = map[string]*htcondor.StartupLimit{}

	if err := m.reconcile(context.Background(), []ratelimit.Rule{rule}); err != nil {
		t.Fatalf("reconcile while unreachable: %v", err)
	}
	if len(m.activeLimits) != 0 {
		t.Fatalf("daemon still believes %d limits are installed after the schedd lost them", len(m.activeLimits))
	}

	// Once writes work again, the next cycle installs a fresh limit.
	f.pushErr = nil
	if err := m.reconcile(context.Background(), []ratelimit.Rule{rule}); err != nil {
		t.Fatalf("reconcile after recovery: %v", err)
	}
	if len(f.installed) != 1 {
		t.Errorf("%d limits after recovery, want 1", len(f.installed))
	}
}

// TestLeaseNeverOutlivesTheRule keeps an expiring rule from being kept alive by
// the lease past its own deadline.
func TestLeaseNeverOutlivesTheRule(t *testing.T) {
	f := newFakeSchedd()
	m := newTestLimitManager(t, f)

	now := time.Now()
	rule := testRule(5)
	rule.ExpiresAt = now.Add(10 * time.Second) // shorter than the 60s lease

	if got, want := m.leaseFor(rule, now), 10*time.Second; got != want {
		t.Errorf("leaseFor = %v, want %v (capped by the rule's own expiry)", got, want)
	}

	rule.ExpiresAt = now.Add(time.Hour) // longer than the lease
	if got, want := m.leaseFor(rule, now), defaultLimitLease; got != want {
		t.Errorf("leaseFor = %v, want the configured lease %v", got, want)
	}

	// The schedd rejects a non-future expiration; the floor keeps a
	// nearly-expired rule from producing one.
	rule.ExpiresAt = now.Add(-time.Minute)
	if got := m.leaseFor(rule, now); got < time.Second {
		t.Errorf("leaseFor = %v, want at least 1s so the schedd accepts it", got)
	}
	_ = f
}

// TestRateChangeStillRewritesTheLimit checks that decoupling renewal from the
// change deadband did not stop rate changes from reaching the schedd.
func TestRateChangeStillRewritesTheLimit(t *testing.T) {
	f := newFakeSchedd()
	m := newTestLimitManager(t, f)

	if err := m.reconcile(context.Background(), []ratelimit.Rule{testRule(5)}); err != nil {
		t.Fatalf("reconcile: %v", err)
	}
	if err := m.reconcile(context.Background(), []ratelimit.Rule{testRule(50)}); err != nil {
		t.Fatalf("reconcile with new rate: %v", err)
	}

	last := f.pushes[len(f.pushes)-1]
	if last.RateCount != 50 {
		t.Errorf("last push RateCount = %d, want 50", last.RateCount)
	}
	if got := f.installed["uuid-1"].RateCount; got != 50 {
		t.Errorf("installed RateCount = %d, want 50", got)
	}
}

// TestConfiguredLeaseIsHonored makes the operator knob real.
func TestConfiguredLeaseIsHonored(t *testing.T) {
	f := newFakeSchedd()
	m := newTestLimitManager(t, f)
	m.cfg.lease = 5 * time.Second

	if err := m.reconcile(context.Background(), []ratelimit.Rule{testRule(5)}); err != nil {
		t.Fatalf("reconcile: %v", err)
	}
	if got := f.pushes[0].Expiration; got != 5 {
		t.Errorf("Expiration = %d, want 5", got)
	}
}

// TestRenewalIntervalLeavesRoomToFail: renewal at a third of the lease means
// two consecutive failures -- a schedd restart, a dropped connection -- still
// leave a third of the lease to recover in.
func TestRenewalIntervalLeavesRoomToFail(t *testing.T) {
	m := newTestLimitManager(t, newFakeSchedd())

	m.cfg.lease = 60 * time.Second
	if got, want := m.renewalInterval(), 20*time.Second; got != want {
		t.Errorf("renewalInterval = %v, want %v", got, want)
	}

	// The interval must follow the lease, not any other configured interval:
	// that coupling is what makes the poll and advertise intervals irrelevant to
	// whether a limit survives.
	m.cfg.lease = 15 * time.Second
	if got, want := m.renewalInterval(), 5*time.Second; got != want {
		t.Errorf("renewalInterval = %v, want %v", got, want)
	}

	// A lease so short that a third of it rounds toward zero would otherwise
	// produce a ticker that never usefully fires.
	m.cfg.lease = time.Second
	if got := m.renewalInterval(); got < time.Second {
		t.Errorf("renewalInterval = %v, want at least 1s", got)
	}
}

// TestRenewAllRefreshesWithoutReconsideringPolicy: renewal is a liveness
// signal. It must reach the schedd on its own, so that a limit survives however
// long it is between policy cycles.
func TestRenewAllRefreshesWithoutReconsideringPolicy(t *testing.T) {
	f := newFakeSchedd()
	m := newTestLimitManager(t, f)

	if err := m.reconcile(context.Background(), []ratelimit.Rule{testRule(5)}); err != nil {
		t.Fatalf("reconcile: %v", err)
	}
	pushesAfterReconcile := len(f.pushes)

	m.renewAll(context.Background())
	m.renewAll(context.Background())

	if got := len(f.pushes) - pushesAfterReconcile; got != 2 {
		t.Fatalf("%d pushes from 2 renewals, want 2: the lease is not being refreshed off the policy cycle", got)
	}
	// Renewal extends what is there; it must not create a second limit.
	if len(f.installed) != 1 {
		t.Errorf("%d limits installed after renewals, want 1", len(f.installed))
	}
	last := f.pushes[len(f.pushes)-1]
	if last.UUID != "uuid-1" {
		t.Errorf("renewal pushed UUID %q, want uuid-1", last.UUID)
	}
	if want := int(defaultLimitLease.Seconds()); last.Expiration != want {
		t.Errorf("renewal Expiration = %d, want %d", last.Expiration, want)
	}
}

// TestRenewAllSurvivesAFailedRenewal: a renewal that cannot reach the schedd is
// logged and dropped. The limit stays in activeLimits, because the next renewal
// is due well before the lease runs out and re-pushing the same UUID is what
// puts it back.
func TestRenewAllSurvivesAFailedRenewal(t *testing.T) {
	f := newFakeSchedd()
	m := newTestLimitManager(t, f)

	if err := m.reconcile(context.Background(), []ratelimit.Rule{testRule(5)}); err != nil {
		t.Fatalf("reconcile: %v", err)
	}

	f.pushErr = fmt.Errorf("schedd unreachable")
	m.renewAll(context.Background()) // must not panic or wipe state
	f.pushErr = nil
	m.renewAll(context.Background())

	if len(m.activeLimits) != 1 {
		t.Fatalf("%d tracked limits after a failed renewal, want 1", len(m.activeLimits))
	}
	if len(f.installed) != 1 {
		t.Errorf("%d limits in the schedd after recovery, want 1", len(f.installed))
	}
}

// TestRenewAllIsQuietWhenDisabled: against a schedd too old to support startup
// limits the manager disables itself, and the renewal timer keeps ticking
// regardless.
func TestRenewAllIsQuietWhenDisabled(t *testing.T) {
	f := newFakeSchedd()
	m := newTestLimitManager(t, f)
	m.cfg.enabled = false

	m.renewAll(context.Background())

	if len(f.pushes) != 0 {
		t.Errorf("%d pushes from a disabled manager, want 0", len(f.pushes))
	}
}

// TestDefaultsDoNotLapse is the regression test for the bug the dedicated
// renewal timer fixes.
//
// Renewal used to ride the advertise cycle, and the default advertise interval
// is exactly the default lease -- one minute. So at stock settings every limit
// reached its expiry at the same moment its renewal was due, lapsed, and was
// reinstalled on the next cycle, with a gap in between where nothing was
// throttled. Nothing errored; nothing logged; the daemon looked healthy.
//
// The renewal interval must be strictly, comfortably shorter than the lease,
// and it must not depend on either of the other intervals.
func TestDefaultsDoNotLapse(t *testing.T) {
	// The real defaults, loaded rather than restated, so this cannot pass
	// against numbers that have since moved.
	t.Setenv("CONDOR_CONFIG", "ONLY_ENV")
	condorCfg, err := condorconfig.New()
	if err != nil {
		t.Fatalf("condor config: %v", err)
	}
	cfg, err := config.LoadFrom(condorCfg)
	if err != nil {
		t.Fatalf("LoadFrom: %v", err)
	}

	s := &Service{
		pollInterval:       cfg.PollInterval,
		advertiseInterval:  cfg.AdvertiseInterval,
		limitLeaseDuration: cfg.LimitLease,
	}

	renew := s.renewalInterval()
	if renew >= cfg.LimitLease {
		t.Fatalf("renewal every %v against a %v lease: limits lapse between renewals", renew, cfg.LimitLease)
	}
	if renew >= s.advertiseInterval {
		t.Errorf("renewal interval %v is not shorter than the advertise interval %v; it is still coupled to the policy cycle",
			renew, s.advertiseInterval)
	}

	// Changing the intervals that have nothing to do with the lease must not
	// change how often the lease is renewed.
	s.advertiseInterval = 17 * time.Minute
	s.pollInterval = 11 * time.Minute
	if got := s.renewalInterval(); got != renew {
		t.Errorf("renewal interval moved to %v when the poll/advertise intervals changed; want %v", got, renew)
	}
}

// TestServiceAndManagerAgreeOnTheRenewalInterval: the ticker is created from
// the service's view of the lease before the limit manager exists, and the
// manager is what actually honors it. If the two ever disagree, the ticker
// fires at the wrong rate for the lease being installed.
func TestServiceAndManagerAgreeOnTheRenewalInterval(t *testing.T) {
	for _, lease := range []time.Duration{0, time.Second, 15 * time.Second, defaultLimitLease, 5 * time.Minute} {
		s := &Service{limitLeaseDuration: lease}
		m := newTestLimitManager(t, newFakeSchedd())
		m.cfg.lease = lease

		if got, want := s.renewalInterval(), m.renewalInterval(); got != want {
			t.Errorf("lease %v: service says %v, limit manager says %v", lease, got, want)
		}
	}
}
