package condor

import (
	"strings"
	"testing"
)

// TestSameHostMatching covers the comparison that decides whether a schedd is
// this access point's. A schedd advertises Machine and a Name of
// "<SCHEDD_NAME>@<FULL_HOSTNAME>", and the two ends of the comparison do not
// always agree on whether the name is qualified.
func TestSameHostMatching(t *testing.T) {
	same := []struct{ a, b string }{
		{"ap2101.example.org", "ap2101.example.org"},
		{"AP2101.example.org", "ap2101.example.org"}, // advertised case varies
		{"ap2101", "ap2101.example.org"},             // short local, qualified ad
		{"ap2101.example.org", "ap2101"},             // and the reverse
	}
	for _, c := range same {
		if !sameHost(c.a, c.b) {
			t.Errorf("sameHost(%q, %q) = false, want true", c.a, c.b)
		}
	}

	// The case that matters: two different hosts must never match, however much
	// of the domain they share. This is what sent the daemon at a schedd on
	// another site.
	different := []struct{ a, b string }{
		{"path-ap2101", "scatter2.dev.nanohub.hub.rcac.purdue.edu"},
		{"ap2101.example.org", "ap2102.example.org"},
		{"ap.example.org", "ap.other.org"}, // same first label, different domain
		{"", "ap2101"},                     // unknown local host matches nothing
		{"ap2101", ""},
	}
	for _, c := range different {
		if sameHost(c.a, c.b) {
			t.Errorf("sameHost(%q, %q) = true, want false", c.a, c.b)
		}
	}
}

// TestSameHostRejectsDifferentDomains is called out separately because the
// first-label fallback is the risky half: it exists so a short FULL_HOSTNAME
// matches a qualified ad, and it must not thereby match a same-named host in
// another domain.
func TestSameHostRejectsDifferentDomains(t *testing.T) {
	if sameHost("submit.a.example", "submit.b.example") {
		t.Error("two qualified names sharing only their first label matched")
	}
	if !sameHost("submit", "submit.a.example") {
		t.Error("a short name did not match its own qualified form")
	}
}

func TestHostPart(t *testing.T) {
	for in, want := range map[string]string{
		"hub2osg@scatter2.dev.nanohub.hub.rcac.purdue.edu": "scatter2.dev.nanohub.hub.rcac.purdue.edu",
		"submit-1@ap2101.example.org":                      "ap2101.example.org",
		"no-at-sign":                                       "",
		"":                                                 "",
		// A name with several @ takes the last, matching how HTCondor builds it.
		"a@b@host.example": "host.example",
	} {
		if got := hostPart(in); got != want {
			t.Errorf("hostPart(%q) = %q, want %q", in, got, want)
		}
	}
}

// TestLocalHostnameIsFoundSomehow: the local host is what identifies this AP's
// schedd when no name is configured, so failing to determine it at all would
// silently reinstate "pick whatever the collector returns first".
func TestLocalHostnameIsFoundSomehow(t *testing.T) {
	if h := LocalHostname(); strings.TrimSpace(h) == "" {
		t.Error("LocalHostname returned nothing; an unnamed schedd could not be matched to this host")
	}
}

// TestNewClientRecordsTheLocalHost: NewClient has to fill localHost in, because
// an empty one falls back to accepting any single schedd -- fine on a personal
// pool, wrong on a shared collector.
func TestNewClientRecordsTheLocalHost(t *testing.T) {
	c, err := NewClient("collector.example.org:9618", "", "GLIDEIN_Site")
	if err != nil {
		t.Fatalf("NewClient: %v", err)
	}
	if got := c.(*htcClient).localHost; got == "" {
		t.Error("NewClient left localHost empty")
	}

	c, err = NewClientForHost("collector.example.org:9618", "", "", "  AP2101.Example.ORG  ")
	if err != nil {
		t.Fatalf("NewClientForHost: %v", err)
	}
	// Normalized on the way in, so the comparison does not have to care.
	if got := c.(*htcClient).localHost; got != "ap2101.example.org" {
		t.Errorf("localHost = %q, want it trimmed and lower-cased", got)
	}
}

// TestPickLocalScheddIgnoresOtherHosts is the regression test for the bug this
// fixes. With no schedd name configured, the daemon asked the collector for an
// unnamed schedd and got an arbitrary one -- on a shared pool collector, another
// site's. It then reported a confusing authentication failure against a host it
// had no business touching.
//
// The candidate list below is the shape of the pool it actually ran in.
func TestPickLocalScheddIgnoresOtherHosts(t *testing.T) {
	pool := []scheddCandidate{
		{"hub2osg@scatter2.dev.nanohub.hub.rcac.purdue.edu", "scatter2.dev.nanohub.hub.rcac.purdue.edu", "<128.211.145.78:9618?sock=schedd_9485_d04a>"},
		{"other@ap9999.example.org", "ap9999.example.org", "<10.0.0.9:9618?sock=schedd_1_a>"},
		{"path-ap2101@path-ap2101.example.org", "path-ap2101.example.org", "<10.0.0.1:9618?sock=schedd_2_b>"},
	}

	got, err := pickLocalSchedd(pool, "path-ap2101.example.org")
	if err != nil {
		t.Fatalf("pickLocalSchedd: %v", err)
	}
	if got.machine != "path-ap2101.example.org" {
		t.Errorf("picked the schedd on %s; want this host's", got.machine)
	}

	// Ordering must not decide it: the remote schedd is listed first above, and
	// "whichever came back first" is exactly the old behavior.
	if got.name == pool[0].name {
		t.Error("picked the first schedd the collector listed rather than the local one")
	}
}

// TestPickLocalScheddRefusesToGuess: with several schedds and none on this host,
// there is no defensible choice. Picking one anyway is how the daemon ends up
// managing an access point that is not its own.
func TestPickLocalScheddRefusesToGuess(t *testing.T) {
	pool := []scheddCandidate{
		{"a@ap1.example.org", "ap1.example.org", "<10.0.0.1:9618>"},
		{"b@ap2.example.org", "ap2.example.org", "<10.0.0.2:9618>"},
	}
	_, err := pickLocalSchedd(pool, "ap3.example.org")
	if err == nil {
		t.Fatal("picked a schedd on another host with no name configured")
	}
	// The message has to name both what it looked for and what it found, or the
	// admin cannot tell which of the two is wrong.
	for _, want := range []string{"ap3.example.org", "a@ap1.example.org", "b@ap2.example.org", "PELICAN_MANAGER_SCHEDD_NAME"} {
		if !strings.Contains(err.Error(), want) {
			t.Errorf("error does not mention %q: %v", want, err)
		}
	}
}

// TestPickLocalScheddAcceptsASoloSchedd: a personal pool or a test rig may
// advertise a hostname this daemon does not recognize as itself. One schedd is
// unambiguous, so take it.
func TestPickLocalScheddAcceptsASoloSchedd(t *testing.T) {
	pool := []scheddCandidate{{"solo@somehost", "somehost", "<10.0.0.1:9618>"}}
	got, err := pickLocalSchedd(pool, "a-name-that-matches-nothing")
	if err != nil {
		t.Fatalf("pickLocalSchedd with one schedd: %v", err)
	}
	if got.name != "solo@somehost" {
		t.Errorf("picked %q", got.name)
	}

	// But a solo schedd on the local host is still matched by host, not by luck.
	got, err = pickLocalSchedd(pool, "somehost")
	if err != nil || got.name != "solo@somehost" {
		t.Errorf("local solo schedd: %q, %v", got.name, err)
	}
}

// TestPickLocalScheddNeedsAnAddress: an ad with no MyAddress cannot be dialed,
// and saying so beats returning a client that fails obscurely later.
func TestPickLocalScheddNeedsAnAddress(t *testing.T) {
	if _, err := pickLocalSchedd([]scheddCandidate{{"a@here", "here", ""}}, "here"); err == nil {
		t.Error("accepted a local schedd with no MyAddress")
	}
	if _, err := pickLocalSchedd([]scheddCandidate{{"a@elsewhere", "elsewhere", ""}}, ""); err == nil {
		t.Error("accepted a solo schedd with no MyAddress")
	}
	if _, err := pickLocalSchedd(nil, "here"); err == nil {
		t.Error("accepted an empty pool")
	}
}

// TestPickLocalScheddMatchesOnEitherAttribute: Machine is the usual one, but a
// schedd whose Machine is missing or oddly set is still identifiable from the
// host half of its Name.
func TestPickLocalScheddMatchesOnEitherAttribute(t *testing.T) {
	byName := []scheddCandidate{
		{"other@ap9.example.org", "ap9.example.org", "<10.0.0.9:9618>"},
		{"mine@ap1.example.org", "", "<10.0.0.1:9618>"}, // no Machine
	}
	got, err := pickLocalSchedd(byName, "ap1.example.org")
	if err != nil || got.name != "mine@ap1.example.org" {
		t.Errorf("did not match on the Name host part: %q, %v", got.name, err)
	}
}
