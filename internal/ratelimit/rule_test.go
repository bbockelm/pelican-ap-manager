package ratelimit

import (
	"testing"
	"time"
)

func TestParseRule(t *testing.T) {
	tests := []struct {
		name    string
		spec    string
		want    Rule
		wantErr bool
	}{
		{
			name: "user and site",
			spec: `user=alice site=UCSD rate=20 window=60s`,
			want: Rule{Name: "r", Origin: OriginStatic, User: "alice", Site: "UCSD", RateCount: 20, RateWindow: time.Minute},
		},
		{
			name: "site only with quoted note",
			spec: `site=PSU-LIGO rate=5 window=2m note="incident 4471"`,
			want: Rule{Name: "r", Origin: OriginStatic, Site: "PSU-LIGO", RateCount: 5, RateWindow: 2 * time.Minute, Note: "incident 4471"},
		},
		{
			name: "source list",
			spec: `user=bob sources=osdf://ospool,osdf://other rate=1`,
			want: Rule{Name: "r", Origin: OriginStatic, User: "bob", Sources: []string{"osdf://ospool", "osdf://other"}, RateCount: 1},
		},
		{
			name: "monitor-only rate of zero is allowed",
			spec: `site=UCSD rate=0`,
			want: Rule{Name: "r", Origin: OriginStatic, Site: "UCSD", RateCount: 0},
		},
		{
			name: "explicit wildcard matches everything",
			spec: `user=* rate=100`,
			want: Rule{Name: "r", Origin: OriginStatic, User: "*", RateCount: 100},
		},
		{name: "unknown key is rejected", spec: `user=alice raet=20`, wantErr: true},
		{name: "non-numeric rate is rejected", spec: `user=alice rate=lots`, wantErr: true},
		{name: "bad window is rejected", spec: `user=alice rate=1 window=60`, wantErr: true},
		{name: "negative rate is rejected", spec: `user=alice rate=-1`, wantErr: true},
		{name: "empty selector is rejected", spec: `rate=5`, wantErr: true},
		{name: "unterminated quote is rejected", spec: `user=alice rate=1 note="oops`, wantErr: true},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			got, err := ParseRule("r", tc.spec)
			if tc.wantErr {
				if err == nil {
					t.Fatalf("ParseRule(%q) = %+v, want error", tc.spec, got)
				}
				return
			}
			if err != nil {
				t.Fatalf("ParseRule(%q): %v", tc.spec, err)
			}
			if got.Name != tc.want.Name || got.Origin != tc.want.Origin ||
				got.User != tc.want.User || got.Site != tc.want.Site ||
				got.RateCount != tc.want.RateCount || got.RateWindow != tc.want.RateWindow ||
				got.Note != tc.want.Note {
				t.Errorf("ParseRule(%q) = %+v, want %+v", tc.spec, got, tc.want)
			}
			if len(got.Sources) != len(tc.want.Sources) {
				t.Fatalf("sources = %v, want %v", got.Sources, tc.want.Sources)
			}
			for i := range got.Sources {
				if got.Sources[i] != tc.want.Sources[i] {
					t.Errorf("sources[%d] = %q, want %q", i, got.Sources[i], tc.want.Sources[i])
				}
			}
		})
	}
}

func TestRuleWindowDefaults(t *testing.T) {
	if got := (Rule{}).Window(); got != DefaultRateWindow {
		t.Errorf("Window() = %v, want %v", got, DefaultRateWindow)
	}
	if got := (Rule{RateWindow: 5 * time.Second}).Window(); got != 5*time.Second {
		t.Errorf("Window() = %v, want 5s", got)
	}
}

func TestRuleActive(t *testing.T) {
	now := time.Unix(1_700_000_000, 0)
	if (Rule{Disabled: true}).Active(now) {
		t.Error("a disabled rule must not be active")
	}
	if (Rule{ExpiresAt: now.Add(-time.Second)}).Active(now) {
		t.Error("an expired rule must not be active")
	}
	if !(Rule{ExpiresAt: now.Add(time.Second)}).Active(now) {
		t.Error("an unexpired rule must be active")
	}
	if !(Rule{}).Active(now) {
		t.Error("a rule with no expiry must be active")
	}
}

func TestRuleMatches(t *testing.T) {
	tests := []struct {
		name       string
		rule       Rule
		user, site string
		want       bool
	}{
		{"exact", Rule{User: "alice", Site: "UCSD"}, "alice", "UCSD", true},
		{"wrong user", Rule{User: "alice", Site: "UCSD"}, "bob", "UCSD", false},
		{"site wildcard", Rule{User: "alice", Site: "*"}, "alice", "anywhere", true},
		{"empty user is a wildcard", Rule{Site: "UCSD"}, "anyone", "UCSD", true},
		{"raw expression never matches by selector", Rule{Expression: "true", User: "alice"}, "alice", "UCSD", false},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			if got := tc.rule.Matches(tc.user, tc.site); got != tc.want {
				t.Errorf("Matches(%q, %q) = %v, want %v", tc.user, tc.site, got, tc.want)
			}
		})
	}
}

func TestClassAdExpression(t *testing.T) {
	const siteAttr = "GLIDEIN_Site"

	tests := []struct {
		name string
		rule Rule
		want string
	}{
		{
			name: "user and site",
			rule: Rule{User: "alice", Site: "UCSD"},
			want: `(JOB.Owner =?= "alice" && TARGET.GLIDEIN_Site =?= "UCSD")`,
		},
		{
			name: "single source",
			rule: Rule{User: "alice", Site: "UCSD", Sources: []string{"osdf://ospool"}},
			want: `(JOB.Owner =?= "alice" && stringListMember("osdf://ospool", JOB.PelicanInputPrefixes ?: "") && TARGET.GLIDEIN_Site =?= "UCSD")`,
		},
		{
			name: "several sources are a disjunction",
			rule: Rule{User: "alice", Sources: []string{"a", "b"}},
			want: `(JOB.Owner =?= "alice" && (stringListMember("a", JOB.PelicanInputPrefixes ?: "") || stringListMember("b", JOB.PelicanInputPrefixes ?: "")))`,
		},
		{
			name: "wildcards drop their clauses",
			rule: Rule{User: "*", Site: "UCSD"},
			want: `(TARGET.GLIDEIN_Site =?= "UCSD")`,
		},
		{
			name: "all wildcards matches everything",
			rule: Rule{User: "*", Site: "*"},
			want: "true",
		},
		{
			name: "raw expression wins",
			rule: Rule{User: "alice", Expression: "RequestGpus > 0"},
			want: "RequestGpus > 0",
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			if got := tc.rule.ClassAdExpression(siteAttr); got != tc.want {
				t.Errorf("ClassAdExpression() =\n  %s\nwant\n  %s", got, tc.want)
			}
		})
	}
}

func TestLimitName(t *testing.T) {
	r := Rule{Name: "ligo/ucsd-1", Origin: OriginStatic}
	if got, want := r.LimitName(), "pelican_static_ligo_ucsd_1"; got != want {
		t.Errorf("LimitName() = %q, want %q", got, want)
	}
	d := Rule{Name: "ligo/ucsd-1", Origin: OriginDynamic}
	if d.LimitName() == r.LimitName() {
		t.Error("static and dynamic rules with the same name must not collide in the schedd")
	}
}
