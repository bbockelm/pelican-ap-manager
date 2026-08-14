package ratelimit

import (
	"testing"
	"time"
)

func TestParseMode(t *testing.T) {
	for _, raw := range []string{"enforcing", "ENFORCE", " active "} {
		got, err := ParseMode(raw)
		if err != nil || got != ModeEnforcing {
			t.Errorf("ParseMode(%q) = %q, %v; want %q", raw, got, err, ModeEnforcing)
		}
	}
	for _, raw := range []string{"observing", "Observe", "monitor", "dry-run"} {
		got, err := ParseMode(raw)
		if err != nil || got != ModeObserving {
			t.Errorf("ParseMode(%q) = %q, %v; want %q", raw, got, err, ModeObserving)
		}
	}
	if _, err := ParseMode("maybe"); err == nil {
		t.Error("ParseMode(\"maybe\") should fail")
	}
}

// TestObservingKeepsStaticRules is the behavior the feature exists for: an
// operator running in observing mode still gets the rules they wrote by hand,
// and none of the controller's.
func TestObservingKeepsStaticRules(t *testing.T) {
	now := time.Unix(1_700_000_000, 0)
	rules := []Rule{
		{Name: "operator", Origin: OriginStatic, Site: "UCSD", RateCount: 5},
		{Name: "controller", Origin: OriginDynamic, Site: "UCSD", RateCount: 50},
		{Name: "parked", Origin: OriginStatic, Site: "PSU", RateCount: 1, Disabled: true},
		{Name: "lapsed", Origin: OriginStatic, Site: "MIT", RateCount: 1, ExpiresAt: now.Add(-time.Minute)},
	}

	got := ModeObserving.Installable(rules, now)
	if len(got) != 1 || got[0].Name != "operator" {
		t.Fatalf("observing installable = %v, want just [operator]", names(got))
	}

	got = ModeEnforcing.Installable(rules, now)
	if len(got) != 2 || got[0].Name != "controller" || got[1].Name != "operator" {
		t.Fatalf("enforcing installable = %v, want [controller operator] (sorted by name)", names(got))
	}
}

func TestEnforces(t *testing.T) {
	if !ModeObserving.Enforces(OriginStatic) {
		t.Error("observing mode must still enforce static rules")
	}
	if ModeObserving.Enforces(OriginDynamic) {
		t.Error("observing mode must not enforce dynamic rules")
	}
	if !ModeEnforcing.Enforces(OriginDynamic) || !ModeEnforcing.Enforces(OriginStatic) {
		t.Error("enforcing mode must enforce both origins")
	}
}

func names(rules []Rule) []string {
	out := make([]string, len(rules))
	for i, r := range rules {
		out[i] = r.Name
	}
	return out
}
