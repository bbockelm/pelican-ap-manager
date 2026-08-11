package store

import (
	"context"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/bbockelm/pelican-ap-manager/internal/ratelimit"
)

func newFileStore(t *testing.T) (*FileStore, string) {
	t.Helper()
	path := filepath.Join(t.TempDir(), "rules.json")
	s, err := OpenFileStore(path)
	if err != nil {
		t.Fatalf("OpenFileStore: %v", err)
	}
	return s, path
}

func TestFileStoreRoundTrip(t *testing.T) {
	ctx := context.Background()
	s, path := newFileStore(t)

	rules, err := s.ListRules(ctx)
	if err != nil {
		t.Fatalf("ListRules on a fresh store: %v", err)
	}
	if len(rules) != 0 {
		t.Fatalf("fresh store has %d rules, want 0", len(rules))
	}

	want := ratelimit.Rule{
		Name: "ligo_ucsd", Origin: ratelimit.OriginStatic,
		User: "ligo", Site: "UCSD", Sources: []string{"osdf://ospool"},
		RateCount: 20, RateWindow: time.Minute,
		ConfigManaged: true, Note: "burst guard",
		UpdatedAt: time.Unix(1_700_000_000, 0),
	}
	if err := s.PutRule(ctx, want); err != nil {
		t.Fatalf("PutRule: %v", err)
	}

	// Reopen from disk: the point of the store is that it survives the process.
	reopened, err := OpenFileStore(path)
	if err != nil {
		t.Fatalf("reopening: %v", err)
	}
	got, err := reopened.ListRules(ctx)
	if err != nil {
		t.Fatalf("ListRules after reopen: %v", err)
	}
	if len(got) != 1 {
		t.Fatalf("after reopen: %d rules, want 1", len(got))
	}
	if got[0].Name != want.Name || got[0].RateCount != want.RateCount ||
		got[0].RateWindow != want.RateWindow || got[0].Site != want.Site ||
		!got[0].ConfigManaged || got[0].Note != want.Note {
		t.Errorf("after reopen: %+v, want %+v", got[0], want)
	}
	if len(got[0].Sources) != 1 || got[0].Sources[0] != "osdf://ospool" {
		t.Errorf("sources = %v, want [osdf://ospool]", got[0].Sources)
	}
}

func TestFileStorePutReplacesByName(t *testing.T) {
	ctx := context.Background()
	s, _ := newFileStore(t)

	base := ratelimit.Rule{Name: "r", Origin: ratelimit.OriginStatic, Site: "UCSD", RateCount: 5}
	if err := s.PutRule(ctx, base); err != nil {
		t.Fatalf("PutRule: %v", err)
	}
	base.RateCount = 9
	if err := s.PutRule(ctx, base); err != nil {
		t.Fatalf("PutRule (replace): %v", err)
	}

	got, err := s.ListRules(ctx)
	if err != nil {
		t.Fatalf("ListRules: %v", err)
	}
	if len(got) != 1 {
		t.Fatalf("%d rules after replacing one, want 1", len(got))
	}
	if got[0].RateCount != 9 {
		t.Errorf("RateCount = %d, want 9", got[0].RateCount)
	}
}

func TestFileStoreDelete(t *testing.T) {
	ctx := context.Background()
	s, _ := newFileStore(t)

	if err := s.PutRule(ctx, ratelimit.Rule{Name: "r", Origin: ratelimit.OriginStatic, Site: "UCSD", RateCount: 5}); err != nil {
		t.Fatalf("PutRule: %v", err)
	}
	// Deleting a rule that was never there is not an error: reconciliation
	// deletes optimistically.
	if err := s.DeleteRule(ctx, "absent"); err != nil {
		t.Errorf("DeleteRule on a missing rule: %v", err)
	}
	if err := s.DeleteRule(ctx, "r"); err != nil {
		t.Fatalf("DeleteRule: %v", err)
	}
	got, err := s.ListRules(ctx)
	if err != nil {
		t.Fatalf("ListRules: %v", err)
	}
	if len(got) != 0 {
		t.Errorf("%d rules after delete, want 0", len(got))
	}
}

func TestFileStoreRejectsInvalidRule(t *testing.T) {
	s, _ := newFileStore(t)
	// No selector at all: would silently match every job in the queue.
	if err := s.PutRule(context.Background(), ratelimit.Rule{Name: "r", Origin: ratelimit.OriginStatic, RateCount: 5}); err == nil {
		t.Error("PutRule accepted a rule with no selector")
	}
}

func TestFileStoreRefusesNewerVersion(t *testing.T) {
	path := filepath.Join(t.TempDir(), "rules.json")
	if err := os.WriteFile(path, []byte(`{"version":99,"rules":[]}`), 0o644); err != nil {
		t.Fatalf("seeding: %v", err)
	}
	// Opening it read-write would truncate a document we cannot understand.
	if _, err := OpenFileStore(path); err == nil {
		t.Error("OpenFileStore accepted a document from a newer version")
	}
}

func TestSyncConfigRules(t *testing.T) {
	ctx := context.Background()
	s, _ := newFileStore(t)

	// A rule created out of band (not from the configuration), and a dynamic
	// rule the control loop persisted. Neither is the configuration's to touch.
	handWritten := ratelimit.Rule{Name: "hand", Origin: ratelimit.OriginStatic, Site: "MIT", RateCount: 3}
	derived := ratelimit.Rule{Name: "derived", Origin: ratelimit.OriginDynamic, User: "bob", Site: "PSU", RateCount: 7}
	for _, r := range []ratelimit.Rule{handWritten, derived} {
		if err := s.PutRule(ctx, r); err != nil {
			t.Fatalf("seeding %s: %v", r.Name, err)
		}
	}

	declared := []ratelimit.Rule{{Name: "cfg_a", Site: "UCSD", RateCount: 5}, {Name: "cfg_b", User: "alice", RateCount: 2}}
	if err := SyncConfigRules(ctx, s, declared); err != nil {
		t.Fatalf("SyncConfigRules: %v", err)
	}

	got := byName(t, s)
	for _, name := range []string{"hand", "derived", "cfg_a", "cfg_b"} {
		if _, ok := got[name]; !ok {
			t.Errorf("rule %q missing after sync", name)
		}
	}
	if !got["cfg_a"].ConfigManaged || got["cfg_a"].Origin != ratelimit.OriginStatic {
		t.Errorf("cfg_a = %+v, want config-managed and static", got["cfg_a"])
	}

	// Drop cfg_b from the configuration: it must be retired, and nothing else.
	if err := SyncConfigRules(ctx, s, declared[:1]); err != nil {
		t.Fatalf("SyncConfigRules (retiring cfg_b): %v", err)
	}
	got = byName(t, s)
	if _, ok := got["cfg_b"]; ok {
		t.Error("cfg_b survived removal from the configuration")
	}
	for _, name := range []string{"hand", "derived", "cfg_a"} {
		if _, ok := got[name]; !ok {
			t.Errorf("rule %q was retired but should not have been", name)
		}
	}
}

func byName(t *testing.T, s RuleStore) map[string]ratelimit.Rule {
	t.Helper()
	rules, err := s.ListRules(context.Background())
	if err != nil {
		t.Fatalf("ListRules: %v", err)
	}
	out := make(map[string]ratelimit.Rule, len(rules))
	for _, r := range rules {
		out[r.Name] = r
	}
	return out
}
