package dbaddr

import (
	"os"
	"path/filepath"
	"strings"
	"testing"

	condorconfig "github.com/bbockelm/golang-htcondor/config"
)

func testCfg(t *testing.T) *condorconfig.Config {
	t.Helper()
	t.Setenv("CONDOR_CONFIG", "ONLY_ENV")
	cfg, err := condorconfig.New()
	if err != nil {
		t.Fatalf("condor config: %v", err)
	}
	return cfg
}

// TestLiteralAddressesPassThrough: an explicit address is what a site pointing
// at a remote htcondordb writes, and it must not be touched.
func TestLiteralAddressesPassThrough(t *testing.T) {
	cfg := testCfg(t)
	for _, in := range []string{
		"db.example.org:9618",
		"<127.0.0.1:9618?addrs=127.0.0.1-9618&noUDP&sock=htcondordb_42_ab>",
		"192.0.2.7:1234",
	} {
		got, err := Resolve(in, cfg)
		if err != nil {
			t.Errorf("Resolve(%q): %v", in, err)
			continue
		}
		if got != in {
			t.Errorf("Resolve(%q) = %q, want it unchanged", in, got)
		}
	}
}

// TestEmptyMeansUnconfigured: the address doubles as the on/off switch for the
// database-backed paths, so empty has to stay empty rather than becoming an
// attempt to find something.
func TestEmptyMeansUnconfigured(t *testing.T) {
	got, err := Resolve("   ", testCfg(t))
	if err != nil {
		t.Fatalf("Resolve: %v", err)
	}
	if got != "" {
		t.Errorf("Resolve(blank) = %q, want empty", got)
	}
	if IsConfigured("") || IsConfigured("  ") {
		t.Error("blank counted as configured")
	}
	if !IsConfigured(Auto) || !IsConfigured("h:1") {
		t.Error("a real value counted as unconfigured")
	}
}

// TestAddressFilePathIsRead is the form that makes a static configuration work
// at all: the file's contents change when htcondordb restarts, the configured
// path does not.
func TestAddressFilePathIsRead(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, ".htcondordb_address")
	want := "<127.0.0.1:9618?addrs=127.0.0.1-9618&noUDP&sock=htcondordb_1234_ab>"
	if err := os.WriteFile(path, []byte(want+"\n"), 0o644); err != nil {
		t.Fatalf("write: %v", err)
	}

	got, err := Resolve(path, testCfg(t))
	if err != nil {
		t.Fatalf("Resolve: %v", err)
	}
	if got != want {
		t.Errorf("Resolve(%q) = %q, want %q", path, got, want)
	}
}

// TestResolvingIsRepeatedNotCached: the whole reason to resolve per dial is that
// a restarted htcondordb publishes a new socket name. Reading the same
// configured path again must see the new address.
func TestResolvingIsRepeatedNotCached(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, ".htcondordb_address")
	cfg := testCfg(t)

	first := "<127.0.0.1:9618?sock=htcondordb_100_aa>"
	if err := os.WriteFile(path, []byte(first), 0o644); err != nil {
		t.Fatalf("write: %v", err)
	}
	if got, _ := Resolve(path, cfg); got != first {
		t.Fatalf("first resolve = %q, want %q", got, first)
	}

	// htcondordb restarts under a new pid, so a new socket name.
	second := "<127.0.0.1:9618?sock=htcondordb_222_bb>"
	if err := os.WriteFile(path, []byte(second), 0o644); err != nil {
		t.Fatalf("rewrite: %v", err)
	}
	got, err := Resolve(path, cfg)
	if err != nil {
		t.Fatalf("Resolve: %v", err)
	}
	if got != second {
		t.Errorf("second resolve = %q, want %q -- a stale address survives a restart", got, second)
	}
}

// TestMissingAddressFileErrors: an htcondordb that is not running yet must
// produce an error the caller can fall back on, not an empty address that would
// be dialed and fail obscurely.
func TestMissingAddressFileErrors(t *testing.T) {
	got, err := Resolve(filepath.Join(t.TempDir(), "absent"), testCfg(t))
	if err == nil {
		t.Fatalf("Resolve of a missing address file returned %q and no error", got)
	}
	if !strings.Contains(err.Error(), "absent") {
		t.Errorf("error does not name the path it tried: %v", err)
	}
}

// TestAutoUsesTheHtcondordbKnobs: "auto" is the AP case -- both daemons on one
// host, no address to write down. It must resolve exactly the way htcondordb's
// own clients do, which includes honoring the environment knobs they honor.
func TestAutoUsesTheHtcondordbKnobs(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, ".htcondordb_address")
	want := "<127.0.0.1:9618?sock=htcondordb_7_cd>"
	if err := os.WriteFile(path, []byte(want), 0o644); err != nil {
		t.Fatalf("write: %v", err)
	}
	t.Setenv("HTCONDORDB_ADDRESS_FILE", path)

	got, err := Resolve(Auto, testCfg(t))
	if err != nil {
		t.Fatalf("Resolve(auto): %v", err)
	}
	if got != want {
		t.Errorf("Resolve(auto) = %q, want %q", got, want)
	}

	// Spelled in whatever case the admin used.
	if got, err := Resolve("AUTO", testCfg(t)); err != nil || got != want {
		t.Errorf(`Resolve("AUTO") = %q, %v`, got, err)
	}
}

// TestAutoWithNothingToFindErrors: better a named failure the caller can fall
// back from than a blank address dialed into nowhere.
func TestAutoWithNothingToFindErrors(t *testing.T) {
	t.Setenv("HTCONDORDB_ADDRESS_FILE", filepath.Join(t.TempDir(), "absent"))
	t.Setenv("HTCONDORDB_HOST", "")
	if got, err := Resolve(Auto, testCfg(t)); err == nil {
		t.Errorf("Resolve(auto) with nothing to find returned %q and no error", got)
	}
}
