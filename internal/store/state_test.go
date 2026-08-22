package store

import (
	"context"
	"path/filepath"
	"testing"

	"github.com/bbockelm/pelican-ap-manager/internal/state"
	"github.com/google/go-cmp/cmp"
)

// TestFileStateStoreRoundTrip: the file backend is the default, and the one
// every existing deployment is using.
func TestFileStateStoreRoundTrip(t *testing.T) {
	path := filepath.Join(t.TempDir(), "state.json")
	s, err := OpenFileStateStore(path)
	if err != nil {
		t.Fatalf("OpenFileStateStore: %v", err)
	}
	defer func() { _ = s.Close() }()

	st := state.New()
	st.RestoreSections(populatedSections())
	if err := s.Save(context.Background(), st); err != nil {
		t.Fatalf("Save: %v", err)
	}

	loaded, err := s.Load(context.Background())
	if err != nil {
		t.Fatalf("Load: %v", err)
	}
	if diff := cmp.Diff(st.Sections(), loaded.Sections()); diff != "" {
		t.Errorf("state did not survive a save/load:\n%s", diff)
	}
}

// TestFileStateStoreStartsEmpty: a store that has never been written is not an
// error. A first start would otherwise fail, and buildService treats a load
// error as fatal precisely so a real failure is not mistaken for a fresh start.
func TestFileStateStoreStartsEmpty(t *testing.T) {
	s, err := OpenFileStateStore(filepath.Join(t.TempDir(), "absent.json"))
	if err != nil {
		t.Fatalf("OpenFileStateStore: %v", err)
	}
	st, err := s.Load(context.Background())
	if err != nil {
		t.Fatalf("Load of a nonexistent state: %v", err)
	}
	if st == nil {
		t.Fatal("Load returned no state")
	}
	if got := st.LastJobEpochID(); got != (state.EpochID{}) {
		t.Errorf("a fresh state has a cursor of %v, want zero", got)
	}
}

// TestOpenStateSelectsTheBackend: the address is what switches backends, and
// getting this wrong would silently keep writing to a local file on a site that
// asked for the database.
func TestOpenStateSelectsTheBackend(t *testing.T) {
	cfg := testCondorConfig(t)
	path := filepath.Join(t.TempDir(), "state.json")

	s, desc, err := OpenState(StateOptions{FilePath: path, Config: cfg})
	if err != nil {
		t.Fatalf("OpenState (file): %v", err)
	}
	if _, ok := s.(*FileStateStore); !ok {
		t.Errorf("no DB address gave a %T, want the file backend", s)
	}
	if desc != "file "+path {
		t.Errorf("description = %q", desc)
	}

	s, desc, err = OpenState(StateOptions{DBAddress: "db.example.org:9618", FilePath: path, Config: cfg})
	if err != nil {
		t.Fatalf("OpenState (db): %v", err)
	}
	if _, ok := s.(*DBStateStore); !ok {
		t.Errorf("a DB address gave a %T, want the htcondordb backend", s)
	}
	// The default table has to appear in the log line, or an operator cannot
	// tell which table to look in.
	if want := "htcondordb db.example.org:9618 table pelican_manager_state"; desc != want {
		t.Errorf("description = %q, want %q", desc, want)
	}

	if _, _, err := OpenState(StateOptions{Config: cfg}); err == nil {
		t.Error("OpenState accepted neither an address nor a path")
	}
}
