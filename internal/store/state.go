package store

import (
	"context"
	"fmt"

	"github.com/bbockelm/golang-htcondor/config"
	"github.com/bbockelm/pelican-ap-manager/internal/state"
)

// StateStore persists the daemon's own working state: the epoch cursors, the
// per-bucket transfer summaries, the control loop's per-pair conclusions, and
// the rolling scratch it derives them from.
//
// This is not policy -- unlike the rules, nobody writes it by hand -- but it is
// what makes a restart continue rather than start over. Losing it costs a
// lookback window of re-read history and a control loop that has forgotten
// every pair it had classified.
//
// Implementations need not be safe for concurrent use: the poll loop is the
// only writer, and it saves synchronously.
type StateStore interface {
	// Load returns the persisted state, or a fresh one when nothing is stored
	// yet. A store that has never been written is not an error.
	Load(ctx context.Context) (*state.State, error)

	// Save persists the state. Implementations may write only what changed.
	Save(ctx context.Context, st *state.State) error

	Close() error
}

// FileStateStore keeps the state in the JSON document under SPOOL, which is
// where all of it lived before there was an interface here.
type FileStateStore struct{ path string }

// OpenFileStateStore prepares the file-backed state store.
func OpenFileStateStore(path string) (*FileStateStore, error) {
	if path == "" {
		return nil, fmt.Errorf("store: state file path is required")
	}
	return &FileStateStore{path: path}, nil
}

// Load implements StateStore.
func (s *FileStateStore) Load(context.Context) (*state.State, error) {
	return state.Load(s.path)
}

// Save implements StateStore.
func (s *FileStateStore) Save(_ context.Context, st *state.State) error {
	return st.Save(s.path)
}

// Close implements StateStore.
func (s *FileStateStore) Close() error { return nil }

// StateOptions selects and configures a state store backend.
type StateOptions struct {
	// DBAddress, when non-empty, selects the htcondordb backend.
	DBAddress string
	// DBTable names the state table. Defaults to DefaultStateTable.
	DBTable string
	// FilePath is the JSON document used when DBAddress is empty.
	FilePath string
	// Config supplies the client security policy for the htcondordb backend.
	Config *config.Config
}

// OpenState returns the configured state store, along with a description of the
// backend suitable for a startup log line.
func OpenState(opts StateOptions) (StateStore, string, error) {
	if opts.DBAddress != "" {
		s, err := OpenDBStateStore(StateDBConfig{
			Address: opts.DBAddress,
			Table:   opts.DBTable,
			Config:  opts.Config,
		})
		if err != nil {
			return nil, "", err
		}
		return s, fmt.Sprintf("htcondordb %s table %s", opts.DBAddress, s.table), nil
	}

	if opts.FilePath == "" {
		return nil, "", fmt.Errorf("store: neither an htcondordb address nor a state file path is configured")
	}
	s, err := OpenFileStateStore(opts.FilePath)
	if err != nil {
		return nil, "", err
	}
	return s, "file " + opts.FilePath, nil
}
