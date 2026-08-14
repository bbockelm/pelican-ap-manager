package store

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"sync"

	"github.com/bbockelm/pelican-ap-manager/internal/ratelimit"
)

// FileStore keeps the rule set in a JSON document on local disk. It is the
// default backend: a rate rule is small, rarely written, and read once per poll
// cycle, so a whole-file rewrite per write costs nothing and buys crash safety
// for free (write to a sibling temp file, then rename).
type FileStore struct {
	path string

	mu    sync.RWMutex
	rules map[string]ratelimit.Rule
}

// ruleFile is the on-disk document. The version field exists so a later schema
// change can be detected rather than silently misread.
type ruleFile struct {
	Version int              `json:"version"`
	Rules   []ratelimit.Rule `json:"rules"`
}

const ruleFileVersion = 1

// OpenFileStore loads the rule set at path, creating an empty store when the
// file does not exist yet.
func OpenFileStore(path string) (*FileStore, error) {
	if path == "" {
		return nil, fmt.Errorf("store: rule file path is required")
	}
	s := &FileStore{path: path, rules: make(map[string]ratelimit.Rule)}

	data, err := os.ReadFile(path)
	if err != nil {
		if os.IsNotExist(err) {
			return s, nil
		}
		return nil, fmt.Errorf("store: reading %s: %w", path, err)
	}
	if len(data) == 0 {
		return s, nil
	}

	var doc ruleFile
	if err := json.Unmarshal(data, &doc); err != nil {
		return nil, fmt.Errorf("store: parsing %s: %w", path, err)
	}
	if doc.Version > ruleFileVersion {
		return nil, fmt.Errorf("store: %s was written by a newer version (%d > %d); refusing to truncate it",
			path, doc.Version, ruleFileVersion)
	}
	for _, r := range doc.Rules {
		s.rules[r.Name] = r
	}
	return s, nil
}

// ListRules implements RuleStore.
func (s *FileStore) ListRules(_ context.Context) ([]ratelimit.Rule, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.snapshotLocked(), nil
}

// PutRule implements RuleStore.
func (s *FileStore) PutRule(_ context.Context, rule ratelimit.Rule) error {
	if err := rule.Validate(); err != nil {
		return err
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	s.rules[rule.Name] = rule
	return s.flushLocked()
}

// DeleteRule implements RuleStore.
func (s *FileStore) DeleteRule(_ context.Context, name string) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if _, ok := s.rules[name]; !ok {
		return nil
	}
	delete(s.rules, name)
	return s.flushLocked()
}

// Close implements RuleStore. The file store holds no resources beyond the
// file itself, which is only open during a write.
func (s *FileStore) Close() error { return nil }

func (s *FileStore) snapshotLocked() []ratelimit.Rule {
	out := make([]ratelimit.Rule, 0, len(s.rules))
	for _, r := range s.rules {
		out = append(out, r)
	}
	ratelimit.SortRules(out)
	return out
}

// flushLocked rewrites the document. The caller holds the write lock.
func (s *FileStore) flushLocked() error {
	doc := ruleFile{Version: ruleFileVersion, Rules: s.snapshotLocked()}
	data, err := json.MarshalIndent(doc, "", "  ")
	if err != nil {
		return fmt.Errorf("store: encoding rules: %w", err)
	}
	data = append(data, '\n')

	dir := filepath.Dir(s.path)
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return fmt.Errorf("store: creating %s: %w", dir, err)
	}
	// Same directory, so the rename is atomic on the same filesystem: a reader
	// (or a crash) sees either the previous document or the new one, never a
	// half-written one.
	tmp, err := os.CreateTemp(dir, filepath.Base(s.path)+".tmp*")
	if err != nil {
		return fmt.Errorf("store: creating temp file in %s: %w", dir, err)
	}
	tmpName := tmp.Name()
	defer func() { _ = os.Remove(tmpName) }() // no-op once the rename succeeds

	if _, err := tmp.Write(data); err != nil {
		_ = tmp.Close()
		return fmt.Errorf("store: writing %s: %w", tmpName, err)
	}
	if err := tmp.Sync(); err != nil {
		_ = tmp.Close()
		return fmt.Errorf("store: syncing %s: %w", tmpName, err)
	}
	if err := tmp.Close(); err != nil {
		return fmt.Errorf("store: closing %s: %w", tmpName, err)
	}
	if err := os.Chmod(tmpName, 0o644); err != nil {
		return fmt.Errorf("store: chmod %s: %w", tmpName, err)
	}
	if err := os.Rename(tmpName, s.path); err != nil {
		return fmt.Errorf("store: replacing %s: %w", s.path, err)
	}
	return nil
}
