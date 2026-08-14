//go:build integration

package integration

import (
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/bbockelm/pelican-ap-manager/internal/epochhistory"
)

// seedEpochHistory pre-populates the spool/epoch_history file with sanitized ads so
// tests can exercise history parsing without waiting for new condor writes.
func seedEpochHistory(t *testing.T, moduleRoot, spoolDir string) {
	t.Helper()

	jobPath := filepath.Join(moduleRoot, "internal", "condor", "testdata", "job_epochs_from_transfers.sanitized.json")
	transferPath := filepath.Join(moduleRoot, "internal", "condor", "testdata", "transfers.sanitized.json")
	target := filepath.Join(spoolDir, "epoch_history")

	if err := epochhistory.Generate(target, jobPath, transferPath, time.Now()); err != nil {
		t.Fatalf("seed epoch_history: %v", err)
	}
}

func moduleRoot(t *testing.T) string {
	t.Helper()
	cwd, err := os.Getwd()
	if err != nil {
		t.Fatalf("getwd: %v", err)
	}
	return filepath.Dir(cwd)
}
