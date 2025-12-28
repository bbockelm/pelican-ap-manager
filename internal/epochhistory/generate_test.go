package epochhistory

import (
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/PelicanPlatform/classad/classad"
)

func TestGenerateMatchesGolden(t *testing.T) {
	cwd, err := os.Getwd()
	if err != nil {
		t.Fatalf("getwd: %v", err)
	}
	moduleRoot := filepath.Dir(filepath.Dir(cwd))

	jobPath := filepath.Join(moduleRoot, "internal", "condor", "testdata", "job_epochs_from_transfers_5.sanitized.json")
	transferPath := filepath.Join(moduleRoot, "internal", "condor", "testdata", "transfers_5.sanitized.json")
	golden := filepath.Join(moduleRoot, "artifacts", "integration_history", "epoch_history")

	tmp := filepath.Join(t.TempDir(), "epoch_history")
	anchor := time.Unix(1_700_000_000, 0)
	if err := Generate(tmp, jobPath, transferPath, anchor); err != nil {
		t.Fatalf("generate: %v", err)
	}

	got, err := os.ReadFile(tmp)
	if err != nil {
		t.Fatalf("read generated: %v", err)
	}
	want, err := os.ReadFile(golden)
	if err != nil {
		t.Fatalf("read golden: %v", err)
	}

	if os.Getenv("UPDATE_EPOCH_HISTORY") == "1" {
		if err := os.WriteFile(golden, got, 0o644); err != nil {
			t.Fatalf("update golden: %v", err)
		}
		want = got
	}

	if string(got) != string(want) {
		t.Fatalf("generated epoch_history does not match golden; regenerate %s using Generate with anchor %v", golden, anchor)
	}
}

func TestMarshalOldDeterministic(t *testing.T) {
	ad := classad.New()
	if err := ad.Set("NumVacatesByReason", map[string]any{
		"TransferInputErrorPelican": 2,
		"FailedToActivateClaim":     1,
		"TransferInputError":        3,
		"TransferInputErrorCedar":   1,
		"ReconnectFailed":           2,
	}); err != nil {
		t.Fatalf("set map: %v", err)
	}
	if err := ad.Set("InputPluginInvocations", []any{
		map[string]any{"PluginName": "alpha", "InvocationCount": 2},
		map[string]any{"PluginName": "beta", "InvocationCount": 1},
	}); err != nil {
		t.Fatalf("set slice: %v", err)
	}

	first := ad.MarshalOld()
	second := ad.MarshalOld()
	if first != second {
		t.Fatalf("MarshalOld output is not deterministic:\nfirst:\n%s\nsecond:\n%s", first, second)
	}
}
