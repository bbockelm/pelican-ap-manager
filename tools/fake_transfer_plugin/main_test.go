package main

import (
	"errors"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
)

func TestClassAdOutput(t *testing.T) {
	bin := buildPluginBinary(t)

	cmd := exec.Command(bin, "-classad")
	output, err := cmd.CombinedOutput()
	if err != nil {
		t.Fatalf("classad command failed: %v (%s)", err, string(output))
	}

	out := string(output)
	if !strings.Contains(out, "SupportedMethods = \"fake\"") {
		t.Fatalf("missing SupportedMethods in classad: %s", out)
	}
	if !strings.Contains(out, "PluginType = \"FileTransfer\"") {
		t.Fatalf("missing PluginType in classad: %s", out)
	}
}

func TestInputTransferFailureCreatesArtifacts(t *testing.T) {
	bin := buildPluginBinary(t)

	workDir := t.TempDir()
	adPath := filepath.Join(workDir, "job.ad")
	resultPath := filepath.Join(workDir, "result.txt")
	dstPath := filepath.Join(workDir, "dst", "payload.txt")

	adContent := fmt.Sprintf("FakeInputResultStatus = 5\nFakeInputResultFile = \"%s\"\n", resultPath)
	if err := os.WriteFile(adPath, []byte(adContent), 0o644); err != nil {
		t.Fatalf("write job ad: %v", err)
	}

	cmd := exec.Command(bin, "fake:///input/input_fail/payload.txt", dstPath)
	cmd.Env = append(os.Environ(), "_CONDOR_JOB_AD="+adPath)
	output, err := cmd.CombinedOutput()
	exitCode := exitStatus(err)
	if exitCode != 5 {
		t.Fatalf("expected exit code 5, got %d (output: %s, err: %v)", exitCode, string(output), err)
	}

	payload, err := os.ReadFile(dstPath)
	if err != nil {
		t.Fatalf("read payload: %v", err)
	}
	if !strings.Contains(string(payload), "direction=input") || !strings.Contains(string(payload), "status=5") {
		t.Fatalf("unexpected payload contents: %s", string(payload))
	}

	result, err := os.ReadFile(resultPath)
	if err != nil {
		t.Fatalf("read result file: %v", err)
	}
	if !strings.Contains(string(result), "direction=input") || !strings.Contains(string(result), "status=5") {
		t.Fatalf("unexpected result contents: %s", string(result))
	}
}

func buildPluginBinary(t *testing.T) string {
	t.Helper()

	bin := filepath.Join(t.TempDir(), "fake_transfer_plugin")
	wd, err := os.Getwd()
	if err != nil {
		t.Fatalf("getwd: %v", err)
	}

	cmd := exec.Command("go", "build", "-buildvcs=false", "-o", bin, ".")
	cmd.Dir = wd
	cmd.Env = os.Environ()
	if output, err := cmd.CombinedOutput(); err != nil {
		t.Fatalf("build plugin: %v (%s)", err, string(output))
	}
	return bin
}

func exitStatus(err error) int {
	if err == nil {
		return 0
	}
	var ee *exec.ExitError
	if ok := errors.As(err, &ee); ok {
		return ee.ExitCode()
	}
	return -1
}
