package condor

import (
	"testing"

	condorconfig "github.com/bbockelm/golang-htcondor/config"
)

// testCondorConfig builds an in-memory HTCondor configuration, so tests do not
// need a condor_config on disk.
func testCondorConfig(t *testing.T) *condorconfig.Config {
	t.Helper()
	t.Setenv("CONDOR_CONFIG", "ONLY_ENV")
	cfg, err := condorconfig.New()
	if err != nil {
		t.Fatalf("condor config: %v", err)
	}
	return cfg
}
