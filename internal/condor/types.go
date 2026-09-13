package condor

import (
	"context"
	"time"

	"github.com/PelicanPlatform/classad/classad"
	htcondor "github.com/bbockelm/golang-htcondor"
	"github.com/bbockelm/pelican-ap-manager/internal/state"
)

// TransferRecord represents a single transfer attempt drawn from the HTCondor epoch history.
type TransferRecord struct {
	EpochID state.EpochID
	// Kind is the archive row's own type -- INPUT, OUTPUT or CHECKPOINT -- kept
	// separate from Direction, which is derived and lossy (a checkpoint has no
	// direction of its own and is reported as an upload). It is what identifies
	// a row within an epoch, since one epoch can produce several.
	Kind string
	// Seq is this record's position within the archive row it came from: one row
	// expands into one record per file, all sharing EpochID and Kind, so the
	// position is what tells them apart. Stable for a given row, because the
	// expansion walks the row's files in order.
	Seq               int
	User              string
	Endpoint          string
	Site              string
	Direction         string
	Success           bool
	EndedAt           time.Time
	JobRuntime        time.Duration
	Files             []TransferFile
	SandboxName       string
	SandboxSize       int64
	WallClockDuration time.Duration // Actual elapsed time from first file start to last file end
}

// JobEpochRecord represents a completed job drawn from the HTCondor job epoch history.
type JobEpochRecord struct {
	EpochID           state.EpochID
	User              string
	Site              string
	Runtime           time.Duration // Total time including setup/teardown (ActivationDuration)
	ExecutionDuration time.Duration // Execution time only (ActivationExecutionDuration)
	EndedAt           time.Time
	Success           bool
}

// TransferFile captures file-level details from a transfer ad.
type TransferFile struct {
	URL          string
	LastEndpoint string // The endpoint from the final attempt
	Bytes        int64
	TotalBytes   int64
	DurationSec  float64
	Start        time.Time
	End          time.Time
	Cached       bool
	Success      bool
	Attempts     []TransferAttempt
	Direction    string
}

// TransferAttempt records a single try within a transfer, including endpoint, cache hint, bytes, and timing.
type TransferAttempt struct {
	Endpoint    string
	Cached      bool
	Bytes       int64   // Bytes transferred in this attempt (TransferFileBytesN)
	DurationSec float64 // Time taken for this attempt (TransferTimeN)
}

// CondorClient abstracts the interactions with HTCondor needed by pelican-man.
type CondorClient interface {
	FetchTransferEpochs(sinceEpoch state.EpochID, cutoff time.Time) ([]TransferRecord, state.EpochID, error)
	FetchJobEpochs(sinceEpoch state.EpochID, cutoff time.Time) ([]JobEpochRecord, state.EpochID, error)
	AdvertiseClassAds(payload []map[string]any) error
	QueryJobs(ctx context.Context, constraint string, projection []string) ([]*classad.ClassAd, error)

	// LocateSchedd resolves the schedd this manager is responsible for. It is
	// on the interface because the rate-limit manager needs the same schedd the
	// history queries use -- and, critically, the same configured collector. A
	// second lookup built from an empty collector address fails with "no address
	// specified in client configuration", which silently disables rate limiting.
	LocateSchedd(ctx context.Context) (*htcondor.Schedd, error)
}
