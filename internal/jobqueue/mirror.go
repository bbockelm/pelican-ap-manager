package jobqueue

import (
	"context"
	"errors"
	"fmt"
	"os"
	"strings"
	"sync"

	"github.com/PelicanPlatform/classad/classad"
	"github.com/bbockelm/golang-htcondor/classadlog"
	htcondorlogging "github.com/bbockelm/golang-htcondor/logging"
	"github.com/bbockelm/pelican-ap-manager/internal/condor"
)

// Key uniquely identifies a job in the queue.
type Key struct {
	ClusterID int64
	ProcID    int64
}

// JobInfo is a whitelisted view of a job ad.
type JobInfo struct {
	Owner               string
	TransferInput       string
	TransferOutput      string
	TransferInputRemaps string
	Cmd                 string
	Args                string
	Iwd                 string
	GlobalJobID         string
	JobStatus           int64
	EnteredCurrentState int64
}

// Mirror maintains an in-memory snapshot of the schedd job queue.
type Mirror struct {
	mu               sync.RWMutex
	jobs             map[Key]JobInfo
	reader           *classadlog.Reader
	client           condor.CondorClient
	projection       []string
	logger           *htcondorlogging.Logger
	logPath          string
	logMissingLogged bool
	logDirLogged     bool
	logInitErrLogged bool
	logEmptyLogged   bool
}

var defaultProjection = []string{
	"ClusterId",
	"ProcId",
	"Owner",
	"TransferInput",
	"TransferOutput",
	"TransferInputRemaps",
	"Cmd",
	"Args",
	"Iwd",
	"GlobalJobId",
	"JobStatus",
	"EnteredCurrentStatus",
}

// NewMirror constructs a mirror using the job queue log when available; otherwise it falls back to schedd polling.
func NewMirror(jobQueueLogPath string, client condor.CondorClient, logger *htcondorlogging.Logger) (*Mirror, error) {
	m := &Mirror{
		jobs:       make(map[Key]JobInfo),
		client:     client,
		projection: defaultProjection,
		logger:     logger,
		logPath:    jobQueueLogPath,
	}

	if err := m.tryInitLogReader(); err != nil {
		return nil, err
	}

	if m.reader == nil && jobQueueLogPath == "" {
		m.note("job queue log path not set; using schedd polling")
	}

	return m, nil
}

// Sync refreshes the in-memory job snapshot from the log (preferred) or a schedd query.
func (m *Mirror) Sync(ctx context.Context) error {
	if m.reader == nil {
		if err := m.tryInitLogReader(); err != nil {
			m.note("job queue log init error: %v", err)
		}
	}

	if m.reader != nil {
		if err := m.reader.Poll(ctx); err != nil {
			return fmt.Errorf("poll job queue log: %w", err)
		}
		ads, err := m.reader.Query("true", m.projection)
		if err != nil {
			return fmt.Errorf("query job queue log: %w", err)
		}
		if len(ads) > 0 {
			m.ingest(ads)
			return nil
		}
		if !m.logEmptyLogged {
			m.note("job queue log returned no ads; falling back to schedd polling")
			m.logEmptyLogged = true
		}
	}

	ads, err := m.client.QueryJobs(ctx, "true", m.projection)
	if err != nil {
		return fmt.Errorf("schedd job query: %w", err)
	}
	m.ingest(ads)
	return nil
}

// Snapshot returns a copy of the current job map.
func (m *Mirror) Snapshot() map[Key]JobInfo {
	m.mu.RLock()
	defer m.mu.RUnlock()

	out := make(map[Key]JobInfo, len(m.jobs))
	for k, v := range m.jobs {
		out[k] = v
	}
	return out
}

// Close releases any resources held by the log reader.
func (m *Mirror) Close() error {
	if m.reader != nil {
		return m.reader.Close()
	}
	return nil
}

func (m *Mirror) ingest(ads []*classad.ClassAd) {
	updates := make(map[Key]JobInfo, len(ads))
	for _, ad := range ads {
		key, info, ok := jobInfoFromAd(ad)
		if !ok {
			continue
		}
		updates[key] = info
	}

	m.mu.Lock()
	defer m.mu.Unlock()
	m.jobs = updates
}

func jobInfoFromAd(ad *classad.ClassAd) (Key, JobInfo, bool) {
	cluster, ok := ad.EvaluateAttrInt("ClusterId")
	if !ok {
		return Key{}, JobInfo{}, false
	}
	proc, ok := ad.EvaluateAttrInt("ProcId")
	if !ok {
		return Key{}, JobInfo{}, false
	}

	owner, _ := ad.EvaluateAttrString("Owner")
	transferInput, _ := ad.EvaluateAttrString("TransferInput")
	transferOutput, _ := ad.EvaluateAttrString("TransferOutput")
	transferInputRemaps, _ := ad.EvaluateAttrString("TransferInputRemaps")
	cmd, _ := ad.EvaluateAttrString("Cmd")
	args, _ := ad.EvaluateAttrString("Args")
	iwd, _ := ad.EvaluateAttrString("Iwd")
	globalJobID, _ := ad.EvaluateAttrString("GlobalJobId")
	jobStatus, _ := ad.EvaluateAttrInt("JobStatus")
	enteredCurrent, _ := ad.EvaluateAttrInt("EnteredCurrentStatus")

	info := JobInfo{
		Owner:               owner,
		TransferInput:       transferInput,
		TransferOutput:      transferOutput,
		TransferInputRemaps: transferInputRemaps,
		Cmd:                 cmd,
		Args:                args,
		Iwd:                 iwd,
		GlobalJobID:         globalJobID,
		JobStatus:           jobStatus,
		EnteredCurrentState: enteredCurrent,
	}

	return Key{ClusterID: cluster, ProcID: proc}, info, true
}

func (m *Mirror) note(format string, args ...any) {
	if m.logger == nil {
		return
	}
	msg := fmt.Sprintf(format, args...)
	msg = strings.TrimSpace(msg)
	m.logger.Infof(htcondorlogging.DestinationGeneral, "job mirror: %s", msg)
}

func (m *Mirror) tryInitLogReader() error {
	if m.reader != nil || m.logPath == "" {
		return nil
	}

	info, err := os.Stat(m.logPath)
	if err != nil {
		if errors.Is(err, os.ErrNotExist) {
			if !m.logMissingLogged {
				m.note("job queue log %s missing; using schedd polling", m.logPath)
				m.logMissingLogged = true
			}
			return nil
		}
		return fmt.Errorf("stat job queue log: %w", err)
	}

	if info.IsDir() {
		if !m.logDirLogged {
			m.note("job queue log path %s is a directory; using schedd polling", m.logPath)
			m.logDirLogged = true
		}
		return nil
	}

	reader, err := classadlog.NewReader(m.logPath)
	if err != nil {
		if !m.logInitErrLogged {
			m.note("create job queue reader: %v", err)
			m.logInitErrLogged = true
		}
		return nil
	}

	m.reader = reader
	m.note("job queue log enabled at %s", m.logPath)
	return nil
}
