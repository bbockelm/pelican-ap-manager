package integration

import (
	"context"
	"encoding/json"
	"os"
	"time"

	"github.com/PelicanPlatform/classad/classad"
	"github.com/bbockelm/pelican-ap-manager/internal/condor"
	"github.com/bbockelm/pelican-ap-manager/internal/state"
)

// mockCondorClient implements CondorClient by reading from pre-seeded test data files.
type mockCondorClient struct {
	jobEpochsPath    string
	transfersPath    string
	lastTransferRead bool
	lastJobRead      bool
}

// newMockCondorClient creates a mock that reads from JSON test data files.
func newMockCondorClient(jobEpochsPath, transfersPath string) *mockCondorClient {
	return &mockCondorClient{
		jobEpochsPath: jobEpochsPath,
		transfersPath: transfersPath,
	}
}

func (m *mockCondorClient) FetchTransferEpochs(sinceEpoch state.EpochID, cutoff time.Time) ([]condor.TransferRecord, state.EpochID, error) {
	if m.lastTransferRead {
		// Return empty on subsequent calls to simulate no new data
		return nil, sinceEpoch, nil
	}

	data, err := os.ReadFile(m.transfersPath)
	if err != nil {
		return nil, sinceEpoch, err
	}

	var rawAds []map[string]interface{}
	if err := json.Unmarshal(data, &rawAds); err != nil {
		return nil, sinceEpoch, err
	}

	// For mock, just return empty records but with the correct count
	// The actual parsing is complex and tested elsewhere
	records := make([]condor.TransferRecord, 0, len(rawAds))
	var newestEpoch state.EpochID

	for _, ad := range rawAds {
		cluster, _ := ad["ClusterId"].(float64)
		proc, _ := ad["ProcId"].(float64)
		run, _ := ad["RunInstanceID"].(float64)
		
		epoch := state.EpochID{
			ClusterID:     int64(cluster),
			ProcID:        int64(proc),
			RunInstanceID: int64(run),
		}
		
		if epoch.After(newestEpoch) {
			newestEpoch = epoch
		}

		owner, _ := ad["Owner"].(string)
		endpoint, _ := ad["TransferEndpoint"].(string)
		site, _ := ad["MachineAttrGLIDEIN_Site0"].(string)
		success, _ := ad["TransferSuccess"].(bool)
		transferType, _ := ad["TransferType"].(string)
		
		direction := "download"
		if transferType == "OUTPUT_FILES" || transferType == "CHECKPOINT_FILES" {
			direction = "upload"
		}
		
		endTime := time.Now()
		if endedAt, ok := ad["TransferEndTime"].(float64); ok {
			endTime = time.Unix(int64(endedAt), 0)
		}

		// Parse file-level transfer details from InputPluginResultList/OutputPluginResultList
		files := parseTransferFiles(ad, transferType)

		records = append(records, condor.TransferRecord{
			EpochID:   epoch,
			User:      owner,
			Endpoint:  endpoint,
			Site:      site,
			Direction: direction,
			Success:   success,
			EndedAt:   endTime,
			Files:     files,
		})
	}

	m.lastTransferRead = true
	return records, newestEpoch, nil
}

// parseTransferFiles extracts file-level details from transfer plugin result lists
func parseTransferFiles(ad map[string]interface{}, transferType string) []condor.TransferFile {
	var files []condor.TransferFile
	
	// Determine which result list to use
	resultKey := "InputPluginResultList"
	if transferType == "OUTPUT_FILES" {
		resultKey = "OutputPluginResultList"
	}
	
	resultList, ok := ad[resultKey].([]interface{})
	if !ok || len(resultList) == 0 {
		return files
	}
	
	for _, item := range resultList {
		fileAd, ok := item.(map[string]interface{})
		if !ok {
			continue
		}
		
		url, _ := fileAd["TransferUrl"].(string)
		endpoint, _ := fileAd["TransferEndpoint"].(string)
		bytes, _ := fileAd["TransferFileBytes"].(float64)
		totalBytes, _ := fileAd["TransferTotalBytes"].(float64)
		startTime, _ := fileAd["TransferStartTime"].(float64)
		endTime, _ := fileAd["TransferEndTime"].(float64)
		success, _ := fileAd["TransferSuccess"].(bool)
		duration, _ := fileAd["TransferDurationSeconds"].(float64)
		
		files = append(files, condor.TransferFile{
			URL:         url,
			Endpoint:    endpoint,
			Bytes:       int64(bytes),
			TotalBytes:  int64(totalBytes),
			Start:       time.Unix(int64(startTime), 0),
			End:         time.Unix(int64(endTime), 0),
			Success:     success,
			DurationSec: duration,
			LastAttempt: true,
		})
	}
	
	return files
}

func (m *mockCondorClient) FetchJobEpochs(sinceEpoch state.EpochID, cutoff time.Time) ([]condor.JobEpochRecord, state.EpochID, error) {
	if m.lastJobRead {
		// Return empty on subsequent calls to simulate no new data
		return nil, sinceEpoch, nil
	}

	data, err := os.ReadFile(m.jobEpochsPath)
	if err != nil {
		return nil, sinceEpoch, err
	}

	var rawAds []map[string]interface{}
	if err := json.Unmarshal(data, &rawAds); err != nil {
		return nil, sinceEpoch, err
	}

	records := make([]condor.JobEpochRecord, 0, len(rawAds))
	var newestEpoch state.EpochID

	for _, ad := range rawAds {
		cluster, _ := ad["ClusterId"].(float64)
		proc, _ := ad["ProcId"].(float64)
		run, _ := ad["EpochNumber"].(float64)
		
		epoch := state.EpochID{
			ClusterID:     int64(cluster),
			ProcID:        int64(proc),
			RunInstanceID: int64(run),
		}
		
		if epoch.After(newestEpoch) {
			newestEpoch = epoch
		}

		owner, _ := ad["Owner"].(string)
		site, _ := ad["MachineAttrGLIDEIN_Site0"].(string)
		
		var runtime time.Duration
		if startDate, ok := ad["JobCurrentStartDate"].(float64); ok {
			if endedAt, ok := ad["EnteredCurrentStatus"].(float64); ok {
				runtime = time.Unix(int64(endedAt), 0).Sub(time.Unix(int64(startDate), 0))
			}
		}
		
		endTime := time.Now()
		if endedAt, ok := ad["EnteredCurrentStatus"].(float64); ok {
			endTime = time.Unix(int64(endedAt), 0)
		}

		// Consider job successful if it completed (not held/removed)
		success := true
		if status, ok := ad["JobStatus"].(float64); ok {
			// JobStatus 5 = Held, assume others are success for test
			success = status != 5
		}

		records = append(records, condor.JobEpochRecord{
			EpochID: epoch,
			User:    owner,
			Site:    site,
			Runtime: runtime,
			EndedAt: endTime,
			Success: success,
		})
	}

	m.lastJobRead = true
	return records, newestEpoch, nil
}

func (m *mockCondorClient) AdvertiseClassAds(payload []map[string]any) error {
	// Mock does nothing for advertising - the service uses dry-run mode anyway
	return nil
}

func (m *mockCondorClient) QueryJobs(ctx context.Context, constraint string, projection []string) ([]*classad.ClassAd, error) {
	// Not needed for summarization test
	return nil, nil
}
