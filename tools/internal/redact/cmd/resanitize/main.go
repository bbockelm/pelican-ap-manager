package main

import (
	"encoding/json"
	"log"
	"os"
	"path/filepath"

	"github.com/bbockelm/pelican-ap-manager/tools/internal/redact"
)

var filePairs = []struct {
	raw       string
	sanitized string
}{
	{"internal/condor/testdata/transfers_5.raw.json", "internal/condor/testdata/transfers_5.sanitized.json"},
	{"internal/condor/testdata/job_epochs_5.raw.json", "internal/condor/testdata/job_epochs_5.sanitized.json"},
	{"internal/condor/testdata/job_epochs_from_transfers_5.raw.json", "internal/condor/testdata/job_epochs_from_transfers_5.sanitized.json"},
}

func loadRecords(path string) ([]map[string]any, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return nil, err
	}

	var records []map[string]any
	if err := json.Unmarshal(data, &records); err != nil {
		return nil, err
	}
	return records, nil
}

func saveRecords(path string, records []map[string]any) error {
	if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
		return err
	}

	f, err := os.Create(path)
	if err != nil {
		return err
	}
	defer func() { _ = f.Close() }()

	enc := json.NewEncoder(f)
	enc.SetIndent("", "  ")
	return enc.Encode(records)
}

func main() {
	dictPath := "internal/condor/testdata/redaction_dict.json"

	redactor := redact.NewRedactor()
	if err := redactor.Load(dictPath); err != nil {
		log.Fatalf("load dictionary: %v", err)
	}

	for _, pair := range filePairs {
		records, err := loadRecords(pair.raw)
		if err != nil {
			log.Fatalf("load %s: %v", pair.raw, err)
		}

		sanitized := redactor.SanitizeRecords(records)
		if err := saveRecords(pair.sanitized, sanitized); err != nil {
			log.Fatalf("save %s: %v", pair.sanitized, err)
		}
	}

	if err := redactor.Save(dictPath); err != nil {
		log.Fatalf("save dictionary: %v", err)
	}
}
