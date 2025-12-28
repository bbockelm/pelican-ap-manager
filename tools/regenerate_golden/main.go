package main

import (
	"log"
	"time"

	"github.com/bbockelm/pelican-ap-manager/internal/epochhistory"
)

func main() {
	now := time.Date(2025, 1, 15, 12, 0, 0, 0, time.UTC)

	if err := epochhistory.Generate(
		"artifacts/integration_history/epoch_history",
		"internal/condor/testdata/job_epochs_from_transfers.sanitized.json",
		"internal/condor/testdata/transfers.sanitized.json",
		now,
	); err != nil {
		log.Fatalf("generate epoch_history: %v", err)
	}

	log.Println("Successfully regenerated artifacts/integration_history/epoch_history")
}
