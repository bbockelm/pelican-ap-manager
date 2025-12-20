package condor

import (
	"testing"
	"time"

	"github.com/PelicanPlatform/classad/classad"
	"github.com/bbockelm/pelican-ap-manager/internal/state"
)

func TestComputeSandboxNormalization(t *testing.T) {
	files := []TransferFile{
		{URL: "osdf:///ospool/ap40//data/foo?token=abc", Bytes: 10},
		{URL: "osdf:///ospool/ap40/data//bar//?x=1", Bytes: 20},
	}

	sandbox, size := computeSandbox(files)
	if size != 30 {
		t.Fatalf("expected size 30, got %d", size)
	}

	filesNorm := []TransferFile{
		{URL: "osdf:///ospool/ap40/data/foo", Bytes: 10},
		{URL: "osdf:///ospool/ap40/data/bar", Bytes: 20},
	}
	cleaned, _ := computeSandbox(filesNorm)
	if sandbox != cleaned {
		t.Fatalf("expected sandbox to ignore query/dup slashes; got %s vs %s", sandbox, cleaned)
	}
}

func TestExtractFilesAttempts(t *testing.T) {
	ad := classad.New()
	ad.Set("InputPluginResultList", []any{
		map[string]any{
			"TransferUrl":        "osdf:///ospool/ap40/data/foo",
			"TransferFileBytes":  int64(10),
			"TransferTotalBytes": int64(10),
			"TransferStartTime":  int64(1),
			"TransferEndTime":    int64(2),
			"TransferSuccess":    true,
			"DeveloperData": map[string]any{
				"Endpoint0": "origin-A",
				"DataAge0":  float64(0),
				"Endpoint1": "cache-B",
				"DataAge1":  float64(5),
			},
		},
	})

	defaultEndpoint := "fallback"
	files := extractFiles(ad, defaultEndpoint, time.Unix(5, 0))
	if len(files) != 1 {
		t.Fatalf("expected 1 file, got %d", len(files))
	}

	f := files[0]
	if f.Endpoint != "cache-B" {
		t.Fatalf("expected final endpoint cache-B, got %s", f.Endpoint)
	}
	if !f.Cached {
		t.Fatalf("expected final cached=true")
	}
	if len(f.Attempts) != 2 {
		t.Fatalf("expected 2 attempts, got %d", len(f.Attempts))
	}
	if f.Attempts[0].Endpoint != "origin-A" || f.Attempts[0].Cached {
		t.Fatalf("attempt 0 mismatch: %+v", f.Attempts[0])
	}
	if f.Attempts[1].Endpoint != "cache-B" || !f.Attempts[1].Cached {
		t.Fatalf("attempt 1 mismatch: %+v", f.Attempts[1])
	}
}

func TestConvertTransferAdDownloadDirection(t *testing.T) {
	ad := classad.New()
	ad.Set("TransferType", "download")
	ad.Set("TransferEndpoint", "osdf-cache")
	ad.Set("Owner", "alice")
	ad.Set("TransferEndTime", int64(2))
	ad.Set("TransferStartTime", int64(1))
	ad.Set("TransferSuccess", true)
	ad.Set("InputPluginResultList", []any{
		map[string]any{
			"TransferUrl":        "osdf:///ospool/ap40/data/foo",
			"TransferFileBytes":  int64(10),
			"TransferTotalBytes": int64(10),
			"TransferStartTime":  int64(1),
			"TransferEndTime":    int64(2),
			"TransferSuccess":    true,
			"DeveloperData": map[string]any{
				"Endpoint0": "osdf-cache",
				"DataAge0":  float64(0),
			},
		},
	})

	c := htcClient{}
	recs, _ := c.convertTransferAd(ad)
	if len(recs) != 1 {
		t.Fatalf("expected 1 transfer record, got %d", len(recs))
	}
	if recs[0].Direction != string(state.DirectionDownload) {
		t.Fatalf("expected download direction, got %s", recs[0].Direction)
	}
}

func TestConvertJobEpochAdSuccess(t *testing.T) {
	ad := classad.New()
	ad.Set("ClusterId", int64(1))
	ad.Set("ProcId", int64(2))
	ad.Set("RunInstanceID", int64(3))
	ad.Set("Owner", "bob")
	ad.Set("JobStatus", int64(4))
	ad.Set("JobStartDate", int64(10))
	ad.Set("EnteredCurrentStatus", int64(70))
	ad.Set("RemoteWallClockTime", int64(50))

	c := htcClient{}
	rec, run := c.convertJobEpochAd(ad)
	if rec == nil {
		t.Fatalf("expected record")
	}
	if rec.Runtime != 50*time.Second {
		t.Fatalf("expected runtime 50s, got %s", rec.Runtime)
	}
	if run.RunInstanceID != 3 {
		t.Fatalf("unexpected run id: %+v", run)
	}
}

func TestConvertJobEpochAdFallbackRuntime(t *testing.T) {
	ad := classad.New()
	ad.Set("Owner", "alice")
	ad.Set("JobStatus", int64(4))
	ad.Set("JobStartDate", int64(100))
	ad.Set("EnteredCurrentStatus", int64(160))

	c := htcClient{}
	rec, _ := c.convertJobEpochAd(ad)
	if rec == nil {
		t.Fatalf("expected record")
	}
	if rec.Runtime != 60*time.Second {
		t.Fatalf("expected fallback runtime 60s, got %s", rec.Runtime)
	}
}
