package daemon

import (
	"fmt"
	"io"
	"log"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/bbockelm/pelican-ap-manager/internal/condor"
	"github.com/bbockelm/pelican-ap-manager/internal/director"
	"github.com/bbockelm/pelican-ap-manager/internal/state"
)

func TestBuildProcessedTransfersDownloadCached(t *testing.T) {
	svc := &Service{director: director.New(time.Minute), logger: log.New(io.Discard, "", 0)}
	rec := condor.TransferRecord{
		User:      "user",
		Site:      "SiteA",
		Direction: string(state.DirectionDownload),
		Success:   true,
		Files: []condor.TransferFile{{
			URL:          "https://example.org/ospool/ap40/data/foo",
			LastEndpoint: "cache-endpoint",
			Cached:       true,
			Start:        time.Unix(0, 0),
			End:          time.Unix(1, 0),
			Success:      true,
			Attempts: []condor.TransferAttempt{{
				Endpoint:    "cache-endpoint",
				Cached:      true,
				Bytes:       0,
				DurationSec: 1.0,
			}},
		}},
	}

	out := svc.buildProcessedTransfers(rec)
	if len(out) != 1 {
		t.Fatalf("expected 1 transfer, got %d", len(out))
	}
	if out[0].Source != "cache-endpoint" {
		t.Fatalf("download cached source should be endpoint; got %s", out[0].Source)
	}
	if out[0].Destination != "SiteA" {
		t.Fatalf("download destination should be site; got %s", out[0].Destination)
	}
}

func TestBuildProcessedTransfersDownloadAndUploadResolution(t *testing.T) {
	client, headHits, server := newDirector(t)
	defer server.Close()
	svc := &Service{director: client, logger: log.New(io.Discard, "", 0)}

	download := condor.TransferRecord{
		User:      "user",
		Site:      "SiteB",
		Direction: string(state.DirectionDownload),
		Success:   true,
		Files: []condor.TransferFile{{
			URL:          server.URL + "/ospool/ap40/data/foo",
			LastEndpoint: "cache-endpoint",
			Cached:       false,
			Start:        time.Unix(0, 0),
			End:          time.Unix(2, 0),
			Success:      true,
			Attempts: []condor.TransferAttempt{{
				Endpoint:    "cache-endpoint",
				Cached:      false,
				Bytes:       0,
				DurationSec: 2.0,
			}},
		}},
	}

	dOut := svc.buildProcessedTransfers(download)
	if len(dOut) != 1 {
		t.Fatalf("expected 1 download transfer, got %d", len(dOut))
	}
	expectedNS := server.URL + "/ospool/ap40"
	if dOut[0].Source != expectedNS {
		t.Fatalf("download uncached source should be %s; got %s", expectedNS, dOut[0].Source)
	}
	if dOut[0].Destination != "SiteB" {
		t.Fatalf("download destination should be site; got %s", dOut[0].Destination)
	}
	if *headHits == 0 {
		t.Fatalf("expected HEAD lookup for uncached download")
	}

	upload := condor.TransferRecord{
		User:      "user",
		Site:      "SiteC",
		Direction: string(state.DirectionUpload),
		Success:   true,
		Files: []condor.TransferFile{{
			URL:          server.URL + "/ospool/ap40/data/bar",
			LastEndpoint: "edge-endpoint",
			Cached:       false,
			Start:        time.Unix(3, 0),
			End:          time.Unix(4, 0),
			Success:      true,
			Attempts: []condor.TransferAttempt{{
				Endpoint:    "edge-endpoint",
				Cached:      false,
				Bytes:       0,
				DurationSec: 1.0,
			}},
		}},
	}

	uOut := svc.buildProcessedTransfers(upload)
	if len(uOut) != 1 {
		t.Fatalf("expected 1 upload transfer, got %d", len(uOut))
	}
	if uOut[0].Source != "SiteC" {
		t.Fatalf("upload source should be site; got %s", uOut[0].Source)
	}
	if uOut[0].Destination != expectedNS {
		t.Fatalf("upload destination should be %s; got %s", expectedNS, uOut[0].Destination)
	}
}

func newDirector(t *testing.T) (*director.Client, *int, *httptest.Server) {
	t.Helper()
	headHits := 0
	var srv *httptest.Server
	handler := func(w http.ResponseWriter, r *http.Request) {
		switch {
		case r.Method == http.MethodGet && r.URL.Path == "/.well-known/pelican-configuration":
			fmt.Fprintf(w, `{"director_endpoint": %q}`, srv.URL)
		case r.Method == http.MethodHead && strings.HasPrefix(r.URL.Path, "/ospool/ap40"):
			headHits++
			w.Header().Set("X-Pelican-Namespace", "namespace=/ospool/ap40")
			w.WriteHeader(http.StatusOK)
		default:
			w.WriteHeader(http.StatusNotFound)
		}
	}
	srv = httptest.NewServer(http.HandlerFunc(handler))

	client := director.New(time.Minute)

	return client, &headHits, srv
}
