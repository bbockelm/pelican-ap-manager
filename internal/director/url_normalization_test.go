package director

import (
	"net/http"
	"net/http/httptest"
	"testing"
	"time"
)

func TestResolveVirtualSourceCleansPathAndQuery(t *testing.T) {
	headHits := 0
	var srv *httptest.Server
	handler := func(w http.ResponseWriter, r *http.Request) {
		switch {
		case r.Method == http.MethodHead && r.URL.Path == "/ospool/ap40/data/foo":
			headHits++
			w.Header().Set("X-Pelican-Namespace", "namespace=/ospool/ap40")
			w.WriteHeader(http.StatusOK)
		case r.Method == http.MethodGet && r.URL.Path == "/.well-known/pelican-configuration":
			w.Write([]byte(`{"director_endpoint": "` + srv.URL + `"}`))
		default:
			w.WriteHeader(http.StatusNotFound)
		}
	}
	srv = httptest.NewServer(http.HandlerFunc(handler))
	defer srv.Close()

	urls := []string{
		"osdf:///ospool/ap40/data//foo?token=abc",
		"osdf://ospool/ap40/data/foo?token=abc",
	}

	for _, raw := range urls {
		headHits = 0
		client := New(time.Minute)
		client.httpClient = srv.Client()
		client.directorCache.Set("https://osg-htc.org", srv.URL, time.Minute)

		ns, err := client.ResolveVirtualSource(raw)
		if err != nil {
			t.Fatalf("resolve %s: %v", raw, err)
		}
		if ns != "/ospool/ap40" {
			t.Fatalf("expected /ospool/ap40, got %s", ns)
		}
		if headHits != 1 {
			t.Fatalf("expected one HEAD for %s, got %d", raw, headHits)
		}
	}
}

func TestResolveVirtualSourceCredentialScheme(t *testing.T) {
	headHits := 0
	var srv *httptest.Server
	handler := func(w http.ResponseWriter, r *http.Request) {
		switch {
		case r.Method == http.MethodHead && r.URL.Path == "/ospool/ap40/data/foo":
			headHits++
			w.Header().Set("X-Pelican-Namespace", "namespace=/ospool/ap40")
			w.WriteHeader(http.StatusOK)
		case r.Method == http.MethodGet && r.URL.Path == "/.well-known/pelican-configuration":
			w.Write([]byte(`{"director_endpoint": "` + srv.URL + `"}`))
		default:
			w.WriteHeader(http.StatusNotFound)
		}
	}
	srv = httptest.NewServer(http.HandlerFunc(handler))
	defer srv.Close()

	client := New(time.Minute)
	client.httpClient = srv.Client()
	client.directorCache.Set("https://osg-htc.org", srv.URL, time.Minute)

	raw := "foo+bar+osdf:///ospool/ap40/data/foo"
	ns, err := client.ResolveVirtualSource(raw)
	if err != nil {
		t.Fatalf("resolve %s: %v", raw, err)
	}
	if ns != "/ospool/ap40" {
		t.Fatalf("expected /ospool/ap40, got %s", ns)
	}
	if headHits != 1 {
		t.Fatalf("expected one HEAD, got %d", headHits)
	}
}
