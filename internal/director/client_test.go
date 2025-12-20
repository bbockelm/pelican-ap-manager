package director

import (
	"fmt"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"
)

func TestResolveVirtualSourceOSDF(t *testing.T) {
	headHits := 0
	var directorEndpoint string

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch {
		case r.Method == http.MethodGet && r.URL.Path == "/.well-known/pelican-configuration":
			fmt.Fprintf(w, `{"director_endpoint": %q}`, directorEndpoint)
		case r.Method == http.MethodHead && r.URL.Path == "/ospool/ap40/data/foo":
			headHits++
			w.Header().Set("X-Pelican-Namespace", "namespace=/ospool/ap40, require-token=true, collections-url=https://ap40.uw.osg-htc.org:8443")
			w.WriteHeader(http.StatusOK)
		default:
			w.WriteHeader(http.StatusNotFound)
		}
	}))
	defer server.Close()

	directorEndpoint = server.URL

	client := New(time.Minute)
	client.httpClient = server.Client()
	client.directorCache.Set("https://osg-htc.org", directorEndpoint, time.Minute)

	ns, err := client.ResolveVirtualSource("osdf:///ospool/ap40/data/foo")
	if err != nil {
		t.Fatalf("resolve virtual source: %v", err)
	}
	if ns != "/ospool/ap40" {
		t.Fatalf("expected namespace /ospool/ap40, got %s", ns)
	}
	if headHits != 1 {
		t.Fatalf("expected one HEAD request, got %d", headHits)
	}

	cached, err := client.ResolveVirtualSource("osdf:///ospool/ap40/data/bar")
	if err != nil {
		t.Fatalf("cached lookup: %v", err)
	}
	if cached != ns {
		t.Fatalf("expected cached namespace %s, got %s", ns, cached)
	}
	if headHits != 1 {
		t.Fatalf("expected cached lookup to avoid HEAD; got %d", headHits)
	}
}

func TestResolveVirtualSourceWithOrigin(t *testing.T) {
	headHits := 0
	var directorEndpoint string

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch {
		case r.Method == http.MethodGet && r.URL.Path == "/.well-known/pelican-configuration":
			fmt.Fprintf(w, `{"director_endpoint": %q}`, directorEndpoint)
		case r.Method == http.MethodHead && r.URL.Path == "/ospool/ap40/data/foo":
			headHits++
			w.Header().Set("X-Pelican-Namespace", "namespace=/ospool/ap40")
			w.WriteHeader(http.StatusOK)
		default:
			w.WriteHeader(http.StatusNotFound)
		}
	}))
	defer server.Close()

	directorEndpoint = server.URL

	client := New(time.Minute)
	client.httpClient = server.Client()
	client.directorCache.Set("https://cache.example", directorEndpoint, time.Minute)

	ns, err := client.ResolveVirtualSource("https://cache.example/ospool/ap40/data/foo")
	if err != nil {
		t.Fatalf("resolve virtual source: %v", err)
	}
	if ns != "https://cache.example/ospool/ap40" {
		t.Fatalf("expected namespace with origin https://cache.example/ospool/ap40, got %s", ns)
	}
	if headHits != 1 {
		t.Fatalf("expected one HEAD request, got %d", headHits)
	}

	cached, err := client.ResolveVirtualSource("https://cache.example/ospool/ap40/data/bar")
	if err != nil {
		t.Fatalf("cached lookup: %v", err)
	}
	if cached != ns {
		t.Fatalf("expected cached namespace %s, got %s", ns, cached)
	}
	if headHits != 1 {
		t.Fatalf("expected cached lookup to avoid HEAD; got %d", headHits)
	}
}

func TestLookupCachedNamespaceComponentWalk(t *testing.T) {
	client := New(time.Minute)
	client.namespaceCache.Set("/foo", "ns-foo", time.Minute)
	client.namespaceCache.Set("/foo/bar", "ns-bar", time.Minute)

	got := client.lookupCachedNamespace("", "/foo/bar/baz/qux")
	if got != "ns-bar" {
		t.Fatalf("expected deepest hit ns-bar, got %s", got)
	}

	got = client.lookupCachedNamespace("", "/foobar/qux")
	if got != "" {
		t.Fatalf("expected no match for /foobar, got %s", got)
	}
}
