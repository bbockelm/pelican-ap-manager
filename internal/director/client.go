package director

import (
	"encoding/json"
	"fmt"
	"log"
	"net/http"
	"net/url"
	"path"
	"strings"
	"time"

	"github.com/jellydator/ttlcache/v3"
)

// Client resolves Pelican namespace prefixes to virtual sources with caching.
type Client struct {
	httpClient     *http.Client
	ttl            time.Duration
	directorCache  *ttlcache.Cache[string, string]
	namespaceCache *ttlcache.Cache[string, string]
}

// New creates a director client with the provided TTL.
func New(ttl time.Duration) *Client {
	directorCache := ttlcache.New[string, string]()
	namespaceCache := ttlcache.New[string, string]()

	go directorCache.Start()
	go namespaceCache.Start()

	return &Client{
		httpClient: &http.Client{
			Timeout: 5 * time.Second,
			CheckRedirect: func(req *http.Request, via []*http.Request) error {
				// We only need headers; avoid following redirects which costs extra round-trips.
				return http.ErrUseLastResponse
			},
		},
		ttl:            ttl,
		directorCache:  directorCache,
		namespaceCache: namespaceCache,
	}
}

// ResolveVirtualSource returns a virtual source (namespace) for a transfer URL.
// It cleans the URL to derive the discovery host and object path, discovers the
// director, issues a HEAD request, and caches director/namespace lookups. Cached
// namespaces are matched by longest prefix. When a scheme/host are present on
// the original URL (e.g., https://cache.example), they are re-attached to the
// resolved namespace; osdf:// URLs do not carry a host so the namespace remains
// path-only.
func (c *Client) ResolveVirtualSource(rawURL string) (string, error) {
	discoveryBase, objectPath, cacheKey, origin, err := c.cleanURL(rawURL)
	if err != nil {
		return "", err
	}

	log.Printf("director: resolve start raw=%s objectPath=%s", rawURL, objectPath)

	if cached := c.lookupCachedNamespace(origin, objectPath); cached != "" {
		log.Printf("director: cache hit objectPath=%s namespace=%s", objectPath, cached)
		return cached, nil
	}

	log.Printf("director: discover director base=%s", discoveryBase)
	directorEndpoint, err := c.director(discoveryBase)
	if err != nil {
		return "", err
	}
	log.Printf("director: director endpoint=%s", directorEndpoint)

	log.Printf("director: fetch namespace path=%s from=%s", objectPath, directorEndpoint)
	namespace, err := c.fetchNamespace(directorEndpoint, objectPath)
	if err != nil {
		return "", err
	}
	resolvedNamespace := namespace
	if origin != "" {
		resolvedNamespace = strings.TrimSuffix(origin, "/") + namespace
	}

	log.Printf("director: resolved namespace=%s for path=%s", resolvedNamespace, objectPath)

	c.namespaceCache.Set(cacheKey, resolvedNamespace, c.ttl)
	return resolvedNamespace, nil
}

// cleanURL normalizes the transfer URL, returning discovery base, object path, cache key, and original origin.
func (c *Client) cleanURL(rawURL string) (string, string, string, string, error) {
	u, err := url.Parse(rawURL)
	if err != nil {
		return "", "", "", "", fmt.Errorf("parse url: %w", err)
	}

	scheme := normalizeScheme(u.Scheme)

	objectPath := path.Clean("/" + strings.TrimPrefix(u.Path, "/"))
	if scheme == "osdf" && u.Host != "" {
		combined := path.Clean("/" + path.Join(u.Host, strings.TrimPrefix(u.Path, "/")))
		objectPath = combined
	}
	if objectPath == "/" {
		return "", "", "", "", fmt.Errorf("empty object path")
	}

	origin := ""
	if scheme != "osdf" && u.Host != "" {
		schemeForOrigin := scheme
		if schemeForOrigin == "" {
			schemeForOrigin = "https"
		}
		origin = fmt.Sprintf("%s://%s", schemeForOrigin, u.Host)
	}

	cacheKey := origin + path.Dir(objectPath)
	if cacheKey == "." {
		cacheKey = objectPath
	}

	base, err := c.discoveryBase(u)
	if err != nil {
		return "", "", "", "", err
	}

	return base, objectPath, cacheKey, origin, nil
}

// discoveryBase derives the discovery URL (host) to use when contacting the director.

func (c *Client) discoveryBase(u *url.URL) (string, error) {
	scheme := normalizeScheme(u.Scheme)
	if scheme == "osdf" {
		return "https://osg-htc.org", nil
	}

	host := u.Host
	if host == "" {
		return "", fmt.Errorf("missing host in url: %s", u.String())
	}

	if scheme != "http" && scheme != "https" {
		scheme = "https"
	}

	return fmt.Sprintf("%s://%s", scheme, host), nil
}

// director returns the director endpoint for a discovery base, using the cache first.
func (c *Client) director(discoveryBase string) (string, error) {
	if item := c.directorCache.Get(discoveryBase); item != nil {
		return item.Value(), nil
	}

	wellKnown := strings.TrimSuffix(discoveryBase, "/") + "/.well-known/pelican-configuration"
	req, err := http.NewRequest(http.MethodGet, wellKnown, nil)
	if err != nil {
		return "", fmt.Errorf("build discovery request: %w", err)
	}

	resp, err := c.httpClient.Do(req)
	if err != nil {
		return "", fmt.Errorf("discover director: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		return "", fmt.Errorf("director discovery returned %d", resp.StatusCode)
	}

	var payload struct {
		DirectorEndpoint string `json:"director_endpoint"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&payload); err != nil {
		return "", fmt.Errorf("decode director discovery: %w", err)
	}
	if payload.DirectorEndpoint == "" {
		return "", fmt.Errorf("director discovery missing endpoint")
	}

	c.directorCache.Set(discoveryBase, payload.DirectorEndpoint, c.ttl)
	return payload.DirectorEndpoint, nil
}

// fetchNamespace issues a HEAD request against the director to retrieve the namespace header.
func (c *Client) fetchNamespace(directorEndpoint, objectPath string) (string, error) {
	url := strings.TrimSuffix(directorEndpoint, "/") + objectPath
	log.Printf("director: HEAD namespace url=%s", url)
	req, err := http.NewRequest(http.MethodHead, url, nil)
	if err != nil {
		return "", fmt.Errorf("build namespace request: %w", err)
	}

	resp, err := c.httpClient.Do(req)
	if err != nil {
		return "", fmt.Errorf("namespace request failed: %w", err)
	}
	defer resp.Body.Close()
	log.Printf("director: HEAD status=%d", resp.StatusCode)

	if resp.StatusCode >= 400 {
		return "", fmt.Errorf("namespace request returned %d", resp.StatusCode)
	}

	namespace := resp.Header.Get("X-Pelican-Namespace")
	if namespace == "" {
		return "", fmt.Errorf("namespace header missing")
	}

	cleaned, err := parseNamespace(namespace)
	if err != nil {
		return "", err
	}
	log.Printf("director: parsed namespace header=%s cleaned=%s", namespace, cleaned)

	return cleaned, nil
}

// parseNamespace extracts the namespace value from the X-Pelican-Namespace header.
// The header often contains comma-separated key/value pairs like
// "namespace=/ospool/ap40, require-token=true, collections-url=https://...".
func parseNamespace(headerVal string) (string, error) {
	for _, part := range strings.Split(headerVal, ",") {
		kv := strings.SplitN(strings.TrimSpace(part), "=", 2)
		if len(kv) != 2 {
			continue
		}
		if !strings.EqualFold(strings.TrimSpace(kv[0]), "namespace") {
			continue
		}
		val := strings.TrimSpace(kv[1])
		cleaned := path.Clean("/" + strings.TrimPrefix(val, "/"))
		if cleaned == "/" {
			return "", fmt.Errorf("namespace header empty after clean")
		}
		return cleaned, nil
	}

	cleaned := path.Clean("/" + strings.TrimSpace(strings.TrimPrefix(headerVal, "/")))
	if cleaned != "/" {
		return cleaned, nil
	}

	return "", fmt.Errorf("namespace header missing namespace key")
}

func normalizeScheme(raw string) string {
	if raw == "" {
		return raw
	}
	if idx := strings.LastIndex(raw, "+"); idx >= 0 && idx+1 < len(raw) {
		return raw[idx+1:]
	}
	return raw
}

// lookupCachedNamespace walks path components, returning the last cache hit before a miss.
// Matching is path-based (component boundaries) to avoid false positives like /foo matching /foobar.
func (c *Client) lookupCachedNamespace(origin, objectPath string) string {
	trimmed := strings.Trim(strings.TrimPrefix(objectPath, "/"), "/")
	if trimmed == "" {
		return ""
	}
	parts := strings.Split(trimmed, "/")
	last := ""
	for i := 1; i <= len(parts); i++ {
		candidate := "/" + strings.Join(parts[:i], "/")
		key := origin + candidate
		item := c.namespaceCache.Get(key)
		if item == nil {
			if last != "" {
				break
			}
			continue
		}
		last = item.Value()
	}
	return last
}
