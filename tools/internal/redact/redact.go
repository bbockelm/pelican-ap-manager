// Package redact sanitizes collected ClassAds for safer sharing.
package redact

import (
	"crypto/sha1"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strconv"
	"strings"
)

// Dictionary persists redaction mappings for stable anonymization across runs.
type Dictionary struct {
	Users map[string]string `json:"users"`
	Paths map[string]string `json:"paths"`
}

// Redactor applies redaction rules while tracking reversible mappings.
type Redactor struct {
	dict     Dictionary
	nextUser int
}

// NewRedactor creates a redactor with empty mappings.
func NewRedactor() *Redactor {
	return &Redactor{
		dict: Dictionary{
			Users: make(map[string]string),
			Paths: make(map[string]string),
		},
		nextUser: 1,
	}
}

// Load initializes the redactor from an existing dictionary, if present.
func (r *Redactor) Load(path string) error {
	data, err := os.ReadFile(path)
	if errors.Is(err, os.ErrNotExist) {
		return nil
	}
	if err != nil {
		return fmt.Errorf("read redaction dictionary: %w", err)
	}

	var d Dictionary
	if err := json.Unmarshal(data, &d); err != nil {
		return fmt.Errorf("decode redaction dictionary: %w", err)
	}
	if d.Users == nil {
		d.Users = make(map[string]string)
	}
	if d.Paths == nil {
		d.Paths = make(map[string]string)
	}

	r.dict = d
	r.nextUser = nextUserIndex(d.Users)
	return nil
}

// Save writes the dictionary to disk, creating parent directories as needed.
func (r *Redactor) Save(path string) error {
	if path == "" {
		return nil
	}
	if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
		return fmt.Errorf("mkdir redaction dictionary: %w", err)
	}

	f, err := os.Create(path)
	if err != nil {
		return fmt.Errorf("create redaction dictionary: %w", err)
	}
	defer f.Close()

	enc := json.NewEncoder(f)
	enc.SetIndent("", "  ")
	if err := enc.Encode(r.dict); err != nil {
		return fmt.Errorf("write redaction dictionary: %w", err)
	}
	return nil
}

// SanitizeRecords returns deep copies of the provided records with sensitive data redacted.
func (r *Redactor) SanitizeRecords(records []map[string]any) []map[string]any {
	sanitized := make([]map[string]any, 0, len(records))
	for _, rec := range records {
		sanitized = append(sanitized, r.sanitizeMap(rec))
	}
	return sanitized
}

func (r *Redactor) sanitizeMap(in map[string]any) map[string]any {
	// First pass: extract users from owner/acctgroupuser/osuser fields
	// This ensures users are in the dictionary before processing other fields
	for k, v := range in {
		lowerKey := strings.ToLower(k)
		if isUserKey(lowerKey) {
			if str, ok := v.(string); ok {
				r.redactUser(str)
			}
		}
	}

	// Second pass: sanitize all fields
	out := make(map[string]any, len(in))
	for k, v := range in {
		out[k] = r.sanitizeValue(k, v)
	}
	return out
}

func (r *Redactor) sanitizeValue(key string, value any) any {
	switch v := value.(type) {
	case map[string]any:
		return r.sanitizeMap(v)
	case []any:
		items := make([]any, 0, len(v))
		for _, item := range v {
			items = append(items, r.sanitizeValue(key, item))
		}
		return items
	case string:
		return r.sanitizeString(key, v)
	default:
		return value
	}
}

func (r *Redactor) sanitizeString(key, val string) string {
	lowerKey := strings.ToLower(key)

	// Only add to users dictionary from actual user fields
	if isUserKey(lowerKey) {
		return r.redactUser(val)
	}
	if isAccountingKey(lowerKey) {
		return r.redactAccountingGroup(val)
	}

	// For non-user fields, only replace already-known users (don't add new ones)
	withKnownUsers := r.replaceKnownUsers(val)
	parts := strings.Split(withKnownUsers, ",")
	if len(parts) > 1 {
		for i, p := range parts {
			parts[i] = r.sanitizeToken(strings.TrimSpace(p))
		}
		return strings.Join(parts, ",")
	}

	return r.sanitizeToken(strings.TrimSpace(withKnownUsers))
}

func (r *Redactor) sanitizeToken(val string) string {
	if val == "" {
		return val
	}
	val = r.replaceKnownUsers(val)

	if isExpr(val) {
		return val
	}
	if looksLikeURI(val) {
		return r.redactPath(val, true)
	}
	if strings.HasPrefix(val, "/") {
		return r.redactPath(val, false)
	}
	return val
}

func (r *Redactor) redactUser(raw string) string {
	if raw == "" {
		return raw
	}
	if alias, ok := r.dict.Users[raw]; ok {
		return alias
	}
	alias := fmt.Sprintf("user%d", r.nextUser)
	r.nextUser++
	r.dict.Users[raw] = alias
	return alias
}

func (r *Redactor) redactAccountingGroup(raw string) string {
	if raw == "" {
		return raw
	}
	// AccountingGroup format is typically: group_prefix.ProjectName.username
	// Only replace already-known users, don't add new entries
	return r.replaceKnownUsers(raw)
}

func (r *Redactor) redactPath(raw string, keepScheme bool) string {
	if raw == "" {
		return raw
	}
	if isExpr(raw) {
		return raw
	}
	if sanitized, ok := r.dict.Paths[raw]; ok {
		if isValidSanitizedPath(sanitized, keepScheme) {
			return sanitized
		}
	}

	original := raw
	scheme := ""
	path := raw
	if keepScheme {
		parts := strings.SplitN(raw, "://", 2)
		if len(parts) == 2 {
			scheme = parts[0]
			path = parts[1]
		}
	}

	trimmed := strings.TrimPrefix(path, "/")
	segments := strings.Split(trimmed, "/")
	redacted := make([]string, 0, len(segments))
	for i, seg := range segments {
		redacted = append(redacted, r.redactPathSegment(seg, i, segments))
	}

	preserve := 3
	if len(redacted) < preserve {
		preserve = len(redacted)
	}
	sanitizedSegs := append([]string{}, redacted[:preserve]...)
	if len(redacted) > preserve {
		remainder := strings.Join(redacted[preserve:], "/")
		digest := hashToken(remainder)
		sanitizedSegs = append(sanitizedSegs, "sanitized", digest)
	}

	sanitizedPath := "/" + strings.Join(sanitizedSegs, "/")
	if scheme != "" {
		sanitizedPath = fmt.Sprintf("%s://%s", scheme, strings.TrimPrefix(sanitizedPath, "/"))
	}

	r.dict.Paths[original] = sanitizedPath
	return sanitizedPath
}

func (r *Redactor) redactPathSegment(segment string, idx int, all []string) string {
	seg := r.replaceKnownUsers(segment)
	prev := ""
	if idx > 0 {
		prev = all[idx-1]
	}

	if isUserSegment(seg, prev) {
		return sanitizePathUser(seg)
	}

	return seg
}

func (r *Redactor) replaceKnownUsers(val string) string {
	out := val
	for original, alias := range r.dict.Users {
		if strings.Contains(out, original) {
			out = strings.ReplaceAll(out, original, alias)
		}
	}
	return out
}

func isUserKey(lowerKey string) bool {
	// Only these fields should be treated as actual user identifiers
	switch lowerKey {
	case "owner", "acctgroupuser", "osuser":
		return true
	default:
		return false
	}
}

func isAccountingKey(lowerKey string) bool {
	// Only AccountingGroup and RequestedAcctGroup have user.project format
	// AcctGroup is just the project name, not user-related
	switch lowerKey {
	case "accountinggroup", "requestedacctgroup":
		return true
	default:
		return false
	}
}

func looksLikeURI(val string) bool {
	idx := strings.Index(val, "://")
	return idx > 0 && idx < len(val)-3
}

func isExpr(val string) bool {
	return strings.HasPrefix(val, "/Expr(")
}

func isValidSanitizedPath(val string, keepScheme bool) bool {
	rest := val
	if keepScheme {
		parts := strings.SplitN(val, "://", 2)
		if len(parts) != 2 {
			return false
		}
		rest = parts[1]
	}
	rest = strings.TrimPrefix(rest, "/")
	segs := strings.Split(rest, "/")
	if len(segs) == 0 {
		return false
	}
	if segs[0] == "sanitized" {
		return false
	}
	return true
}

func isUserSegment(seg, prev string) bool {
	if seg == "" {
		return false
	}
	if prev == "home" || prev == "homes" || prev == "users" || prev == "user" || prev == "data" || prev == "spool" {
		return true
	}
	if strings.Contains(seg, "@") {
		return true
	}
	if strings.HasPrefix(seg, "user") && len(seg) > len("user") {
		if _, err := strconv.Atoi(strings.TrimPrefix(seg, "user")); err == nil {
			return true
		}
	}
	return false
}

func sanitizePathUser(seg string) string {
	// If the segment is already a redacted username (user\d+), don't hash it again
	if strings.HasPrefix(seg, "user") {
		if _, err := strconv.Atoi(strings.TrimPrefix(seg, "user")); err == nil {
			return seg
		}
	}
	return fmt.Sprintf("user-%s", hashToken(seg))
}

func hashToken(raw string) string {
	sum := sha1.Sum([]byte(raw))
	return hex.EncodeToString(sum[:])[:16]
}

func nextUserIndex(users map[string]string) int {
	maxUser := 0
	for _, alias := range users {
		if strings.HasPrefix(alias, "user") {
			if n, err := strconv.Atoi(strings.TrimPrefix(alias, "user")); err == nil && n > maxUser {
				maxUser = n
			}
		}
	}
	if maxUser == 0 {
		return len(users) + 1
	}
	return maxUser + 1
}
