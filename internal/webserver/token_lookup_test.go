package webserver

import (
	"database/sql"
	"fmt"
	"path/filepath"
	"testing"
	"time"

	"golang.org/x/crypto/bcrypt"
)

func newTestDB(t *testing.T) *DB {
	t.Helper()
	db, err := NewDB(filepath.Join(t.TempDir(), "tokens.db"))
	if err != nil {
		t.Fatalf("NewDB: %v", err)
	}
	t.Cleanup(func() { _ = db.Close() })
	return db
}

// TestValidateTokenExaminesOneCandidate is the regression test for a
// self-inflicted denial of service.
//
// Validation used to compare the presented token against every unexpired token
// with bcrypt, which is deliberately slow: measured at ~60ms per live token, so
// 200 live tokens meant 12 seconds per sandbox request, and it grew linearly
// with the number of jobs registered in the last 24 hours.
//
// Counting the comparisons rather than timing them: the property is "one
// candidate row", and a wall-clock assertion could only approximate that.
func TestValidateTokenExaminesOneCandidate(t *testing.T) {
	db := newTestDB(t)

	// Enough that a scan would be obvious in the count, few enough that seeding
	// them stays fast: each registration costs a bcrypt at the default cost.
	// Enough that a scan would be obvious in the count, few enough that seeding
	// them stays fast: each registration costs a bcrypt at the default cost.
	const live = 25
	var wanted string
	for i := 0; i < live; i++ {
		tok, _, err := db.RegisterJob(fmt.Sprintf("%d.0", i), `{"a":1}`, "alice", 1000, 1000)
		if err != nil {
			t.Fatalf("RegisterJob %d: %v", i, err)
		}
		wanted = tok
	}

	// The token registered *last*. Which one is asked for matters: a scan runs
	// in insertion order, so asking for the first token would find it on
	// comparison one and a scan would be indistinguishable from a lookup. The
	// last token is the scan's worst case, and the only choice that makes the
	// count mean what it claims.
	before := bcryptComparisons.Load()
	jobID, uid, _, _, err := db.ValidateToken(wanted)
	if err != nil {
		t.Fatalf("ValidateToken: %v", err)
	}
	compares := bcryptComparisons.Load() - before

	if want := fmt.Sprintf("%d.0", live-1); jobID != want || uid != 1000 {
		t.Errorf("validated as job %q uid %d, want %s / 1000", jobID, uid, want)
	}
	if compares != 1 {
		t.Errorf("%d bcrypt comparisons against %d live tokens, want 1: validation is scanning again",
			compares, live)
	}
}

// TestEveryIssuedTokenValidates: the lookup addresses one row out of many, so a
// token in the middle of the table has to resolve as reliably as one at either
// end.
func TestEveryIssuedTokenValidates(t *testing.T) {
	db := newTestDB(t)

	tokens := map[string]string{}
	for i := 0; i < 5; i++ {
		jobID := fmt.Sprintf("%d.0", i)
		tok, _, err := db.RegisterJob(jobID, fmt.Sprintf(`{"n":%d}`, i), "alice", 1000, 1000)
		if err != nil {
			t.Fatalf("RegisterJob: %v", err)
		}
		tokens[tok] = jobID
	}

	for tok, wantJob := range tokens {
		gotJob, _, _, adJSON, err := db.ValidateToken(tok)
		if err != nil {
			t.Errorf("token for job %s was rejected: %v", wantJob, err)
			continue
		}
		if gotJob != wantJob {
			t.Errorf("token for job %s resolved to %s", wantJob, gotJob)
		}
		if adJSON == "" {
			t.Errorf("token for job %s returned no job ad", wantJob)
		}
	}
}

// TestUnknownTokenCostsNothing: a token that matches no row must not be a way
// to make the daemon do bcrypt work. Otherwise anyone who can reach the
// endpoint can spend its CPU without holding a credential at all.
func TestUnknownTokenCostsNothing(t *testing.T) {
	db := newTestDB(t)
	for i := 0; i < 20; i++ {
		if _, _, err := db.RegisterJob(fmt.Sprintf("%d.0", i), "{}", "alice", 1000, 1000); err != nil {
			t.Fatalf("RegisterJob: %v", err)
		}
	}

	before := bcryptComparisons.Load()
	if _, _, _, _, err := db.ValidateToken("pelican_token_not-a-real-token"); err == nil {
		t.Error("an unknown token validated")
	}
	if got := bcryptComparisons.Load() - before; got != 0 {
		t.Errorf("%d bcrypt comparisons for an unknown token, want 0", got)
	}
}

// TestLegacyTokensStillValidate: tokens issued before lookup_hash existed have
// none, and they stay valid for up to their 24-hour lifetime after an upgrade.
// Rejecting them would break every job holding one at the moment pelican_web
// restarts.
func TestLegacyTokensStillValidate(t *testing.T) {
	db := newTestDB(t)

	// A registration whose token row predates lookup_hash.
	var regID int64
	err := db.db.QueryRow(`
		INSERT INTO job_registrations (job_id, job_ad_json, owner, owner_uid, owner_gid, created_at, updated_at)
		VALUES (?, ?, ?, ?, ?, ?, ?) RETURNING id
	`, "77.0", `{"legacy":true}`, "bob", 1001, 1001, time.Now(), time.Now()).Scan(&regID)
	if err != nil {
		t.Fatalf("insert registration: %v", err)
	}

	legacy := tokenPrefix + "legacy-token-value"
	hashed, err := bcrypt.GenerateFromPassword([]byte(legacy), bcrypt.MinCost)
	if err != nil {
		t.Fatalf("bcrypt: %v", err)
	}
	if _, err := db.db.Exec(`
		INSERT INTO job_tokens (job_registration_id, hashed_token, expires_at, created_at)
		VALUES (?, ?, ?, ?)
	`, regID, string(hashed), time.Now().Add(time.Hour), time.Now()); err != nil {
		t.Fatalf("insert legacy token: %v", err)
	}

	jobID, uid, _, adJSON, err := db.ValidateToken(legacy)
	if err != nil {
		t.Fatalf("a legacy token was rejected: %v", err)
	}
	if jobID != "77.0" || uid != 1001 || adJSON != `{"legacy":true}` {
		t.Errorf("legacy token resolved to job %q uid %d ad %q", jobID, uid, adJSON)
	}
}

// TestExpiredTokensAreRejected: the expiry filter moved into the indexed lookup,
// which is exactly where a condition gets dropped by accident.
func TestExpiredTokensAreRejected(t *testing.T) {
	db := newTestDB(t)

	tok, _, err := db.RegisterJob("5.0", "{}", "alice", 1000, 1000)
	if err != nil {
		t.Fatalf("RegisterJob: %v", err)
	}
	if _, _, _, _, err := db.ValidateToken(tok); err != nil {
		t.Fatalf("a fresh token was rejected: %v", err)
	}

	if _, err := db.db.Exec(`UPDATE job_tokens SET expires_at = ?`, time.Now().Add(-time.Minute)); err != nil {
		t.Fatalf("expiring the token: %v", err)
	}
	if _, _, _, _, err := db.ValidateToken(tok); err == nil {
		t.Error("an expired token validated")
	}
}

// TestSchemaMigrationAddsLookupHash: an existing deployment's database has no
// lookup_hash column, and sqlite has no ADD COLUMN IF NOT EXISTS -- so both the
// first open and every one after it have to work.
func TestSchemaMigrationAddsLookupHash(t *testing.T) {
	path := filepath.Join(t.TempDir(), "old.db")

	// Build the pre-migration schema by hand.
	raw, err := sql.Open("sqlite", path)
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	if _, err := raw.Exec(`
		CREATE TABLE job_registrations (
			id INTEGER PRIMARY KEY AUTOINCREMENT, job_id TEXT NOT NULL, job_ad_json TEXT NOT NULL,
			owner TEXT NOT NULL, owner_uid INTEGER NOT NULL, owner_gid INTEGER NOT NULL,
			created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP, updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP);
		CREATE TABLE job_tokens (
			id INTEGER PRIMARY KEY AUTOINCREMENT, job_registration_id INTEGER NOT NULL,
			hashed_token TEXT NOT NULL, expires_at TIMESTAMP NOT NULL,
			created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
			FOREIGN KEY (job_registration_id) REFERENCES job_registrations(id) ON DELETE CASCADE);
	`); err != nil {
		t.Fatalf("create old schema: %v", err)
	}
	if err := raw.Close(); err != nil {
		t.Fatalf("close: %v", err)
	}

	// Two opens: the first migrates, the second must not trip over its own work.
	for attempt := 1; attempt <= 2; attempt++ {
		db, err := NewDB(path)
		if err != nil {
			t.Fatalf("open %d of a pre-migration database: %v", attempt, err)
		}
		if !hasColumn(db.db, "job_tokens", "lookup_hash") {
			t.Fatalf("open %d did not add lookup_hash", attempt)
		}
		// And it works end to end afterwards.
		tok, _, err := db.RegisterJob("9.0", "{}", "alice", 1000, 1000)
		if err != nil {
			t.Fatalf("RegisterJob after migration: %v", err)
		}
		before := bcryptComparisons.Load()
		if _, _, _, _, err := db.ValidateToken(tok); err != nil {
			t.Fatalf("ValidateToken after migration: %v", err)
		}
		if got := bcryptComparisons.Load() - before; got != 1 {
			t.Errorf("%d comparisons after migration, want 1: the index is not being used", got)
		}
		if err := db.Close(); err != nil {
			t.Fatalf("close: %v", err)
		}
	}
}

// TestTokensAreDistinguishable: the lookup key must actually be per-token, or
// one job's token would resolve to another job's sandbox.
func TestTokensAreDistinguishable(t *testing.T) {
	db := newTestDB(t)

	first, _, err := db.RegisterJob("1.0", `{"n":1}`, "alice", 1000, 1000)
	if err != nil {
		t.Fatalf("RegisterJob: %v", err)
	}
	second, _, err := db.RegisterJob("2.0", `{"n":2}`, "bob", 1001, 1001)
	if err != nil {
		t.Fatalf("RegisterJob: %v", err)
	}
	if first == second {
		t.Fatal("two registrations produced the same token")
	}
	if tokenLookupHash(first) == tokenLookupHash(second) {
		t.Fatal("two distinct tokens share a lookup hash")
	}

	for _, tc := range []struct {
		token, wantJob string
		wantUID        int
	}{
		{first, "1.0", 1000},
		{second, "2.0", 1001},
	} {
		jobID, uid, _, _, err := db.ValidateToken(tc.token)
		if err != nil {
			t.Fatalf("ValidateToken: %v", err)
		}
		if jobID != tc.wantJob || uid != tc.wantUID {
			t.Errorf("token resolved to job %q uid %d, want %q / %d", jobID, uid, tc.wantJob, tc.wantUID)
		}
	}
}
