package webserver

import (
	"crypto/rand"
	"crypto/sha256"
	"database/sql"
	"encoding/base64"
	"encoding/hex"
	"fmt"
	"sync/atomic"
	"time"

	_ "github.com/glebarez/sqlite"
	"golang.org/x/crypto/bcrypt"
)

const (
	tokenPrefix     = "pelican_token_"
	tokenLength     = 32
	tokenExpiration = 24 * time.Hour
)

type JobRegistration struct {
	ID        int64
	JobID     string
	JobAdJSON string
	Owner     string
	OwnerUID  int
	OwnerGID  int
	CreatedAt time.Time
	UpdatedAt time.Time
}

type JobToken struct {
	ID                int64
	JobRegistrationID int64
	HashedToken       string
	Salt              string
	ExpiresAt         time.Time
	CreatedAt         time.Time
}

type DB struct {
	db *sql.DB
}

func NewDB(dbPath string) (*DB, error) {
	db, err := sql.Open("sqlite", dbPath)
	if err != nil {
		return nil, fmt.Errorf("failed to open database: %w", err)
	}

	// Enable WAL mode for better concurrency
	if _, err := db.Exec("PRAGMA journal_mode=WAL"); err != nil {
		return nil, fmt.Errorf("failed to enable WAL mode: %w", err)
	}

	// Enforce foreign key constraints
	if _, err := db.Exec("PRAGMA foreign_keys=ON"); err != nil {
		return nil, fmt.Errorf("failed to enforce foreign keys: %w", err)
	}

	// Enable synchronous mode for data integrity
	if _, err := db.Exec("PRAGMA synchronous=NORMAL"); err != nil {
		return nil, fmt.Errorf("failed to set synchronous mode: %w", err)
	}

	// Set busy timeout to allow modest concurrency
	if _, err := db.Exec("PRAGMA busy_timeout=5000"); err != nil {
		return nil, fmt.Errorf("failed to set busy timeout: %w", err)
	}

	if err := initSchema(db); err != nil {
		_ = db.Close()
		return nil, fmt.Errorf("failed to initialize schema: %w", err)
	}

	return &DB{db: db}, nil
}

func (d *DB) Close() error {
	return d.db.Close()
}

func initSchema(db *sql.DB) error {
	schema := `
	CREATE TABLE IF NOT EXISTS job_registrations (
		id INTEGER PRIMARY KEY AUTOINCREMENT,
		job_id TEXT NOT NULL,
		job_ad_json TEXT NOT NULL,
		owner TEXT NOT NULL,
		owner_uid INTEGER NOT NULL,
		owner_gid INTEGER NOT NULL,
		created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
		updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
	);

	CREATE INDEX IF NOT EXISTS idx_job_id ON job_registrations(job_id);
	CREATE INDEX IF NOT EXISTS idx_owner_uid ON job_registrations(owner_uid);

	CREATE TABLE IF NOT EXISTS job_tokens (
		id INTEGER PRIMARY KEY AUTOINCREMENT,
		job_registration_id INTEGER NOT NULL,
		hashed_token TEXT NOT NULL,
		expires_at TIMESTAMP NOT NULL,
		created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
		FOREIGN KEY (job_registration_id) REFERENCES job_registrations(id) ON DELETE CASCADE
	);

	CREATE INDEX IF NOT EXISTS idx_hashed_token ON job_tokens(hashed_token);
	CREATE INDEX IF NOT EXISTS idx_expires_at ON job_tokens(expires_at);
	`

	if _, err := db.Exec(schema); err != nil {
		return fmt.Errorf("failed to create schema: %w", err)
	}

	// lookup_hash is what makes token validation a lookup instead of a scan;
	// see ValidateToken. Added after the fact, so an existing database needs the
	// column bolted on -- sqlite has no ADD COLUMN IF NOT EXISTS, and a second
	// run reports a duplicate column rather than succeeding quietly.
	if !hasColumn(db, "job_tokens", "lookup_hash") {
		if _, err := db.Exec(`ALTER TABLE job_tokens ADD COLUMN lookup_hash TEXT`); err != nil {
			return fmt.Errorf("failed to add lookup_hash column: %w", err)
		}
	}
	if _, err := db.Exec(`CREATE INDEX IF NOT EXISTS idx_lookup_hash ON job_tokens(lookup_hash)`); err != nil {
		return fmt.Errorf("failed to index lookup_hash: %w", err)
	}

	return nil
}

// hasColumn reports whether a table already has a column.
func hasColumn(db *sql.DB, table, column string) bool {
	rows, err := db.Query(fmt.Sprintf("PRAGMA table_info(%s)", table))
	if err != nil {
		return false
	}
	defer func() { _ = rows.Close() }()
	for rows.Next() {
		var (
			cid         int
			name, ctype string
			notNull, pk int
			dflt        sql.NullString
		)
		if err := rows.Scan(&cid, &name, &ctype, &notNull, &dflt, &pk); err != nil {
			return false
		}
		if name == column {
			return true
		}
	}
	return false
}

// bcryptComparisons counts token verifications. It exists so a test can assert
// that validation examines one candidate row rather than every live token --
// the property that was broken, and one that a timing assertion could only
// guess at.
var bcryptComparisons atomic.Int64

// compareToken verifies a token against a stored bcrypt hash.
func compareToken(hashedToken, token string) error {
	bcryptComparisons.Add(1)
	return bcrypt.CompareHashAndPassword([]byte(hashedToken), []byte(token))
}

// tokenLookupHash is the indexed handle on a token: a plain SHA-256 of it, so a
// validation can find the one candidate row instead of comparing against every
// live token.
//
// A fast hash is the right primitive for *finding* the row -- the token is 32
// bytes from crypto/rand, so there is nothing to brute-force -- and it is not
// what authorizes the request. The bcrypt hash still is: the row this locates
// is then verified the same way it always was. That keeps the change to how a
// token is found, not to what makes it valid.
func tokenLookupHash(token string) string {
	sum := sha256.Sum256([]byte(token))
	return hex.EncodeToString(sum[:])
}

func (d *DB) RegisterJob(jobID, jobAdJSON, owner string, uid, gid int) (string, int64, error) {
	tx, err := d.db.Begin()
	if err != nil {
		return "", 0, fmt.Errorf("failed to begin transaction: %w", err)
	}
	defer func() { _ = tx.Rollback() }() // no-op once Commit has run

	var registrationID int64
	err = tx.QueryRow(`
		INSERT INTO job_registrations (job_id, job_ad_json, owner, owner_uid, owner_gid, created_at, updated_at)
		VALUES (?, ?, ?, ?, ?, ?, ?)
		RETURNING id
	`, jobID, jobAdJSON, owner, uid, gid, time.Now(), time.Now()).Scan(&registrationID)
	if err != nil {
		return "", 0, fmt.Errorf("failed to insert job registration: %w", err)
	}

	token, expiresAt, err := d.createTokenForRegistration(tx, registrationID)
	if err != nil {
		return "", 0, err
	}

	if err := tx.Commit(); err != nil {
		return "", 0, fmt.Errorf("failed to commit transaction: %w", err)
	}

	return token, expiresAt.Unix(), nil
}

func (d *DB) createTokenForRegistration(tx *sql.Tx, registrationID int64) (string, time.Time, error) {
	rawToken := make([]byte, tokenLength)
	if _, err := rand.Read(rawToken); err != nil {
		return "", time.Time{}, fmt.Errorf("failed to generate random token: %w", err)
	}
	token := tokenPrefix + base64.URLEncoding.EncodeToString(rawToken)

	// bcrypt handles salting internally, so we store an empty salt
	hashedToken, err := bcrypt.GenerateFromPassword([]byte(token), bcrypt.DefaultCost)
	if err != nil {
		return "", time.Time{}, fmt.Errorf("failed to hash token: %w", err)
	}

	expiresAt := time.Now().Add(tokenExpiration)

	_, err = tx.Exec(`
		INSERT INTO job_tokens (job_registration_id, hashed_token, lookup_hash, expires_at, created_at)
		VALUES (?, ?, ?, ?, ?)
	`, registrationID, string(hashedToken), tokenLookupHash(token), expiresAt, time.Now())
	if err != nil {
		return "", time.Time{}, fmt.Errorf("failed to insert token: %w", err)
	}

	return token, expiresAt, nil
}

// ValidateToken resolves a bearer token to the job it was issued for.
//
// This used to scan: it read every unexpired token and ran a bcrypt comparison
// against each one until one matched. bcrypt is deliberately slow, so the cost
// was the token count times ~60ms -- 12 seconds at 200 live tokens, measured,
// and perfectly linear. Every sandbox GET and PUT paid it, which on a busy
// access point is a denial of service the daemon inflicts on itself.
//
// Now the token's SHA-256 finds the row (indexed), and bcrypt verifies that one
// row. Constant time in the number of live tokens, and a token that matches
// nothing costs no bcrypt at all.
func (d *DB) ValidateToken(token string) (string, int, int, string, error) {
	var (
		jobID, jobAdJSON, hashedToken string
		uid, gid                      int
	)
	err := d.db.QueryRow(`
		SELECT jr.job_id, jr.owner_uid, jr.owner_gid, jr.job_ad_json, jt.hashed_token
		FROM job_tokens jt
		JOIN job_registrations jr ON jt.job_registration_id = jr.id
		WHERE jt.lookup_hash = ? AND jt.expires_at > ?
	`, tokenLookupHash(token), time.Now()).Scan(&jobID, &uid, &gid, &jobAdJSON, &hashedToken)
	switch {
	case err == sql.ErrNoRows:
		// Either no such token, or one written before lookup_hash existed. The
		// legacy scan is the only way to check the latter; it costs what it
		// always did, and shrinks to nothing as those tokens expire.
		return d.validateLegacyToken(token)
	case err != nil:
		return "", 0, 0, "", fmt.Errorf("failed to query token: %w", err)
	}

	// The row was found by a fast hash; bcrypt is still what authorizes it.
	if err := compareToken(hashedToken, token); err != nil {
		return "", 0, 0, "", fmt.Errorf("invalid or expired token")
	}
	return jobID, uid, gid, jobAdJSON, nil
}

// validateLegacyToken handles tokens issued before lookup_hash was added, by
// scanning the rows that have none. Tokens live 24 hours, so this stops finding
// anything a day after an upgrade.
func (d *DB) validateLegacyToken(token string) (string, int, int, string, error) {
	rows, err := d.db.Query(`
		SELECT jr.job_id, jr.owner_uid, jr.owner_gid, jr.job_ad_json, jt.hashed_token
		FROM job_tokens jt
		JOIN job_registrations jr ON jt.job_registration_id = jr.id
		WHERE jt.lookup_hash IS NULL AND jt.expires_at > ?
	`, time.Now())
	if err != nil {
		return "", 0, 0, "", fmt.Errorf("failed to query tokens: %w", err)
	}
	defer func() { _ = rows.Close() }()

	for rows.Next() {
		var jobID, hashedToken, jobAdJSON string
		var uid, gid int
		if err := rows.Scan(&jobID, &uid, &gid, &jobAdJSON, &hashedToken); err != nil {
			return "", 0, 0, "", fmt.Errorf("failed to scan token: %w", err)
		}
		if err := compareToken(hashedToken, token); err == nil {
			return jobID, uid, gid, jobAdJSON, nil
		}
	}
	if err := rows.Err(); err != nil {
		return "", 0, 0, "", fmt.Errorf("failed to read tokens: %w", err)
	}

	return "", 0, 0, "", fmt.Errorf("invalid or expired token")
}

func (d *DB) ValidateUIDAccess(jobID string, uid, gid int) (string, error) {
	var ownerUID, ownerGID int
	var jobAdJSON string
	err := d.db.QueryRow(`
		SELECT owner_uid, owner_gid, job_ad_json
		FROM job_registrations
		WHERE job_id = ?
		ORDER BY created_at DESC
		LIMIT 1
	`, jobID).Scan(&ownerUID, &ownerGID, &jobAdJSON)
	if err != nil {
		if err == sql.ErrNoRows {
			return "", fmt.Errorf("job not found")
		}
		return "", fmt.Errorf("failed to query job: %w", err)
	}

	if uid != ownerUID {
		return "", fmt.Errorf("UID mismatch: expected %d, got %d", ownerUID, uid)
	}

	return jobAdJSON, nil
}

func (d *DB) CleanupExpiredTokens() error {
	_, err := d.db.Exec(`
		DELETE FROM job_tokens WHERE expires_at < ?
	`, time.Now().Add(-7*24*time.Hour))
	if err != nil {
		return fmt.Errorf("failed to cleanup expired tokens: %w", err)
	}
	return nil
}

func (d *DB) CleanupExpiredJobs() error {
	// Delete job registrations that are older than 30 days
	_, err := d.db.Exec(`
		DELETE FROM job_registrations WHERE created_at < datetime('now', '-30 days')
	`)
	if err != nil {
		return fmt.Errorf("failed to cleanup expired jobs: %w", err)
	}
	return nil
}
