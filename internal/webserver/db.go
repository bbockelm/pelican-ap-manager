package webserver

import (
	"crypto/rand"
	"database/sql"
	"encoding/base64"
	"fmt"
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

	if err := initSchema(db); err != nil {
		db.Close()
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

	return nil
}

func (d *DB) RegisterJob(jobID, jobAdJSON, owner string, uid, gid int) (string, int64, error) {
	tx, err := d.db.Begin()
	if err != nil {
		return "", 0, fmt.Errorf("failed to begin transaction: %w", err)
	}
	defer tx.Rollback()

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
		INSERT INTO job_tokens (job_registration_id, hashed_token, expires_at, created_at)
		VALUES (?, ?, ?, ?)
	`, registrationID, string(hashedToken), expiresAt, time.Now())
	if err != nil {
		return "", time.Time{}, fmt.Errorf("failed to insert token: %w", err)
	}

	return token, expiresAt, nil
}

func (d *DB) ValidateToken(token string) (string, int, int, string, error) {
	rows, err := d.db.Query(`
		SELECT jr.job_id, jr.owner_uid, jr.owner_gid, jr.job_ad_json, jt.hashed_token, jt.expires_at
		FROM job_tokens jt
		JOIN job_registrations jr ON jt.job_registration_id = jr.id
		WHERE jt.expires_at > ?
	`, time.Now())
	if err != nil {
		return "", 0, 0, "", fmt.Errorf("failed to query tokens: %w", err)
	}
	defer rows.Close()

	for rows.Next() {
		var jobID, hashedToken, jobAdJSON string
		var uid, gid int
		var expiresAt time.Time
		if err := rows.Scan(&jobID, &uid, &gid, &jobAdJSON, &hashedToken, &expiresAt); err != nil {
			return "", 0, 0, "", fmt.Errorf("failed to scan token: %w", err)
		}

		if err := bcrypt.CompareHashAndPassword([]byte(hashedToken), []byte(token)); err == nil {
			return jobID, uid, gid, jobAdJSON, nil
		}
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
