# Pelican Manager Web Server

The pelican_man daemon now includes an integrated web server for managing job sandboxes via HTTP over Unix domain sockets or TCP/TLS. The server uses the golang-htcondor sandbox API for creating and extracting job sandboxes.

## Features

- **Sandbox Creation**: Automatically creates input tarballs from job ClassAds using `github.com/bbockelm/golang-htcondor/sandbox`
- **Sandbox Extraction**: Extracts output tarballs to appropriate filesystem locations
- **Privilege Dropping**: When running as root, automatically drops privileges using droppriv manager for secure file operations
- **Dual Authentication**: Supports both Unix socket UID/GID authentication and bearer token authentication
- **Database Persistence**: SQLite database stores job registrations and authentication tokens

## Configuration

The web server is configured via HTCondor configuration macros:

```
# Option 1: Unix domain socket (recommended for local access with UID/GID authentication)
PELICAN_MANAGER_WEB_SOCKET_PATH = $(SPOOL)/pelican_manager.sock

# Option 2: TCP/HTTP listen address (requires bearer token authentication)
PELICAN_MANAGER_WEB_LISTEN_ADDRESS = :8080

# Optional: TLS certificate and key paths (for TCP mode)
PELICAN_MANAGER_WEB_TLS_CERT = /path/to/cert.pem
PELICAN_MANAGER_WEB_TLS_KEY = /path/to/key.pem

# Optional: Database path (defaults to SPOOL/pelican_web.db)
PELICAN_MANAGER_WEB_DB_PATH = /path/to/pelican_web.db

# Optional: Privilege dropping configuration (automatic when running as root)
DROP_PRIVILEGES = TRUE
CONDOR_USER = condor
# CONDOR_IDS = 123:456  # Override UID:GID
```

If neither `PELICAN_MANAGER_WEB_LISTEN_ADDRESS` nor `PELICAN_MANAGER_WEB_SOCKET_PATH` is set, the web server will not start.

## Privilege Dropping

When pelican_man is started as root:
- Automatically enables `droppriv` manager from golang-htcondor
- Drops effective privileges to the condor user (configurable via `CONDOR_USER`)
- File operations (reading/writing sandboxes) are performed as the job owner
- Web server itself runs with condor privileges for socket/port access

## REST API

### POST /api/v1/sandbox/register

Register a new job and receive authentication credentials and sandbox URLs.

**Request Body** (Job ClassAd in JSON format):
```json
{
  "ClusterId": 12345,
  "ProcId": 0,
  "Owner": "user@domain",
  "Iwd": "/path/to/working/directory",
  "Cmd": "/bin/executable",
  "TransferExecutable": true,
  "TransferInput": "input1.txt,input2.dat",
  "In": "stdin.txt"
}
```

**Response:**
```json
{
  "token": "pelican_token_AbCdEfGh1234567890...",
  "expires_at": 1735689599,
  "input_urls": ["pelican+unix:///var/lib/condor/spool/pelican_manager.sock/sandboxes/12345.0/input"],
  "output_urls": ["pelican+unix:///var/lib/condor/spool/pelican_manager.sock/sandboxes/12345.0/output"]
}
```

The token is used as a bearer token for subsequent requests. If using Unix sockets, UID/GID authentication is also supported.

### GET /sandboxes/{JOBID}/input

Download the input sandbox for a job as a gzipped tarball. The tarball is created from the job's ClassAd using `sandbox.CreateInputSandboxTar()`.

**Headers:**
- `Authorization: Bearer pelican_token_AbCdEfGh1234567890...` (required for TCP mode)

**Authentication:**
- Unix socket: Authenticated via UID/GID of connecting process
- TCP: Requires valid bearer token

**Tarball Contents:**
- Executable (if `TransferExecutable` is true)
- Files listed in `TransferInput`
- Standard input file (if `In` is set and not a URL)
- All files are read with proper privilege dropping

**Response:**
- Content-Type: `application/x-tar`
- Status: 200 OK with gzipped tarball
- Status: 401 Unauthorized (invalid or expired token, or UID mismatch)
- Status: 500 Internal Server Error (if sandbox creation fails)

### PUT /sandboxes/{JOBID}/output

Upload the output sandbox for a job as a gzipped tarball. The tarball is extracted using `sandbox.ExtractOutputSandbox()`.

**Headers:**
- `Authorization: Bearer pelican_token_AbCdEfGh1234567890...` (required for TCP mode)
- `Content-Type: application/x-tar`

**Request Body:** Gzipped tarball data

**Authentication:**
- Unix socket: Authenticated via UID/GID of connecting process  
- TCP: Requires valid bearer token

**Extraction Behavior:**
- Files are extracted to `Iwd` (initial working directory)
- `TransferOutput` list filters which files are extracted (if specified)
- `_condor_stdout` is remapped to `Out` path
- `_condor_stderr` is remapped to `Err` path
- Files are written with proper privilege dropping

**Response:**
- Status: 200 OK (success)
- Status: 401 Unauthorized (invalid or expired token, or UID mismatch)
- Status: 400 Bad Request (invalid gzip data)
- Status: 500 Internal Server Error (extraction failed)

## Security

### Token-Based Authentication (TCP mode)
- Tokens are randomly generated with the `pelican_token_` prefix
- Tokens are salted and hashed using SHA-256 before storage
- Tokens expire after 24 hours
- Expired tokens older than 7 days are automatically cleaned up
- Bearer token authentication is required for all sandbox operations over TCP

### UID/GID Authentication (Unix socket mode)
- On Linux: Uses SO_PEERCRED to retrieve peer process credentials
- On macOS: Placeholder implementation (returns -1, -1)
- Job registrations store the owner UID/GID
- Sandbox access validates that the requesting UID matches the job owner UID
- No bearer token required when UID/GID authentication succeeds

### Privilege Management
- droppriv manager handles all filesystem operations
- Files are read/written as the job owner, not the daemon user
- Thread-local privilege transitions ensure isolation
- Root privileges are never used for actual file I/O

## Database

The web server uses SQLite to store:
- Job registrations (job_id, job_ad_json, owner, owner_uid, owner_gid)
- Authentication tokens (hashed_token, salt, expires_at)

The database schema includes indexes for efficient token validation, UID lookups, and cleanup.

The full job ClassAd JSON is stored to enable sandbox operations without additional lookups.

## URL Schemes

- `pelican+unix:///path/to/socket/sandboxes/JOB.ID/input` - Unix socket access
- `pelican://host:port/sandboxes/JOB.ID/input` - HTTP/HTTPS access
- `pelican://localhost:8080/sandboxes/JOB.ID/output` - Example output URL

## Implementation Details

### Sandbox Creation (Input)
Uses `github.com/bbockelm/golang-htcondor/sandbox.CreateInputSandboxTar()`:
- Reads `TransferInput`, `Cmd`, `In` from job ClassAd
- Respects `TransferExecutable` flag
- Skips URLs in file lists
- Drops privileges to job owner for file reads
- Streams tarball directly to HTTP response

### Sandbox Extraction (Output)
Uses `github.com/bbockelm/golang-htcondor/sandbox.ExtractOutputSandbox()`:
- Extracts to `Iwd` directory
- Filters by `TransferOutput` if specified
- Remaps `_condor_stdout`/`_condor_stderr` to `Out`/`Err`
- Drops privileges to job owner for file writes
- Handles subdirectories and permissions

## Future Enhancements

- Support for golang-htcondor/httpserver Handler interface
- Full macOS UID/GID authentication support via LOCAL_PEERCRED
- Streaming support for very large sandboxes
- Compression level configuration
- Rate limiting for API endpoints
