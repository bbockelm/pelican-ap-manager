package webserver

import (
	"compress/gzip"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"strings"

	"github.com/PelicanPlatform/classad/classad"
	"github.com/bbockelm/golang-htcondor/logging"
	"github.com/bbockelm/golang-htcondor/sandbox"
)

type Handlers struct {
	db            *DB
	logger        *logging.Logger
	socketPath    string
	listenAddress string
}

func NewHandlers(db *DB, logger *logging.Logger, socketPath, listenAddress string) *Handlers {
	return &Handlers{
		db:            db,
		logger:        logger,
		socketPath:    socketPath,
		listenAddress: listenAddress,
	}
}

type RegisterRequest struct {
	ClusterId int    `json:"ClusterId"`
	ProcId    int    `json:"ProcId"`
	Owner     string `json:"Owner"`
	Iwd       string `json:"Iwd"`
	Cmd       string `json:"Cmd"`
}

type RegisterResponse struct {
	Token      string   `json:"token"`
	ExpiresAt  int64    `json:"expires_at"`
	InputURLs  []string `json:"input_urls"`
	OutputURLs []string `json:"output_urls"`
}

func (h *Handlers) HandleRegister(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
		return
	}

	// Get UID/GID from Unix socket if available
	uid, gid := getSocketCredentials(r)

	body, err := io.ReadAll(r.Body)
	if err != nil {
		h.logger.Errorf(logging.DestinationGeneral, "Failed to read request body: %v", err)
		http.Error(w, "Failed to read request body", http.StatusBadRequest)
		return
	}

	var req RegisterRequest
	if err := json.Unmarshal(body, &req); err != nil {
		h.logger.Errorf(logging.DestinationGeneral, "Failed to parse registration request: %v", err)
		http.Error(w, "Invalid JSON", http.StatusBadRequest)
		return
	}

	jobID := fmt.Sprintf("%d.%d", req.ClusterId, req.ProcId)

	token, expiresAt, err := h.db.RegisterJob(jobID, string(body), req.Owner, uid, gid)
	if err != nil {
		h.logger.Errorf(logging.DestinationGeneral, "Failed to register job: %v", err)
		http.Error(w, "Failed to register job", http.StatusInternalServerError)
		return
	}

	// Construct URLs based on whether we're on Unix socket or HTTP
	var baseURL string
	if h.socketPath != "" {
		baseURL = fmt.Sprintf("pelican+unix://%s", h.socketPath)
	} else {
		baseURL = fmt.Sprintf("pelican://%s", h.listenAddress)
	}

	inputURL := fmt.Sprintf("%s/sandboxes/%s/input", baseURL, jobID)
	outputURL := fmt.Sprintf("%s/sandboxes/%s/output", baseURL, jobID)

	h.logger.Infof(logging.DestinationGeneral, "Registered job %s with token for UID=%d GID=%d", jobID, uid, gid)

	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(RegisterResponse{
		Token:      token,
		ExpiresAt:  expiresAt,
		InputURLs:  []string{inputURL},
		OutputURLs: []string{outputURL},
	})
}

func (h *Handlers) HandleGetSandbox(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
		return
	}

	jobID := extractJobID(r.URL.Path)
	if jobID == "" {
		http.Error(w, "Invalid job ID", http.StatusBadRequest)
		return
	}

	// Authenticate via bearer token or Unix socket credentials
	jobAdJSON, ok := h.authenticateRequest(w, r, jobID)
	if !ok {
		return
	}

	h.logger.Infof(logging.DestinationGeneral, "Serving input sandbox for job %s", jobID)

	// Parse the job ad from the stored JSON
	jobAd, err := classad.Parse(jobAdJSON)
	if err != nil {
		h.logger.Errorf(logging.DestinationGeneral, "Failed to parse job ad for job %s: %v", jobID, err)
		http.Error(w, "Failed to parse job ad", http.StatusInternalServerError)
		return
	}

	w.Header().Set("Content-Type", "application/x-tar")
	w.Header().Set("Content-Disposition", fmt.Sprintf("attachment; filename=\"sandbox-%s-input.tar.gz\"", jobID))

	gzw := gzip.NewWriter(w)
	defer gzw.Close()

	// Create the input sandbox tarball using the golang-htcondor sandbox API
	if err := sandbox.CreateInputSandboxTar(r.Context(), jobAd, gzw); err != nil {
		h.logger.Errorf(logging.DestinationGeneral, "Failed to create input sandbox for job %s: %v", jobID, err)
		// Can't change status code after writing started, just log the error
		return
	}
}

func (h *Handlers) HandlePutSandbox(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPut {
		http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
		return
	}

	jobID := extractJobID(r.URL.Path)
	if jobID == "" {
		http.Error(w, "Invalid job ID", http.StatusBadRequest)
		return
	}

	// Authenticate via bearer token or Unix socket credentials
	jobAdJSON, ok := h.authenticateRequest(w, r, jobID)
	if !ok {
		return
	}

	// Parse the job ad from the stored JSON
	jobAd, err := classad.Parse(jobAdJSON)
	if err != nil {
		h.logger.Errorf(logging.DestinationGeneral, "Failed to parse job ad for job %s: %v", jobID, err)
		http.Error(w, "Failed to parse job ad", http.StatusInternalServerError)
		return
	}

	// Extract the output sandbox using the golang-htcondor sandbox API
	gzr, err := gzip.NewReader(r.Body)
	if err != nil {
		h.logger.Errorf(logging.DestinationGeneral, "Failed to create gzip reader for job %s: %v", jobID, err)
		http.Error(w, "Failed to read gzip data", http.StatusBadRequest)
		return
	}
	defer gzr.Close()

	if err := sandbox.ExtractOutputSandbox(r.Context(), jobAd, gzr); err != nil {
		h.logger.Errorf(logging.DestinationGeneral, "Failed to extract output sandbox for job %s: %v", jobID, err)
		http.Error(w, "Failed to extract output sandbox", http.StatusInternalServerError)
		return
	}

	h.logger.Infof(logging.DestinationGeneral, "Successfully extracted output sandbox for job %s", jobID)

	w.WriteHeader(http.StatusOK)
	w.Write([]byte("Upload successful"))
}

func (h *Handlers) authenticateRequest(w http.ResponseWriter, r *http.Request, jobID string) (string, bool) {
	// Try bearer token authentication first
	authHeader := r.Header.Get("Authorization")
	if strings.HasPrefix(authHeader, "Bearer ") {
		token := strings.TrimPrefix(authHeader, "Bearer ")
		validatedJobID, _, _, jobAdJSON, err := h.db.ValidateToken(token)
		if err != nil {
			h.logger.Errorf(logging.DestinationGeneral, "Invalid token for job %s: %v", jobID, err)
			http.Error(w, "Unauthorized", http.StatusUnauthorized)
			return "", false
		}

		if validatedJobID != jobID {
			h.logger.Errorf(logging.DestinationGeneral, "Token job ID mismatch: expected %s, got %s", jobID, validatedJobID)
			http.Error(w, "Unauthorized", http.StatusUnauthorized)
			return "", false
		}
		return jobAdJSON, true
	}

	// Try Unix socket UID/GID authentication
	uid, gid := getSocketCredentials(r)
	if uid >= 0 {
		jobAdJSON, err := h.db.ValidateUIDAccess(jobID, uid, gid)
		if err != nil {
			h.logger.Errorf(logging.DestinationGeneral, "UID/GID validation failed for job %s: %v", jobID, err)
			http.Error(w, "Unauthorized", http.StatusUnauthorized)
			return "", false
		}
		return jobAdJSON, true
	}

	http.Error(w, "Missing or invalid authorization", http.StatusUnauthorized)
	return "", false
}

func extractJobID(path string) string {
	// Extract job ID from paths like /sandboxes/123.0/input or /sandboxes/123.0/output
	parts := strings.Split(strings.Trim(path, "/"), "/")
	if len(parts) >= 2 && parts[0] == "sandboxes" {
		return parts[1]
	}
	return ""
}
