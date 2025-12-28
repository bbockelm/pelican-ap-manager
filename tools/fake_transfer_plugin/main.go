package main

import (
	"bufio"
	"fmt"
	"net/url"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"time"
)

func main() {
	if len(os.Args) == 2 && os.Args[1] == "-classad" {
		printPluginClassAd()
		return
	}

	if hasArg(os.Args, "-infile") {
		exitCode, err := handleClassAdMode()
		if err != nil {
			fmt.Fprintln(os.Stderr, err)
		}
		if exitCode == 0 {
			os.Exit(0)
		}
		os.Exit(exitCode)
	}

	if len(os.Args) < 3 {
		fmt.Fprintln(os.Stderr, "usage: fake_transfer_plugin <src> <dst>")
		os.Exit(1)
	}

	src := os.Args[1]
	dst := os.Args[2]

	adPath := os.Getenv("_CONDOR_JOB_AD")
	ad := parseJobAd(adPath)

	isInput := strings.Contains(src, "://")
	direction := "output"
	resultFileAttr := "FakeOutputResultFile"
	statusAttr := "FakeOutputResultStatus"
	transferURL := dst
	localPath := src

	if isInput {
		direction = "input"
		resultFileAttr = "FakeInputResultFile"
		statusAttr = "FakeInputResultStatus"
		transferURL = src
		localPath = dst
	}

	status, ok := ad.intValueWithPresence(statusAttr)
	if !ok {
		appendLog(fmt.Sprintf("pid=%d direction=%s missing_attr=%s ad=%s time=%s\n", os.Getpid(), direction, statusAttr, adPath, time.Now().Format(time.RFC3339)))
	}
	resultPath := ad.stringValue(resultFileAttr)
	logLine := fmt.Sprintf("pid=%d direction=%s src=%s dst=%s status=%d ad=%s time=%s\n", os.Getpid(), direction, src, dst, status, adPath, time.Now().Format(time.RFC3339))
	appendLog(logLine)

	success := status == 0
	diag := fmt.Sprintf("fake transfer plugin direction=%s src=%s dst=%s status=%d time=%s", direction, src, dst, status, time.Now().Format(time.RFC3339))
	// For inputs, always materialize the payload so failures still leave diagnostics on disk.
	writePayload := isInput
	start := time.Now()
	end := start
	size := int64(0)

	if resultPath != "" {
		if err := os.MkdirAll(filepath.Dir(resultPath), 0o755); err == nil {
			_ = os.WriteFile(resultPath, []byte(diag+"\n"), 0o644)
		}
	}

	if writePayload {
		payload := []byte(diag + "\n")
		_ = os.MkdirAll(filepath.Dir(dst), 0o755)
		_ = os.WriteFile(dst, payload, 0o644)
		size = int64(len(payload))
		end = time.Now()
	} else if info, err := os.Stat(localPath); err == nil {
		size = info.Size()
	}

	parsedURL, _ := url.Parse(transferURL)
	tr := transferResult{
		URL:        transferURL,
		LocalPath:  localPath,
		Success:    success,
		Diagnostic: diag,
		isInput:    isInput,
		start:      start,
		end:        end,
		size:       size,
		protocol:   parsedURL.Scheme,
	}

	fmt.Println(formatTransferResult(tr))

	if !isInput {
		sidecar := src + ".fake_plugin_result"
		_ = os.WriteFile(sidecar, []byte(diag+"\n"), 0o644)
	}

	if success {
		os.Exit(0)
	}

	exitCode := status
	if exitCode == 0 {
		exitCode = 1
	}
	os.Exit(exitCode)
}

type transferResult struct {
	URL          string
	LocalPath    string
	Success      bool
	Diagnostic   string
	isInput      bool
	writePayload bool
	content      []byte
	start        time.Time
	end          time.Time
	size         int64
	protocol     string
}

func handleClassAdMode() (int, error) {
	args := parseArgs(os.Args)
	inFile := args["-infile"]
	outFile := args["-outfile"]
	if inFile == "" || outFile == "" {
		return 1, fmt.Errorf("missing -infile or -outfile")
	}

	ad := parseJobAd(os.Getenv("_CONDOR_JOB_AD"))
	requests, err := parseTransferRequests(inFile)
	if err != nil {
		return 1, err
	}

	var results []transferResult
	hasFailure := false
	maxStatus := 0
	for _, req := range requests {
		isInput := strings.Contains(req.URL, "/input/")
		direction := "output"
		statusAttr := "FakeOutputResultStatus"
		if isInput {
			direction = "input"
			statusAttr = "FakeInputResultStatus"
		}

		status, ok := ad.intValueWithPresence(statusAttr)
		if !ok {
			appendLog(fmt.Sprintf("pid=%d direction=%s missing_attr=%s ad=%s time=%s\n", os.Getpid(), direction, statusAttr, os.Getenv("_CONDOR_JOB_AD"), time.Now().Format(time.RFC3339)))
		}
		if status > maxStatus {
			maxStatus = status
		}

		success := status == 0
		if !success {
			hasFailure = true
		}

		diag := fmt.Sprintf("fake transfer plugin direction=%s url=%s status=%d", direction, req.URL, status)
		payload := []byte(diag + "\n")
		// For inputs, always write payloads (even on failure) so diagnostics land on disk.
		writePayload := isInput
		start := time.Now()
		end := start
		size := int64(0)
		if writePayload {
			size = int64(len(payload))
		}
		parsedURL, _ := url.Parse(req.URL)

		results = append(results, transferResult{
			URL:          req.URL,
			LocalPath:    req.LocalPath,
			Success:      success,
			Diagnostic:   diag,
			isInput:      isInput,
			writePayload: writePayload,
			content:      payload,
			start:        start,
			end:          end,
			size:         size,
			protocol:     parsedURL.Scheme,
		})
	}

	if err := materializePayloads(results); err != nil {
		return 1, err
	}

	if err := writePluginOut(outFile, results); err != nil {
		return 1, err
	}

	if maxStatus == 0 && hasFailure {
		maxStatus = 1
	}
	return maxStatus, nil
}

func parseArgs(args []string) map[string]string {
	out := make(map[string]string)
	for i := 0; i < len(args)-1; i++ {
		if strings.HasPrefix(args[i], "-") {
			out[args[i]] = args[i+1]
		}
	}
	return out
}

func hasArg(args []string, flag string) bool {
	for _, a := range args {
		if a == flag {
			return true
		}
	}
	return false
}

type transferRequest struct {
	URL       string
	LocalPath string
}

func parseTransferRequests(path string) ([]transferRequest, error) {
	f, err := os.Open(path)
	if err != nil {
		return nil, err
	}
	defer func() { _ = f.Close() }()

	scanner := bufio.NewScanner(f)
	scanner.Buffer(make([]byte, 0, 64*1024), 1024*1024)

	var buf strings.Builder
	var reqs []transferRequest
	for scanner.Scan() {
		line := strings.TrimSpace(scanner.Text())
		if line == "" {
			continue
		}
		buf.WriteString(line)
		if strings.Contains(line, "]") {
			adText := buf.String()
			buf.Reset()
			adText = strings.TrimPrefix(adText, "[")
			adText = strings.TrimSuffix(adText, "]")
			ad := parseAdFields(adText)
			url := ad["Url"]
			local := ad["LocalFileName"]
			if url == "" || local == "" {
				continue
			}
			reqs = append(reqs, transferRequest{URL: url, LocalPath: local})
		}
	}
	if err := scanner.Err(); err != nil {
		return nil, err
	}
	return reqs, nil
}

func parseAdFields(body string) map[string]string {
	out := make(map[string]string)
	parts := strings.Split(body, ";")
	for _, p := range parts {
		p = strings.TrimSpace(p)
		if p == "" {
			continue
		}
		key, val, ok := strings.Cut(p, "=")
		if !ok {
			continue
		}
		key = strings.TrimSpace(key)
		val = strings.TrimSpace(val)
		val = strings.Trim(val, "\"")
		out[key] = val
	}
	return out
}

func materializePayloads(results []transferResult) error {
	for i := range results {
		r := &results[i]
		if r.writePayload {
			if err := os.MkdirAll(filepath.Dir(r.LocalPath), 0o755); err != nil {
				return err
			}
			if err := os.WriteFile(r.LocalPath, r.content, 0o644); err != nil {
				return err
			}
			r.size = int64(len(r.content))
			r.end = time.Now()
			continue
		}

		if r.size == 0 {
			if info, err := os.Stat(r.LocalPath); err == nil {
				r.size = info.Size()
				r.end = time.Now()
			}
		}
	}
	return nil
}

func writePluginOut(path string, results []transferResult) error {
	f, err := os.OpenFile(path, os.O_CREATE|os.O_WRONLY|os.O_TRUNC, 0o644)
	if err != nil {
		return err
	}
	defer func() { _ = f.Close() }()

	for _, r := range results {
		if _, err := fmt.Fprintln(f, formatTransferResult(r)); err != nil {
			return err
		}
	}
	return nil
}

func formatTransferResult(r transferResult) string {
	transferType := "upload"
	if r.isInput {
		transferType = "download"
	}

	protocol := r.protocol
	endpoint := protocol
	if u, err := url.Parse(r.URL); err == nil {
		if u.Scheme != "" {
			protocol = u.Scheme
		}
		if u.Host != "" {
			endpoint = u.Host
		}
		if endpoint == "" {
			endpoint = protocol
		}
	}

	fileName := filepath.Base(r.LocalPath)
	if fileName == "" {
		if u, err := url.Parse(r.URL); err == nil {
			fileName = filepath.Base(u.Path)
		}
	}

	start := r.start
	if start.IsZero() {
		start = time.Now()
	}
	end := r.end
	if end.IsZero() {
		end = start
	}

	size := r.size
	if size < 0 {
		size = 0
	}

	successStr := "false"
	if r.Success {
		successStr = "true"
	}

	errField := ""
	if !r.Success {
		errField = fmt.Sprintf("TransferError = \"%s\"; ", escapeClassadString(r.Diagnostic))
	}

	return fmt.Sprintf("[ TransferSuccess = %s; TransferUrl = \"%s\"; TransferFileName = \"%s\"; TransferType = \"%s\"; TransferProtocol = \"%s\"; TransferFileBytes = %d; TransferTotalBytes = %d; TransferStartTime = %d; TransferEndTime = %d; %sDeveloperData = [ Attempts = 1; Endpoint0 = \"%s\"; TransferTime0 = %.3f ]; Diagnostic = \"%s\" ]",
		successStr,
		escapeClassadString(r.URL),
		escapeClassadString(fileName),
		transferType,
		protocol,
		size,
		size,
		start.Unix(),
		end.Unix(),
		errField,
		escapeClassadString(endpoint),
		end.Sub(start).Seconds(),
		escapeClassadString(r.Diagnostic),
	)
}

func escapeClassadString(s string) string {
	s = strings.ReplaceAll(s, "\\", "\\\\")
	s = strings.ReplaceAll(s, "\"", "\\\"")
	return s
}

func printPluginClassAd() {
	fmt.Println("MultipleFileSupport = true")
	fmt.Println("PluginVersion = \"0.1\"")
	fmt.Println("PluginType = \"FileTransfer\"")
	fmt.Println("ProtocolVersion = 2")
	fmt.Println("SupportedMethods = \"fake\"")
}

type jobAd map[string]string

func parseJobAd(path string) jobAd {
	out := make(jobAd)
	if path == "" {
		return out
	}

	f, err := os.Open(path)
	if err != nil {
		return out
	}
	defer func() { _ = f.Close() }()

	scanner := bufio.NewScanner(f)
	for scanner.Scan() {
		line := strings.TrimSpace(scanner.Text())
		if line == "" || strings.HasPrefix(line, "#") {
			continue
		}
		key, val, ok := strings.Cut(line, "=")
		if !ok {
			continue
		}
		key = strings.TrimSpace(strings.TrimPrefix(key, "+"))
		val = strings.TrimSpace(val)
		val = strings.Trim(val, "\"")
		out[key] = val
	}
	return out
}

func (a jobAd) stringValue(key string) string {
	return a[key]
}

func (a jobAd) intValue(key string, def int) int {
	raw, ok := a[key]
	if !ok {
		return def
	}
	if raw == "" {
		return def
	}
	v, err := strconv.Atoi(raw)
	if err != nil {
		return def
	}
	return v
}

func (a jobAd) intValueWithPresence(key string) (int, bool) {
	raw, ok := a[key]
	if !ok {
		return 0, false
	}
	if raw == "" {
		return 0, false
	}
	v, err := strconv.Atoi(raw)
	if err != nil {
		return 0, false
	}
	return v, true
}

func appendLog(line string) {
	logFile := "/tmp/fake_transfer_plugin.log"
	f, err := os.OpenFile(logFile, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0o644)
	if err != nil {
		return
	}
	defer func() { _ = f.Close() }()
	_, _ = f.WriteString(line)
}
