# pelican-ap-manager build.
#
# Two binaries: pelican_man (the polling/control daemon) and pelican_web (the
# HTTP surface -- sandbox API plus the golang-htcondor REST API). They share the
# module and most of their configuration; pelican_web exists so the serving work
# can be run, restarted and sized independently of the control loop.
#
# `go build` is itself incremental (build cache), so the phony targets just
# invoke it.

BIN_DIR ?= bin
PKG     := github.com/bbockelm/pelican-ap-manager

# Version stamped into both binaries' -version flag (main.version); a plain
# `go build` without this leaves it "dev".
VERSION ?= $(shell git describe --tags --always --dirty 2>/dev/null || echo dev)
LDFLAGS := -X main.version=$(VERSION)

# The sibling modules (classad, cedar, golang-htcondor, htcondordb) are private
# and resolved directly from GitHub rather than through the module proxy, so a
# just-pushed dependency tag is usable immediately. GOFLAGS is cleared because
# the ambient environment often carries a -mod=vendor/-mod=readonly that would
# fight `go mod tidy`.
GOENV := GOFLAGS= GOPRIVATE=github.com/bbockelm,github.com/PelicanPlatform
GO    ?= go

.PHONY: all build manager web build-condor version test test-integration vet fmt tidy clean \
        check-action-pins fetch-job-epochs

all: build

build: manager web ## Build both binaries into $(BIN_DIR)

manager: ## Build the pelican_man polling/control daemon
	$(GOENV) $(GO) build -ldflags '$(LDFLAGS)' -o $(BIN_DIR)/pelican_man ./cmd/pelican_man

web: ## Build the pelican_web HTTP daemon
	$(GOENV) $(GO) build -ldflags '$(LDFLAGS)' -o $(BIN_DIR)/pelican_web ./cmd/pelican_web

build-condor: ## Build both binaries with the `condor` build tag
	$(GOENV) $(GO) build -tags condor -ldflags '$(LDFLAGS)' -o $(BIN_DIR)/pelican_man ./cmd/pelican_man
	$(GOENV) $(GO) build -tags condor -ldflags '$(LDFLAGS)' -o $(BIN_DIR)/pelican_web ./cmd/pelican_web

version: ## Print the version that would be stamped
	@echo $(VERSION)

test: ## Run the unit test suite
	$(GOENV) $(GO) test ./...

test-integration: ## Run the tests that need a local HTCondor (condor_master on PATH)
	$(GOENV) $(GO) test -tags integration -timeout 30m ./integration/...

vet: ## Static checks, including the build-tagged sources
	$(GOENV) $(GO) vet ./...
	$(GOENV) $(GO) vet -tags integration,condor ./...

fmt: ## Rewrite sources with gofmt
	$(GOENV) gofmt -w $$($(GO) list -f '{{.Dir}}' ./...)

tidy: ## Reconcile go.mod / go.sum
	$(GOENV) $(GO) mod tidy

check-action-pins: ## Verify every GitHub Actions reference is pinned to a commit SHA
	./scripts/check-action-pins.sh

clean: ## Remove built binaries
	rm -rf $(BIN_DIR) pelican_man pelican_web

fetch-job-epochs:
	GOFLAGS= go run -tags condor ./tools/collect_job_epochs \
		$(if $(COLLECTOR_HOST),-collector $(COLLECTOR_HOST),) \
		$(if $(SCHEDD_NAME),-schedd $(SCHEDD_NAME),) \
		$(if $(SITE_ATTR),-site_attr $(SITE_ATTR),) \
		$(if $(FILTER),-filter $(FILTER),) \
		-limit $(or $(LIMIT),100) \
		-output internal/condor/testdata/sample_job_epochs_bulk.json

.PHONY: fetch-job-epochs-ap40
fetch-job-epochs-ap40:
	COLLECTOR_HOST=cm-1.ospool.osg-htc.org SCHEDD_NAME=ap40.uw.osg-htc.org $(MAKE) fetch-job-epochs

.PHONY: fetch-job-epochs-ap40-sanitized
fetch-job-epochs-ap40-sanitized:
	GOFLAGS= go run -tags condor ./tools/collect_job_epochs \
		-collector cm-1.ospool.osg-htc.org:9618 \
		-schedd ap40.uw.osg-htc.org \
		-limit 100 \
		-sanitize \
		-redact_dict internal/condor/testdata/redaction_dict.json \
		-raw_output internal/condor/testdata/sample_job_epochs.raw.json \
		-output internal/condor/testdata/sample_job_epochs.sanitized.json

.PHONY: fetch-transfers-ap40-sanitized
fetch-transfers-ap40-sanitized:
	GOFLAGS= go run -tags condor ./tools/collect_transfers \
		-collector cm-1.ospool.osg-htc.org:9618 \
		-schedd ap40.uw.osg-htc.org \
		-limit 100 \
		-job_limit 100 \
		-sanitize \
		-redact_dict internal/condor/testdata/redaction_dict.json \
		-raw_output internal/condor/testdata/transfers.raw.json \
		-output internal/condor/testdata/transfers.sanitized.json \
		-raw_job_output internal/condor/testdata/job_epochs_from_transfers.raw.json \
		-job_output internal/condor/testdata/job_epochs_from_transfers.sanitized.json

.PHONY: fetch-ap40-sanitized
fetch-ap40-sanitized: fetch-job-epochs-ap40-sanitized fetch-transfers-ap40-sanitized

.PHONY: regenerate-golden
regenerate-golden:
	GOFLAGS= go run ./tools/regenerate_golden

.PHONY: update-testdata
update-testdata: fetch-ap40-sanitized regenerate-golden

.PHONY: redact-testdata
redact-testdata:
	GOFLAGS= go run ./tools/internal/redact/cmd/resanitize \
		-input internal/condor/testdata/sample_job_epochs_bulk.raw.json \
		-output internal/condor/testdata/sample_job_epochs_bulk.json \
		-dict internal/condor/testdata/redaction_dict.json

.PHONY: run-ap40
run-ap40:
	@mkdir -p artifacts/ap40_run
	@echo "Running manager for ap40.uw.osg-htc.org..."
	@echo "JSON output will be written to artifacts/ap40_run/"
	@echo "Press Ctrl+C to stop"
	_condor_LOG=artifacts/ap40_run/log \
	_condor_PELICAN_MANAGER_STATE_PATH=artifacts/ap40_run/pelican_state.json \
	_condor_PELICAN_MANAGER_JOB_MIRROR_PATH=artifacts/ap40_run/job_mirror.json \
	_condor_PELICAN_MANAGER_POLL_INTERVAL=30s \
	_condor_PELICAN_MANAGER_ADVERTISE_INTERVAL=1m \
	_condor_PELICAN_MANAGER_STATS_WINDOW=1h \
	_condor_PELICAN_MANAGER_INFO_PATH=artifacts/ap40_run/pelican_summary.json \
	GOFLAGS= go run -tags condor ./cmd/pelican_man \
		-collector cm-1.ospool.osg-htc.org:9618 \
		-schedd ap40.uw.osg-htc.org
