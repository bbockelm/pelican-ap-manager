BINARY=pelican_man
PKG=github.com/bbockelm/pelican-ap-manager

.PHONY: build build-condor clean fetch-job-epochs

build:
	GOFLAGS= go build -o $(BINARY) ./cmd/pelican_man

build-condor:
	GOFLAGS= go build -tags condor -o $(BINARY) ./cmd/pelican_man

clean:
	rm -f $(BINARY)

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
	GOFLAGS= go run -tags condor ./cmd/pelican_man \
		-collector cm-1.ospool.osg-htc.org:9618 \
		-schedd ap40.uw.osg-htc.org \
		-state artifacts/ap40_run/pelican_state.json \
		-job-mirror artifacts/ap40_run/job_mirror.json \
		-advertise-dry-run artifacts/ap40_run/pelican_summary.json \
		-poll 30s \
		-advertise 1m \
		-stats-window 1h
