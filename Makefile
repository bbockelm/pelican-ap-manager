BINARY=pelican_man
PKG=github.com/bbockelm/pelican-ap-manager

.PHONY: build build-condor clean

build:
	GOFLAGS= go build -o $(BINARY) ./cmd/pelican_man

build-condor:
	GOFLAGS= go build -tags condor -o $(BINARY) ./cmd/pelican_man

clean:
	rm -f $(BINARY)
