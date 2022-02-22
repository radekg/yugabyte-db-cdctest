.DEFAULT_GOAL := build

BINARY         = yugabyte-db-cdctest
SOURCES        = $(shell find . -name '*.go' | grep -v /vendor/)
VERSION       ?= $(shell git describe --tags --always --dirty)
GOPKGS         = $(shell go list ./... | grep -v /vendor/)
BUILD_FLAGS   ?=
LDFLAGS       ?= -X github.com/radekg/yugabyte-db-cdctest/config.Version=$(VERSION) -w -s
GOARCH        ?= amd64
GOOS          ?= linux

.PHONY: build 

build: build/$(BINARY)

build/$(BINARY): $(SOURCES)
	GOOS=$(GOOS) GOARCH=$(GOARCH) CGO_ENABLED=0 go build -o build/$(BINARY) $(BUILD_FLAGS) -ldflags "$(LDFLAGS)" .

clean:
	@rm -rf build
