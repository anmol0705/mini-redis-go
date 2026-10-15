# Developer tasks. On Windows use Git Bash or WSL for the shell-based targets.
GO ?= go
BIN := bin/miniredis
PORT ?= 6379

.PHONY: build test race lint bench fuzz-short conformance cover clean help

help:
	@echo "targets: build test race lint bench fuzz-short conformance cover clean"

build:
	$(GO) build -o $(BIN) ./cmd/miniredis

test:
	$(GO) vet ./...
	$(GO) test ./...

# The race detector needs CGO and a C compiler (gcc).
race:
	$(GO) test -race -count=1 ./...

lint:
	@test -z "$$(gofmt -l .)" || { echo "gofmt needed on:"; gofmt -l .; exit 1; }
	$(GO) vet ./...
	golangci-lint run ./...

# Go micro-benchmarks. For redis-benchmark against a running server use bench/run.sh.
bench:
	$(GO) test -run '^$$' -bench . -benchmem ./internal/resp ./internal/store ./internal/engine ./internal/skiplist
	$(GO) test -run '^$$' -bench 'Handler|TCP' -benchtime 2s ./internal/server

# Ten seconds on each fuzz target (go test allows one -fuzz target per run).
fuzz-short:
	$(GO) test -run '^$$' -fuzz '^FuzzReadCommand$$' -fuzztime 10s ./internal/resp
	$(GO) test -run '^$$' -fuzz '^FuzzExecute$$' -fuzztime 10s ./internal/command
	$(GO) test -run '^$$' -fuzz '^FuzzReplay$$' -fuzztime 10s ./internal/aof
	$(GO) test -run '^$$' -fuzz '^FuzzDecode$$' -fuzztime 10s ./internal/snapshot
	$(GO) test -run '^$$' -fuzz '^FuzzMatch$$' -fuzztime 10s ./internal/glob

# Starts a server on $(PORT)+1, runs the redis-cli conformance suite, stops it.
conformance: build
	@./$(BIN) -addr 127.0.0.1:$$(($(PORT)+1)) -log-level warn & pid=$$!; \
	sleep 1; conformance/run.sh 127.0.0.1 $$(($(PORT)+1)); rc=$$?; kill $$pid; exit $$rc

cover:
	$(GO) test -coverprofile=coverage.out ./...
	$(GO) tool cover -func=coverage.out | tail -1

clean:
	rm -rf bin coverage.out
