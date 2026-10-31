SHELL := /bin/bash

# Developer tools are pinned in tools/go.mod and built into bin/. Building them
# with GOWORK=off keeps their dependencies out of the workspace; running the
# binaries without it lets golangci-lint and gotestsum load the workspace.
TOOL_PKGS := \
	github.com/bufbuild/buf/cmd/buf \
	google.golang.org/protobuf/cmd/protoc-gen-go \
	google.golang.org/grpc/cmd/protoc-gen-go-grpc \
	connectrpc.com/connect/cmd/protoc-gen-connect-go \
	github.com/sqlc-dev/sqlc/cmd/sqlc \
	github.com/pressly/goose/v3/cmd/goose \
	gotest.tools/gotestsum \
	github.com/golangci/golangci-lint/v2/cmd/golangci-lint
BIN := $(CURDIR)/bin

GO_MODULES := . sdk/go
GO_PACKAGES := ./... ./sdk/go/...
SEEDS ?= 500

.DEFAULT_GOAL := help

.PHONY: help
help: ## Show this help
	@grep -hE '^[a-z-]+:.*## ' $(MAKEFILE_LIST) | awk -F':.*## ' '{printf "  %-18s %s\n", $$1, $$2}'

$(BIN)/.tools: tools/go.mod tools/go.sum
	GOWORK=off go build -modfile=tools/go.mod -o $(BIN)/ $(TOOL_PKGS)
	@touch $@

.PHONY: tools
tools: $(BIN)/.tools ## Build the pinned developer tools into bin/

.PHONY: dev
dev: deps-up ## Start compose deps and run openetld dev
	cargo build -p openetl-engine
	go run ./cmd/openetld dev --engine $(CURDIR)/target/debug/openetl-engine

.PHONY: deps-up deps-down
deps-up: ## Start local dependencies (Postgres, Temporal, S3)
	docker compose -f deploy/compose/dev.yml up -d --wait
deps-down: ## Stop local dependencies
	docker compose -f deploy/compose/dev.yml down

.PHONY: engine
engine: ## Build openetl-engine
	cargo build --release -p openetl-engine

# Clearing first drops code for deleted protos.
.PHONY: generate
generate: tools ## Generate code from protos and SQL
	rm -rf gen sdk/go/protocol internal/store/db
	PATH="$(BIN):$$PATH" $(BIN)/buf generate
	PATH="$(BIN):$$PATH" $(BIN)/buf generate --template buf.gen.api.yaml
	$(BIN)/sqlc generate -f internal/store/sqlc.yaml

.PHONY: lint
lint: tools ## Lint protos, Go and Rust
	$(BIN)/buf lint
	$(BIN)/buf format --diff --exit-code
	@for m in $(GO_MODULES); do (cd $$m && $(BIN)/golangci-lint run ./...) || exit 1; done
	cargo fmt --all --check
	cargo clippy --workspace --all-targets -- -D warnings

# The runner/engine tests in internal/engineclient need OPENETL_ENGINE and
# skip without it.
.PHONY: test
test: tools ## Unit tests (fast, no Docker), including runner/engine tests
	cargo build -p openetl-engine
	OPENETL_ENGINE=$(CURDIR)/target/debug/openetl-engine $(BIN)/gotestsum --format pkgname -- $(GO_PACKAGES)
	cd sdk/go && GOWORK=off go build ./...
	cargo test --workspace

.PHONY: test-integration
test-integration: ## Integration tests against the compose deps
	go test -tags integration ./...

.PHONY: sim
sim: ## Commit-protocol simulation; SEEDS=n runs n seeds per sink kind
	go test ./test/sim -run TestSimulation -count=1 -timeout 60m -seeds=$(SEEDS)

.PHONY: bench-boundary
bench-boundary: ## Runner/engine boundary benchmark
	@echo "not built yet (tracker item P0.10)"; exit 1

.PHONY: e2e
e2e: ## Full-stack scenarios against the compose deps
	cargo build -p openetl-engine
	OPENETL_ENGINE=$(CURDIR)/target/debug/openetl-engine go test -tags e2e -count=1 -timeout 15m ./test/e2e/...

.PHONY: clean
clean: ## Remove build outputs
	rm -rf $(BIN) target
