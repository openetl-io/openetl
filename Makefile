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
	github.com/golangci/golangci-lint/v2/cmd/golangci-lint \
	go.temporal.io/sdk/contrib/tools/workflowcheck \
	github.com/google/gnostic/cmd/protoc-gen-openapi
BIN := $(CURDIR)/bin
CARGO_DENY_VERSION := 0.20.2

# Mounted at the same path in the compose stack's Spark containers.
export OPENETL_SPARK_SHARE ?= $(CURDIR)/.openetl/spark

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

$(BIN)/.cargo-deny-$(CARGO_DENY_VERSION):
	cargo install --locked --quiet --root $(BIN)/cargo cargo-deny@$(CARGO_DENY_VERSION)
	@touch $@

.PHONY: tools
tools: $(BIN)/.tools ## Build the pinned developer tools into bin/

.PHONY: hooks
hooks: ## Check what each commit touches before it's made
	git config core.hooksPath .githooks

.PHONY: dev
dev: deps-up connectors web ## Start compose deps and run openetld dev, with the UI
	cargo build -p openetl-engine
	go run ./cmd/openetld dev --engine $(CURDIR)/target/debug/openetl-engine --connectors $(CURDIR)/bin/connectors --web $(CURDIR)/web/dist

# The UI. pnpm installs web/'s dependencies, protoc-gen-es among them.
web/node_modules/.modules.yaml: web/package.json web/pnpm-lock.yaml
	pnpm --dir web install --frozen-lockfile
	@touch $@

.PHONY: web web-dev
web: web/node_modules/.modules.yaml ## Build the UI into web/dist
	pnpm --dir web build
web-dev: web/node_modules/.modules.yaml ## Run the UI's dev server against the API on localhost:8080
	pnpm --dir web dev

# DEPS names the services to start, e.g. DEPS="postgres s3"; all by default.
.PHONY: deps-up deps-down
deps-up: ## Start local dependencies (Postgres, S3, GCS, Azure, Kafka, Kinesis, Spark, Flink, an OCI registry); TEMPORAL=1 adds Temporal, as does naming it in DEPS
	@mkdir -p $(OPENETL_SPARK_SHARE)
	docker compose -f deploy/compose/dev.yml $(if $(or $(TEMPORAL),$(findstring temporal,$(DEPS))),--profile temporal) up -d --wait $(DEPS) $(if $(and $(TEMPORAL),$(DEPS)),temporal)
deps-down: ## Stop local dependencies
	docker compose -f deploy/compose/dev.yml --profile temporal down

# The kubeconfig is the cluster's own, so the current kubectl context is
# never touched.
KIND_KUBECONFIG ?= $(CURDIR)/.openetl/kind/kubeconfig
.PHONY: kind-up kind-down kind-load
kind-up: ## Start a kind cluster with the Flink operator, Spark's service account, KEDA and Prometheus, for the e2e tests' Kubernetes targets
	KIND_KUBECONFIG=$(KIND_KUBECONFIG) OPENETL_SPARK_SHARE=$(OPENETL_SPARK_SHARE) sh deploy/kind/up.sh
kind-down: ## Delete the kind cluster
	docker rm -f openetl-kind-spark-submit
	kind delete cluster --name openetl --kubeconfig $(KIND_KUBECONFIG)

# The official connectors, built from a checkout of openetl-connectors
# against this repo's SDK. A go.work replace overrides theirs.
CONNECTORS_SRC ?= ../openetl-connectors
.PHONY: connectors connectors-work
connectors-work:
	@test -f $(CONNECTORS_SRC)/go.mod || { echo "clone openetl-connectors into $(CONNECTORS_SRC), or set CONNECTORS_SRC"; exit 1; }
	@mkdir -p $(BIN)
	@printf 'go 1.26.0\n\nuse %s\n\nreplace github.com/openetl-io/openetl/sdk/go => %s\n' "$(abspath $(CONNECTORS_SRC))" "$(CURDIR)/sdk/go" > $(BIN)/connectors.work
connectors: connectors-work ## Build the official connectors from CONNECTORS_SRC into bin/connectors
	@mkdir -p $(BIN)/connectors
	cd $(CONNECTORS_SRC) && GOWORK=$(BIN)/connectors.work go build -o $(BIN)/connectors/ ./cmd/...

# openetld's image, from Linux builds of openetld, the CLI, the engine and the
# official connectors, and the console. ENGINE is a Linux openetl-engine:
# make engine-linux's, else on Linux the debug build.
IMAGE ?= openetl:dev
IMAGE_ARCH ?= $(shell go env GOARCH)
ENGINE ?= $(or $(wildcard $(BIN)/linux/$(IMAGE_ARCH)/openetl-engine),$(CURDIR)/target/debug/openetl-engine)
.PHONY: image
image: web connectors-work ## Build openetld's image (IMAGE), with ENGINE, a Linux openetl-engine
	@file -b $(ENGINE) | grep -q ELF || { echo "ENGINE=$(ENGINE) isn't a Linux openetl-engine; make engine-linux builds one"; exit 1; }
	rm -rf $(BIN)/image && mkdir -p $(BIN)/image/connectors
	GOOS=linux GOARCH=$(IMAGE_ARCH) CGO_ENABLED=0 go build -o $(BIN)/image/openetld ./cmd/openetld
	GOOS=linux GOARCH=$(IMAGE_ARCH) CGO_ENABLED=0 go build -o $(BIN)/image/openetl ./cmd/openetl
	cd $(CONNECTORS_SRC) && GOOS=linux GOARCH=$(IMAGE_ARCH) CGO_ENABLED=0 GOWORK=$(BIN)/connectors.work go build -o $(BIN)/image/connectors/ ./cmd/...
	cp $(ENGINE) $(BIN)/image/openetl-engine
	cp -R web/dist $(BIN)/image/web
	cp LICENSE NOTICE $(BIN)/image/
	docker build --platform linux/$(IMAGE_ARCH) -t $(IMAGE) -f deploy/image/Dockerfile $(BIN)/image
	rm -rf $(BIN)/image

# A release build of openetl-engine for Linux on IMAGE_ARCH, in the pinned
# Rust toolchain's container, so make image works from any host. Its build
# and crates stay in Docker volumes, for the next build.
RUST_VERSION := $(shell sed -n 's/^channel = "\(.*\)"/\1/p' rust-toolchain.toml)
.PHONY: engine-linux
engine-linux: ## Build a Linux openetl-engine for make image, in Docker, into bin/linux/IMAGE_ARCH
	@mkdir -p $(BIN)/linux/$(IMAGE_ARCH)
	docker run --rm --platform linux/$(IMAGE_ARCH) -v $(CURDIR):/src:ro -w /src \
		-v openetl-engine-target:/target -v openetl-cargo-registry:/usr/local/cargo/registry \
		-v $(BIN)/linux/$(IMAGE_ARCH):/out -e CARGO_TARGET_DIR=/target -e RUSTUP_TOOLCHAIN=$(RUST_VERSION) \
		rust:$(RUST_VERSION)-bookworm sh -c 'cargo build --locked --release -p openetl-engine && cp /target/release/openetl-engine /out/'

# kind load fails on Docker's containerd image store, so the node imports
# what docker save writes, for its own platform only.
kind-load: ## Load IMAGE into make kind-up's cluster
	docker save $(IMAGE) | docker exec -i openetl-control-plane ctr --namespace=k8s.io images import --digests --snapshotter=overlayfs -

# openetl-spark and openetl-flink: the jars, built and tested in a
# pinned Maven image, so no JDK is needed on the host; and openetl-spark's
# Python package's tests. Spark's tests need a passwd entry for the
# container's user, for Hadoop's login. MAVEN_FLAGS=-DskipTests builds the
# jars without their tests.
MAVEN_IMAGE := maven:3.9.12-eclipse-temurin-17
MAVEN_REPO ?= $(HOME)/.m2
.PHONY: spark-lib
spark-lib: ## Build and test openetl-spark (the jar into libs/openetl-spark/java/target)
	@mkdir -p $(MAVEN_REPO) .openetl
	@printf 'root:x:0:0::/root:/bin/sh\nopenetl:x:%s:%s::/var/maven:/bin/sh\n' $$(id -u) $$(id -g) > .openetl/maven-passwd
	docker run --rm -u $$(id -u):$$(id -g) -v $(CURDIR)/.openetl/maven-passwd:/etc/passwd:ro \
		-e MAVEN_CONFIG=/var/maven/.m2 -v $(MAVEN_REPO):/var/maven/.m2 \
		-v $(CURDIR)/libs/openetl-spark/java:/src -w /src $(MAVEN_IMAGE) mvn -B -q -Duser.home=/var/maven $(MAVEN_FLAGS) package
	cd libs/openetl-spark/python && PYTHONPATH=. python3 -m unittest discover -s tests

.PHONY: flink-lib
flink-lib: ## Build and test openetl-flink (the jar into libs/openetl-flink/java/target)
	@mkdir -p $(MAVEN_REPO) .openetl
	@printf 'root:x:0:0::/root:/bin/sh\nopenetl:x:%s:%s::/var/maven:/bin/sh\n' $$(id -u) $$(id -g) > .openetl/maven-passwd
	docker run --rm -u $$(id -u):$$(id -g) -v $(CURDIR)/.openetl/maven-passwd:/etc/passwd:ro \
		-e MAVEN_CONFIG=/var/maven/.m2 -v $(MAVEN_REPO):/var/maven/.m2 -v $(CURDIR)/proto:/proto:ro \
		-v $(CURDIR)/libs/openetl-flink/java:/src -w /src $(MAVEN_IMAGE) mvn -B -q -Duser.home=/var/maven -Dbridge.proto=/proto $(MAVEN_FLAGS) verify

.PHONY: engine
engine: ## Build openetl-engine
	cargo build --release -p openetl-engine

# Clearing first drops code for deleted protos.
.PHONY: generate
generate: tools web/node_modules/.modules.yaml ## Generate code from protos and SQL, and the reference pages from the schemas and settings
	rm -rf gen sdk/go/protocol internal/store/db web/src/gen
	PATH="$(BIN):$$PATH" $(BIN)/buf generate
	PATH="$(BIN):$$PATH" $(BIN)/buf generate --template buf.gen.api.yaml
	$(BIN)/buf generate --template buf.gen.web.yaml
	$(BIN)/sqlc generate -f internal/store/sqlc.yaml
	go run ./internal/refdocs

.PHONY: lint
lint: tools $(BIN)/.cargo-deny-$(CARGO_DENY_VERSION) web/node_modules/.modules.yaml ## Lint protos, Go, Rust and the UI, and check Rust dependencies
	$(BIN)/buf lint
	$(BIN)/buf format --diff --exit-code
	@for m in $(GO_MODULES); do (cd $$m && $(BIN)/golangci-lint run ./...) || exit 1; done
	$(BIN)/workflowcheck ./internal/orchestration/...
	cargo fmt --all --check
	cargo clippy --workspace --all-targets -- -D warnings
	$(BIN)/cargo/bin/cargo-deny check --hide-inclusion-graph
	pnpm --dir web typecheck

# The runner/engine tests in internal/engineclient need OPENETL_ENGINE and
# skip without it.
.PHONY: test
test: tools web/node_modules/.modules.yaml ## Unit tests (fast, no Docker), including runner/engine tests
	cargo build -p openetl-engine
	OPENETL_ENGINE=$(CURDIR)/target/debug/openetl-engine $(BIN)/gotestsum --format pkgname -- $(GO_PACKAGES)
	cd sdk/go && GOWORK=off go build ./...
	cargo test --workspace
	pnpm --dir web test

# Only the packages with integration tests: make test runs the rest. They need
# the compose Postgres, S3, GCS, Azure and Flink.
INTEGRATION_PKGS = $(shell grep -rlE '^//go:build .*integration' --include='*_test.go' cmd internal sdk test | xargs -n1 dirname | sort -u | sed 's|^|./|')
.PHONY: test-integration
test-integration: ## Integration tests against the compose deps
	go test -tags integration -count=1 $(INTEGRATION_PKGS)

.PHONY: sim
sim: ## Commit-protocol simulation; SEEDS=n runs n seeds per sink kind
	go test ./test/sim -run TestSimulation -count=1 -timeout 60m -seeds=$(SEEDS)

# Needs the compose Postgres. BENCH_FLAGS passes flags, e.g. -rows 10000000.
.PHONY: bench-boundary
bench-boundary: engine connectors ## Runner/engine boundary benchmark
	go run ./test/bench/boundary -engine $(CURDIR)/target/release/openetl-engine -connectors $(BIN)/connectors $(BENCH_FLAGS)

# What make e2e and make chaos run: what they build here, or with CANDIDATE=DIR
# a release candidate's files, which deploy/release/candidate.sh fetched and
# unpacked in DIR. Spark's tests also need their own jar, which isn't released.
ifdef CANDIDATE
CANDIDATE_DIR := $(abspath $(CANDIDATE))
SUITE_BUILD := candidate-spark-tests
SUITE_ENV := OPENETL_OPENETLD=$(CANDIDATE_DIR)/openetld OPENETL_ENGINE=$(CANDIDATE_DIR)/openetl-engine OPENETL_CONNECTORS=$(CANDIDATE_DIR)/connectors \
	OPENETL_SPARK_LIB=$(CANDIDATE_DIR)/lib OPENETL_FLINK_LIB=$(CANDIDATE_DIR)/lib
else
SUITE_BUILD := suite-build
SUITE_ENV := OPENETL_ENGINE=$(CURDIR)/target/debug/openetl-engine OPENETL_CONNECTORS=$(BIN)/connectors \
	OPENETL_SPARK_LIB=$(CURDIR)/libs/openetl-spark/java/target OPENETL_FLINK_LIB=$(CURDIR)/libs/openetl-flink/java/target
endif
.PHONY: suite-build candidate-spark-tests
suite-build: connectors
	cargo build -p openetl-engine
candidate-spark-tests: MAVEN_FLAGS = -DskipTests
candidate-spark-tests: spark-lib
	cp libs/openetl-spark/java/target/openetl-spark-*-tests.jar $(CANDIDATE_DIR)/lib/

# E2E_FLAGS passes go test's flags, e.g. E2E_FLAGS=-v for each test's time.
E2E_TIMEOUT ?= 25m
.PHONY: e2e
e2e: $(SUITE_BUILD) ## Full-stack scenarios against the compose deps; CANDIDATE=DIR runs a release candidate's binaries
	$(SUITE_ENV) OPENETL_SPARK_SUBMIT=$(CURDIR)/deploy/compose/spark-submit \
	go test -tags e2e -count=1 -timeout $(E2E_TIMEOUT) $(E2E_FLAGS) ./test/e2e/...

# The Kubernetes targets' tests, on make kind-up's cluster and the compose
# Postgres and S3. KIND_HOST is how pods reach this host. KIND_IMAGE is
# openetld's image in the cluster (make image kind-load), for attempts' pods;
# without it, their test skips.
.PHONY: e2e-kube
e2e-kube: connectors ## Flink on the Kubernetes operator, PySpark on Spark on Kubernetes, attempts in pods, and deploy/kubernetes with KEDA, in kind
	cargo build -p openetl-engine
	OPENETL_ENGINE=$(CURDIR)/target/debug/openetl-engine OPENETL_CONNECTORS=$(BIN)/connectors \
	OPENETL_FLINK_LIB=$(CURDIR)/libs/openetl-flink/java/target \
	OPENETL_E2E_KUBECONFIG=$(KIND_KUBECONFIG) OPENETL_E2E_KUBE_HOST=$(KIND_HOST) OPENETL_E2E_KUBE_IMAGE=$(KIND_IMAGE) \
	OPENETL_SPARK_SHARE=$(OPENETL_SPARK_SHARE) OPENETL_E2E_KUBE_SPARK_SUBMIT=$(CURDIR)/deploy/kind/spark-submit \
	go test -tags e2e -count=1 -timeout 60m -run 'OnKubernetes' -v ./test/e2e

# The chaos suite needs the compose Postgres, S3, Kafka and Iceberg
# catalog, and for its transforms Spark and Flink with make spark-lib and
# flink-lib's jars; runs drawn with a transform that isn't there go without.
# CHAOS_FLAGS passes its flags, e.g. CHAOS_FLAGS='-chaos.runs=200 -chaos.parallel=6'.
CHAOS_TIMEOUT ?= 3h
.PHONY: chaos
chaos: $(SUITE_BUILD) ## Randomized pipelines under crashes, partitions and store faults; CANDIDATE=DIR runs a release candidate's binaries
	$(SUITE_ENV) OPENETL_SPARK_SUBMIT=$(CURDIR)/deploy/compose/spark-submit \
	go test -tags 'e2e chaos' -count=1 -timeout $(CHAOS_TIMEOUT) -run TestChaos -v ./test/e2e $(CHAOS_FLAGS)

.PHONY: clean
clean: ## Remove build outputs
	rm -rf $(BIN) target web/dist web/node_modules
