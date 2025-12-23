.PHONY: help fmt fmt-check clippy lint check build build-release test clean spec-lint wire-lint dev-seed

SHELL := /bin/bash
.SHELLFLAGS := -euo pipefail -c

CARGO ?= cargo
CLIPPY_ARGS ?= -D warnings
CARGO_FEATURES ?= --all-features
CARGO_TARGETS ?= --all-targets

help:
	@echo "quantum make targets"
	@echo "  make build          # debug build of quantum binary + lib ($(CARGO_FEATURES))"
	@echo "  make build-release  # optimized build of quantum binary + lib ($(CARGO_FEATURES))"
	@echo "  make test           # full test suite (--all) $(CARGO_FEATURES) (tests/ only)"
	@echo "  make check          # cargo check $(CARGO_TARGETS) $(CARGO_FEATURES)"
	@echo "  make fmt/fmt-check  # rustfmt (check mode available)"
	@echo "  make clippy|lint    # clippy with warnings as errors"
	@echo "  make spec-lint      # run clustor spec lint"
	@echo "  make wire-lint      # placeholder wire definitions lint"
	@echo "  make dev-seed       # hydrate local data/ using fixtures (TENANT= optional)"
	@echo "  make interop        # run interop harness (ignored tests)"
	@echo "  make package        # stage packaging artifacts under artifacts/"
	@echo "  make clean          # cargo clean"

fmt:
	$(CARGO) fmt --all

fmt-check:
	$(CARGO) fmt --all -- --check

clippy:
	$(CARGO) clippy $(CARGO_TARGETS) $(CARGO_FEATURES) -- $(CLIPPY_ARGS)

lint: fmt-check clippy

check:
	$(CARGO) check $(CARGO_TARGETS) $(CARGO_FEATURES)

build:
	$(CARGO) build $(CARGO_TARGETS) $(CARGO_FEATURES)

build-release:
	$(CARGO) build $(CARGO_TARGETS) $(CARGO_FEATURES) --release

test:
	$(CARGO) test --all $(CARGO_FEATURES)

clean:
	$(CARGO) clean

spec-lint:
	if [[ ! -x ../clustor/target/release/spec_lint ]]; then \
		(cd ../clustor && $(CARGO) build --release --bin spec_lint); \
	fi
	../clustor/target/release/spec_lint --manifest ../clustor/manifests/consensus_core_manifest.json

wire-lint:
	@echo "wire-lint: placeholder (wire definitions not yet implemented)"

dev-seed:
	$(CARGO) run --bin seed_workload_data -- --fixtures data/fixtures/mqtt --data-root data --workload mqtt $(if $(TENANT),--tenant $(TENANT),)

interop:
	$(CARGO) test --test interop -- --ignored

package:
	mkdir -p artifacts
	@echo "package target is a stub; add binary/container steps as needed"
