SHELL := /bin/bash
.PHONY: help fixtures materialise sources test clean
help: ## This list
	@grep -hE '^[a-z-]+:.*##' $(MAKEFILE_LIST) | sed 's/:.*##/\t/' | expand -t16

fixtures: ## Install the seeded generators published by the pinned release
	uv run --no-project python scripts/fixtures.py

materialise: ## Write the vendor exports to _data/ (needs `make fixtures` first)
	uv run --no-project python scripts/materialise_sources.py

sources: fixtures materialise ## Both, in order -- what a consumer runs once

test: fixtures ## The vendor invariants -- static checks on the specs and scripts
	# --no-sync, and it is load-bearing. A bare `uv run` RE-SYNCS and prunes
	# anything absent from the lock -- which evicts the generators `fixtures`
	# just installed, and two of these tests read them to check the spec
	# against what the vendor actually emits. Without this the suite reports
	# ModuleNotFoundError for a package installed seconds earlier.
	uv run --frozen --no-sync --group dev pytest tests -q

clean: ## Drop the materialised exports
	rm -rf _data
