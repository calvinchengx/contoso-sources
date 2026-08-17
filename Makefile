SHELL := /bin/bash
.PHONY: help fixtures materialise sources clean
help: ## This list
	@grep -hE '^[a-z-]+:.*##' $(MAKEFILE_LIST) | sed 's/:.*##/\t/' | expand -t16

fixtures: ## Install the seeded generators published by the pinned release
	uv run --no-project python scripts/fixtures.py

materialise: ## Write the vendor exports to _data/ (needs `make fixtures` first)
	uv run --no-project python scripts/materialise_sources.py

sources: fixtures materialise ## Both, in order -- what a consumer runs once

clean: ## Drop the materialised exports
	rm -rf _data
