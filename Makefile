SHELL := /bin/bash
.PHONY: help materialise clean
help: ## This list
	@grep -hE '^[a-z-]+:.*##' $(MAKEFILE_LIST) | sed 's/:.*##/\t/' | expand -t16

materialise: ## Write the vendor exports to _data/ from the pinned generators
	uv run --extra materialise python scripts/materialise_sources.py

clean: ## Drop the materialised exports
	rm -rf _data
