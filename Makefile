.PHONY: setup format lint test env check clean

UV_CACHE_DIR ?= .cache/uv
UV := UV_CACHE_DIR=$(UV_CACHE_DIR) uv

setup:
	$(UV) sync --all-groups

format:
	$(UV) run ruff format .

lint:
	$(UV) run ruff check .

test:
	$(UV) run pytest

env:
	$(UV) run market-risk-check

check: lint test env

clean:
	$(UV) cache prune --ci
