PYTHON ?= python3

.PHONY: setup-dev lint test smoke-test

setup-dev:
	$(PYTHON) -m pip install -r requirements-dev.txt

lint:
	ruff check .

test:
	pytest

smoke-test:
	pytest tests/test_pipeline_smoke.py
