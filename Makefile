PY := $(if $(wildcard .venv/bin/python),.venv/bin/python,python3)

.PHONY: up down test fmt-check lint verify

up:
	docker compose up --build --wait

down:
	docker compose down -v

test:
	$(PY) -m pytest -q

fmt-check:
	$(PY) -m ruff format --check .
	$(PY) -m ruff check .

lint: fmt-check
	$(PY) -m bandit -q -r controlplane

verify: lint test
	$(PY) -m pip_audit
