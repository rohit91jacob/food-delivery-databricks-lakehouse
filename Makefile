# Thin wrappers over uv / databricks / docker. `make help` lists targets.
-include .env
export

TARGET ?= dev
DATE ?= 2026-09-01
DAYS ?= 1
COMPOSE = docker compose -f airflow/docker-compose.yaml

.PHONY: help setup lint fmt test test-unit test-spark test-airflow seed generate local-run upload \
        bundle-validate deploy bundle-env airflow-up airflow-down standalone clean

help:
	@grep -E '^[a-z-]+:.*##' $(MAKEFILE_LIST) | awk -F':.*## ' '{printf "  %-16s %s\n", $$1, $$2}'

setup: ## install all dependency groups (dev, spark, airflow) + extras
	uv sync --group dev --group spark --group airflow --extra seed --extra databricks

lint: ## ruff lint + format check
	uv run ruff check . && uv run ruff format --check .

fmt: ## auto-format
	uv run ruff check --fix . && uv run ruff format .

test: test-unit test-spark test-airflow ## all test suites

test-unit: ## fast tests (no Spark, no Airflow)
	uv run pytest -m "not spark and not airflow"

test-spark: ## SCD + pipeline end-to-end on local Spark/Delta (needs Java 17+)
	uv run pytest -m spark

test-airflow: ## DAG integrity
	AIRFLOW__CORE__LOAD_EXAMPLES=false uv run pytest -m airflow

seed: ## build the restaurant seed (Kaggle; FD_SEED_SOURCE=synthetic to skip Kaggle)
	uv run fd seed

generate: ## generate landing files: make generate DATE=2026-09-01 DAYS=3
	uv run fd generate --date $(DATE) --days $(DAYS)

local-run: ## run the real pipeline sources + DQ checks locally over data/landing
	uv run fd local-run

upload: ## sync landing files to the UC volume: make upload DATE=... DAYS=...
	uv run fd upload --date $(DATE) --days $(DAYS)

bundle-validate: ## databricks bundle validate (needs workspace auth)
	databricks bundle validate -t $(TARGET)

deploy: ## databricks bundle deploy -t $(TARGET)
	databricks bundle deploy -t $(TARGET)

bundle-env: ## print FD_* names of the deployed target (dev names are prefixed)
	uv run python scripts/bundle_env.py $(TARGET)

airflow-up: ## Airflow via docker compose (http://localhost:8080)
	$(COMPOSE) up -d --build --wait

airflow-down:
	$(COMPOSE) down

standalone: ## Airflow standalone without Docker (port 28880)
	AIRFLOW_HOME=$${AIRFLOW_HOME:-$$PWD/.airflow} AIRFLOW__CORE__DAGS_FOLDER=$$PWD/airflow/dags \
	AIRFLOW__CORE__LOAD_EXAMPLES=false AIRFLOW__API__PORT=28880 uv run airflow standalone

clean:
	rm -rf data/landing data/spark-warehouse .pytest_cache .ruff_cache dist
