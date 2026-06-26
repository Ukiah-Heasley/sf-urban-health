.PHONY: help
help:
	@echo "Targets:"
	@echo "  make ingest         Run permits extractor (DataSF -> S3)"
	@echo "  make sync-dbt       Mirror dbt/ into airflow/include/dbt/ for the Airflow image"
	@echo "  make airflow-up     Start local Airflow stack (Astro CLI), syncing dbt/ first"
	@echo "  make airflow-down   Stop local Airflow stack"
	@echo "  make airflow-logs   Tail Airflow scheduler logs"
	@echo "  make spark-up       Start local MinIO + Spark Thrift Server (lakehouse dev)"
	@echo "  make spark-down     Stop local lakehouse Docker stack"
	@echo "  make dbt-lakehouse-debug  Verify dbt Spark profile against Thrift Server"
	@echo "  make dbt-lakehouse-smoke  Run dbt smoke Iceberg model (requires spark-up)"
	@echo "  make lakehouse-prepare-fixtures  DESTRUCTIVE: reset local MinIO sandbox, seed all dataset fixtures, promote bronze (requires spark-up)"
	@echo "  make lakehouse-prepare-permits-fixture  Alias for lakehouse-prepare-fixtures"
	@echo "  make dbt-lakehouse-permits  Build and test permits_current silver Iceberg model (requires spark-up + fixture prep)"
	@echo "  make dbt-lakehouse-gold     Build and test all lakehouse bronze/silver/gold Iceberg models (requires spark-up + fixture prep)"
	@echo "  make dashboard-dev    Run Dash app locally on http://localhost:8050"
	@echo "  make dashboard-docker Build the dashboard Docker image"
	@echo "  make lint           Ruff lint"
	@echo "  make docs-check     Validate current-state documentation"
	@echo "  make test           Run pytest"
	@echo "  make lakehouse-smoke Promote fixture NDJSON locally without AWS"

ENV_FILE := airflow/.env
DBT_DIR  := dbt
DBT_MIRROR := airflow/include/dbt
LAKEHOUSE_DIR := lakehouse
LAKEHOUSE_COMPOSE := $(LAKEHOUSE_DIR)/docker-compose.yml
LAKEHOUSE_ENV := $(LAKEHOUSE_DIR)/.env
LAKEHOUSE_ENV_EXAMPLE := $(LAKEHOUSE_DIR)/.env.example

ifneq (,$(wildcard $(ENV_FILE)))
include $(ENV_FILE)
export
endif

ifneq (,$(wildcard $(LAKEHOUSE_ENV)))
include $(LAKEHOUSE_ENV)
export
endif

ifndef DBT_SPARK_PORT
export DBT_SPARK_PORT := $(if $(SPARK_THRIFT_PORT),$(SPARK_THRIFT_PORT),10000)
endif

.DEFAULT_GOAL := help

.PHONY: check-env
check-env:
	@test -f $(ENV_FILE) || (echo "Missing $(ENV_FILE) — copy from $(ENV_FILE).example and fill in credentials." && exit 1)

.PHONY: ingest
ingest: check-env
	uv run python airflow/include/scripts/permits.py

# Mirror dbt/ into airflow/include/dbt/ so Astro can bake it into the image.
# Excludes runtime artifacts; airflow/include/dbt/ is gitignored.
.PHONY: sync-dbt
sync-dbt:
	@mkdir -p $(DBT_MIRROR)
	@rsync -a --delete \
		--exclude='target/' --exclude='dbt_packages/' --exclude='logs/' \
		--exclude='.user.yml' \
		$(DBT_DIR)/ $(DBT_MIRROR)/
	@echo "synced $(DBT_DIR)/ -> $(DBT_MIRROR)/"

.PHONY: airflow-up
airflow-up: sync-dbt
	cd airflow && astro dev start

.PHONY: airflow-down
airflow-down:
	cd airflow && astro dev stop

.PHONY: airflow-logs
airflow-logs:
	cd airflow && astro dev logs --scheduler

.PHONY: dashboard-dev
dashboard-dev: check-env
	uv run --group dashboard python -m dashboard.app

.PHONY: dashboard-docker
dashboard-docker:
	docker build -f dashboard/Dockerfile -t sf-urban-health-dashboard .

.PHONY: lint
lint:
	uv run --group dev ruff check .

.PHONY: docs-check
docs-check:
	uv run --group dev python .codex/skills/maintain-project-docs/scripts/audit_docs.py

.PHONY: yamllint
yamllint:
	uv run --group dev yamllint .

.PHONY: pre-commit
pre-commit:
	uv run --group dev pre-commit run --all-files

.PHONY: test
test:
	uv run --group dev pytest

.PHONY: lakehouse-prepare-fixtures
lakehouse-prepare-fixtures: $(LAKEHOUSE_ENV)
	PYTHONPATH=airflow/include uv run --group lakehouse python airflow/include/scripts/lakehouse_fixture_prepare.py

.PHONY: lakehouse-prepare-permits-fixture
lakehouse-prepare-permits-fixture: lakehouse-prepare-fixtures

.PHONY: dbt-lakehouse-permits
dbt-lakehouse-permits: $(LAKEHOUSE_ENV)
	cd $(DBT_DIR) && uv run --group lakehouse dbt build --select +permits_current --profiles-dir .

.PHONY: dbt-lakehouse-gold
dbt-lakehouse-gold: $(LAKEHOUSE_ENV)
	cd $(DBT_DIR) && uv run --group lakehouse dbt build --select tag:lakehouse --profiles-dir .

.PHONY: lakehouse-smoke
lakehouse-smoke:
	PYTHONPATH=airflow/include uv run python airflow/include/scripts/lakehouse_smoke.py

$(LAKEHOUSE_ENV):
	@cp $(LAKEHOUSE_ENV_EXAMPLE) $(LAKEHOUSE_ENV)
	@echo "created $(LAKEHOUSE_ENV) from $(LAKEHOUSE_ENV_EXAMPLE)"

.PHONY: spark-up
spark-up: $(LAKEHOUSE_ENV)
	docker compose -f $(LAKEHOUSE_COMPOSE) --env-file $(LAKEHOUSE_ENV) up -d --build --wait

.PHONY: spark-down
spark-down:
	@if [ -f $(LAKEHOUSE_ENV) ]; then \
		docker compose -f $(LAKEHOUSE_COMPOSE) --env-file $(LAKEHOUSE_ENV) down; \
	else \
		docker compose -f $(LAKEHOUSE_COMPOSE) down; \
	fi

.PHONY: dbt-lakehouse-debug
dbt-lakehouse-debug: $(LAKEHOUSE_ENV)
	cd $(DBT_DIR) && uv run --group lakehouse dbt debug --profiles-dir .

.PHONY: dbt-lakehouse-smoke
dbt-lakehouse-smoke: $(LAKEHOUSE_ENV)
	cd $(DBT_DIR) && uv run --group lakehouse dbt run --select tag:smoke --profiles-dir .
