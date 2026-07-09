.PHONY: help
help:
	@echo "Targets:"
	@echo "  make ingest         Run permits extractor (DataSF -> S3)"
	@echo "  make sync-dbt       Mirror dbt/ and contracts/ into airflow/include/ for the Airflow image"
	@echo "  make airflow-up     Start local Airflow stack (alias for airflow-up-local)"
	@echo "  make airflow-up-local  Copy airflow/.env.local -> airflow/.env, sync assets, start Astro"
	@echo "  make airflow-up-aws    Copy airflow/.env.aws -> airflow/.env, sync assets, start Astro"
	@echo "  make airflow-down   Stop local Airflow stack"
	@echo "  make airflow-logs   Tail Airflow scheduler logs"
	@echo "  make spark-up       Start local MinIO + Spark Thrift Server (lakehouse/.env.local)"
	@echo "  make spark-up-aws   Start Spark Thrift in AWS Glue catalog mode (lakehouse/.env.aws)"
	@echo "  make spark-down     Stop local lakehouse Docker stack"
	@echo "  make dbt-lakehouse-debug  Verify dbt Spark profile against Thrift Server"
	@echo "  make dbt-lakehouse-smoke  Run dbt smoke Iceberg model (requires spark-up)"
	@echo "  make lakehouse-prepare-fixtures  DESTRUCTIVE: reset local MinIO sandbox, seed all dataset fixtures, promote bronze (requires spark-up)"
	@echo "  make lakehouse-prepare-permits-fixture  Alias for lakehouse-prepare-fixtures"
	@echo "  make dbt-lakehouse-permits  Build and test permits_current silver Iceberg model (requires spark-up + fixture prep)"
	@echo "  make dbt-lakehouse-gold     Build and test all lakehouse bronze/silver/gold Iceberg models (requires spark-up + fixture prep)"
	@echo "  make export-evidence-snapshots  Export Evidence Parquet snapshots from selected lakehouse gold (requires Spark + dbt-lakehouse-gold)"
	@echo "  make dashboard-dev    Run Dash app locally on http://localhost:8050"
	@echo "  make dashboard-docker Build the dashboard Docker image"
	@echo "  make lint           Ruff lint"
	@echo "  make docs-check     Validate current-state documentation"
	@echo "  make test           Run pytest"
	@echo "  make lakehouse-smoke Promote fixture NDJSON locally without AWS"

ENV_FILE := airflow/.env
AIRFLOW_ENV_LOCAL := airflow/.env.local
AIRFLOW_ENV_AWS := airflow/.env.aws
AIRFLOW_ENV_LOCAL_EXAMPLE := airflow/.env.local.example
AIRFLOW_ENV_AWS_EXAMPLE := airflow/.env.aws.example
DBT_DIR  := dbt
DBT_MIRROR := airflow/include/dbt
CONTRACTS_DIR := contracts
CONTRACTS_MIRROR := airflow/include/contracts
LAKEHOUSE_DIR := lakehouse
LAKEHOUSE_COMPOSE := $(LAKEHOUSE_DIR)/docker-compose.yml
LAKEHOUSE_ENV_LOCAL := lakehouse/.env.local
LAKEHOUSE_ENV_AWS := lakehouse/.env.aws
LAKEHOUSE_ENV_LOCAL_EXAMPLE := lakehouse/.env.local.example
LAKEHOUSE_ENV_AWS_EXAMPLE := lakehouse/.env.aws.example
LAKEHOUSE_ENV_FILE ?= $(LAKEHOUSE_ENV_LOCAL)

.DEFAULT_GOAL := help

define require_env_file
@test -f $(1) || (echo "Missing $(1) — copy from $(2) and fill in credentials." && exit 1)
endef

define source_airflow_env
@set -a && . $(ENV_FILE) && set +a
endef

define source_lakehouse_env
@set -a && . $(LAKEHOUSE_ENV_FILE) && : "$${DBT_SPARK_PORT:=$${SPARK_THRIFT_PORT:-10000}}" && export DBT_SPARK_PORT && set +a
endef

.PHONY: check-env
check-env:
	$(call require_env_file,$(ENV_FILE),$(AIRFLOW_ENV_LOCAL_EXAMPLE))
	@echo "Using active Airflow env at $(ENV_FILE)"

.PHONY: ingest
ingest: check-env
	$(call source_airflow_env) && uv run python airflow/include/scripts/permits.py

# Mirror repo assets into airflow/include/ so Astro can bake them into the image.
# Excludes runtime artifacts; airflow/include/dbt/ is gitignored.
.PHONY: sync-dbt
sync-dbt:
	@mkdir -p $(DBT_MIRROR)
	@rm -rf \
		$(DBT_MIRROR)/target \
		$(DBT_MIRROR)/dbt_packages \
		$(DBT_MIRROR)/logs \
		$(DBT_MIRROR)/.user.yml
	@rsync -a --delete \
		--exclude='target/' --exclude='dbt_packages/' --exclude='logs/' \
		--exclude='.user.yml' \
		$(DBT_DIR)/ $(DBT_MIRROR)/
	@echo "synced $(DBT_DIR)/ -> $(DBT_MIRROR)/"
	@mkdir -p $(CONTRACTS_MIRROR)
	@rsync -a --delete \
		$(CONTRACTS_DIR)/ $(CONTRACTS_MIRROR)/
	@echo "synced $(CONTRACTS_DIR)/ -> $(CONTRACTS_MIRROR)/"

.PHONY: airflow-up-local
airflow-up-local: sync-dbt
	$(call require_env_file,$(AIRFLOW_ENV_LOCAL),$(AIRFLOW_ENV_LOCAL_EXAMPLE))
	cp $(AIRFLOW_ENV_LOCAL) $(ENV_FILE)
	cd airflow && astro dev start

.PHONY: airflow-up-aws
airflow-up-aws: sync-dbt
	$(call require_env_file,$(AIRFLOW_ENV_AWS),$(AIRFLOW_ENV_AWS_EXAMPLE))
	cp $(AIRFLOW_ENV_AWS) $(ENV_FILE)
	cd airflow && astro dev start

.PHONY: airflow-up
airflow-up: airflow-up-local

.PHONY: airflow-down
airflow-down:
	cd airflow && astro dev stop

.PHONY: airflow-logs
airflow-logs:
	cd airflow && astro dev logs --scheduler

.PHONY: dashboard-dev
dashboard-dev: check-env
	$(call source_airflow_env) && uv run --group dashboard python -m dashboard.app

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

$(LAKEHOUSE_ENV_LOCAL):
	@cp $(LAKEHOUSE_ENV_LOCAL_EXAMPLE) $(LAKEHOUSE_ENV_LOCAL)
	@echo "created $(LAKEHOUSE_ENV_LOCAL) from $(LAKEHOUSE_ENV_LOCAL_EXAMPLE)"

$(LAKEHOUSE_ENV_AWS):
	@cp $(LAKEHOUSE_ENV_AWS_EXAMPLE) $(LAKEHOUSE_ENV_AWS)
	@echo "created $(LAKEHOUSE_ENV_AWS) from $(LAKEHOUSE_ENV_AWS_EXAMPLE)"

.PHONY: lakehouse-prepare-fixtures
lakehouse-prepare-fixtures: $(LAKEHOUSE_ENV_LOCAL)
	$(call source_lakehouse_env) && \
	PYTHONPATH=airflow/include uv run --group lakehouse python airflow/include/scripts/lakehouse_fixture_prepare.py

.PHONY: lakehouse-prepare-permits-fixture
lakehouse-prepare-permits-fixture: lakehouse-prepare-fixtures

.PHONY: dbt-lakehouse-permits
dbt-lakehouse-permits: $(LAKEHOUSE_ENV_FILE)
	$(call source_lakehouse_env) && cd $(DBT_DIR) && uv run --group lakehouse dbt build --select +permits_current --profiles-dir .

.PHONY: dbt-lakehouse-gold
dbt-lakehouse-gold: $(LAKEHOUSE_ENV_FILE)
	$(call source_lakehouse_env) && cd $(DBT_DIR) && uv run --group lakehouse dbt build --select tag:lakehouse --profiles-dir .

.PHONY: export-evidence-snapshots
export-evidence-snapshots: $(LAKEHOUSE_ENV_FILE)
	$(call source_lakehouse_env) && \
	PYTHONPATH=airflow/include uv run --group lakehouse python airflow/include/scripts/evidence_snapshots.py

.PHONY: lakehouse-smoke
lakehouse-smoke:
	PYTHONPATH=airflow/include uv run python airflow/include/scripts/lakehouse_smoke.py

.PHONY: spark-up
spark-up: $(LAKEHOUSE_ENV_LOCAL)
	LAKEHOUSE_CATALOG=hadoop docker compose -f $(LAKEHOUSE_COMPOSE) --profile local --env-file $(LAKEHOUSE_ENV_LOCAL) up -d --build --wait minio
	LAKEHOUSE_CATALOG=hadoop docker compose -f $(LAKEHOUSE_COMPOSE) --profile local --env-file $(LAKEHOUSE_ENV_LOCAL) run --rm minio-init
	LAKEHOUSE_CATALOG=hadoop docker compose -f $(LAKEHOUSE_COMPOSE) --profile local --env-file $(LAKEHOUSE_ENV_LOCAL) up -d --build --wait spark-thrift

.PHONY: spark-up-aws
spark-up-aws: $(LAKEHOUSE_ENV_AWS)
	LAKEHOUSE_CATALOG=glue docker compose -f $(LAKEHOUSE_COMPOSE) --env-file $(LAKEHOUSE_ENV_AWS) up -d --build --wait spark-thrift

.PHONY: spark-down
spark-down:
	@if [ -f $(LAKEHOUSE_ENV_LOCAL) ]; then \
		docker compose -f $(LAKEHOUSE_COMPOSE) --profile local --env-file $(LAKEHOUSE_ENV_LOCAL) down; \
	else \
		docker compose -f $(LAKEHOUSE_COMPOSE) --profile local down; \
	fi

.PHONY: dbt-lakehouse-debug
dbt-lakehouse-debug: $(LAKEHOUSE_ENV_FILE)
	$(call source_lakehouse_env) && cd $(DBT_DIR) && uv run --group lakehouse dbt debug --profiles-dir .

.PHONY: dbt-lakehouse-smoke
dbt-lakehouse-smoke: $(LAKEHOUSE_ENV_FILE)
	$(call source_lakehouse_env) && cd $(DBT_DIR) && uv run --group lakehouse dbt run --select tag:smoke --profiles-dir .
