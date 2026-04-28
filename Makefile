ENV_FILE := airflow/.env
DBT_DIR  := dbt
DBT_MIRROR := airflow/include/dbt

ifneq (,$(wildcard $(ENV_FILE)))
include $(ENV_FILE)
export
endif

.DEFAULT_GOAL := help

.PHONY: help
help:
	@echo "Targets:"
	@echo "  make ingest         Run permits extractor (DataSF -> S3)"
	@echo "  make dbt-deps       Install dbt packages"
	@echo "  make dbt-build      Run dbt build (run + test) against Snowflake"
	@echo "  make dbt-test       Run dbt tests only"
	@echo "  make sync-dbt       Mirror dbt/ into airflow/include/dbt/ for the Airflow image"
	@echo "  make airflow-up     Start local Airflow stack (Astro CLI), syncing dbt/ first"
	@echo "  make airflow-down   Stop local Airflow stack"
	@echo "  make airflow-logs   Tail Airflow scheduler logs"
	@echo "  make dashboard-dev    Run Dash app locally on http://localhost:8050"
	@echo "  make dashboard-docker Build the dashboard Docker image"
	@echo "  make lint           Ruff lint"
	@echo "  make test           Run pytest"

.PHONY: check-env
check-env:
	@test -f $(ENV_FILE) || (echo "Missing $(ENV_FILE) — copy from $(ENV_FILE).example and fill in credentials." && exit 1)

.PHONY: ingest
ingest: check-env
	uv run python airflow/scripts/permits.py

.PHONY: dbt-deps
dbt-deps: check-env
	cd $(DBT_DIR) && uv run --group dbt dbt deps --profiles-dir .

.PHONY: dbt-build
dbt-build: check-env
	cd $(DBT_DIR) && uv run --group dbt dbt build --profiles-dir .

.PHONY: dbt-test
dbt-test: check-env
	cd $(DBT_DIR) && uv run --group dbt dbt test --profiles-dir .

# Mirror dbt/ into airflow/include/dbt/ so Astro can bake it into the image.
# Excludes runtime artifacts; airflow/include/dbt/ is gitignored.
.PHONY: sync-dbt
sync-dbt:
	@mkdir -p $(DBT_MIRROR)
	@rsync -a --delete \
		--exclude='target/' --exclude='dbt_packages/' --exclude='logs/' \
		--exclude='.user.yml' --exclude='profiles.yml' \
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
	uv run ruff check .

.PHONY: test
test:
	uv run pytest
