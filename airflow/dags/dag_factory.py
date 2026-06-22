"""Factory for API-to-raw DataSF ingest DAGs.

Each DAG built here owns one EL responsibility:

1. Use Airflow's data interval to request records from a DataSF SODA endpoint.
2. Write the interval's response to immutable raw NDJSON in S3.
3. Emit the dataset ingest asset so downstream promotion/transform DAGs can run.

The DAG deliberately stops at raw S3. Loading raw files into bronze Parquet and
running dbt models belongs to the transform layer.
"""
from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime, timedelta

from airflow.providers.standard.operators.empty import EmptyOperator
from airflow.providers.standard.operators.python import PythonOperator
from airflow.sdk import DAG

from pipeline_assets import ingest_asset_for
from scripts.soda_ingest import (
    DatasetConfig,
    ExtractWindow,
    S3NdjsonWriter,
    SodaClient,
    build_soda_session,
    extract_to_raw,
)


@dataclass
class DagConfig:
    """Configuration for one generated DataSF raw ingest DAG.

    Inputs:
    - ``dataset`` contains the source endpoint, interval field, and raw S3
      namespace.
    - ``schedule`` and ``start_date`` define the Airflow data intervals that
      become SODA query bounds.
    - ``tags`` are passed through to Airflow for UI filtering.

    Output:
    - ``make_ingest_dag`` turns this config into a two-task DAG:
      ``extract_<dataset>_to_raw`` -> ``ingest_complete``.
    """

    dataset: DatasetConfig
    schedule: str
    start_date: datetime
    tags: list[str] = field(default_factory=list)


def make_ingest_dag(cfg: DagConfig) -> DAG:
    """Build the Airflow DAG that extracts one DataSF dataset to raw S3.

    Input is a ``DagConfig`` describing the dataset and schedule. Output is an
    Airflow ``DAG`` object with the existing ingest asset attached to the final
    task. Empty intervals still complete successfully and emit the asset because
    the source interval was evaluated.
    """

    def _extract_to_raw(**context) -> str | None:
        """Extract the current Airflow interval and expose run metadata as XCom.

        Inputs come from Airflow context:
        - ``data_interval_start`` and ``data_interval_end`` define the SODA
          half-open query window.
        - ``ti`` is used to push metadata for observability and debugging.

        Output is the raw S3 URI when records were written, or ``None`` for an
        empty interval.
        """

        window = ExtractWindow(
            data_interval_start=context["data_interval_start"],
            data_interval_end=context["data_interval_end"],
        )
        result = extract_to_raw(
            cfg.dataset,
            window,
            client=SodaClient(session=build_soda_session()),
            writer=S3NdjsonWriter.from_env(),
        )
        context["ti"].xcom_push(key="raw_path", value=result.raw_path)
        context["ti"].xcom_push(key="raw_key", value=result.raw_key)
        context["ti"].xcom_push(key="records_fetched", value=result.records_fetched)
        context["ti"].xcom_push(
            key="max_loaded_at",
            value=result.max_loaded_at.isoformat() if result.max_loaded_at else None,
        )
        context["ti"].xcom_push(key="bytes_written", value=result.bytes_written)
        context["ti"].xcom_push(key="duration_seconds", value=result.duration_seconds)
        context["ti"].xcom_push(
            key="data_interval_start",
            value=result.data_interval_start.isoformat(),
        )
        context["ti"].xcom_push(
            key="data_interval_end",
            value=result.data_interval_end.isoformat(),
        )
        context["ti"].xcom_push(key="effective_start", value=result.effective_start.isoformat())
        return result.raw_path

    with DAG(
        dag_id=f"ingest_{cfg.dataset.name}",
        description=f"Daily {cfg.dataset.name}: DataSF interval -> S3 raw",
        schedule=cfg.schedule,
        start_date=cfg.start_date,
        catchup=False,
        default_args={
            "owner": "data-eng",
            "retries": 3,
            "retry_delay": timedelta(minutes=5),
        },
        tags=cfg.tags,
    ) as dag:
        extract = PythonOperator(
            task_id=f"extract_{cfg.dataset.name}_to_raw",
            python_callable=_extract_to_raw,
        )

        ingest_complete = EmptyOperator(
            task_id="ingest_complete",
            outlets=[ingest_asset_for(cfg.dataset.name)],
        )

        extract >> ingest_complete

    return dag
