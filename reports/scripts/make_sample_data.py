"""Generate small synthetic snapshots for the Evidence demo build.

Writes fallback Parquet files matching the lakehouse gold table names so the
static site can be previewed without a warehouse. Values are illustrative only
and are not real SF civic data.

    uv run --group dashboard python reports/scripts/make_sample_data.py
"""

from __future__ import annotations

import random
from datetime import date, datetime, timedelta
from pathlib import Path

import polars as pl

OUT_DIR = (
    Path(__file__).resolve().parent.parent / "sources" / "sf_urban_health" / "data"
)
rng = random.Random(42)  # deterministic so the committed sample is stable

NEIGHBORHOODS = {
    "Mission": "9",
    "South Of Market": "6",
    "Bayview Hunters Point": "10",
    "Sunset/Parkside": "4",
    "Financial District/South Beach": "3",
}
USE_TRANSITIONS = ["new_residential", "unit_addition", "renovation_same_use"]
AGE_BUCKETS = ["<90d", "90-180d", "180-365d", ">365d"]
EVICTION_TYPES = ["at_fault", "no_fault"]
INCIDENT_CATEGORIES = ["Larceny Theft", "Assault", "Burglary", "Traffic Violation"]


def _months(n: int) -> list[date]:
    start = date(date.today().year - 2, 1, 1)
    return [
        date(
            start.year + (start.month - 1 + i) // 12, (start.month - 1 + i) % 12 + 1, 1
        )
        for i in range(n)
    ]


def housing() -> pl.DataFrame:
    rows = []
    for month in _months(24):
        for nbhd, district in NEIGHBORHOODS.items():
            for transition in USE_TRANSITIONS:
                filed = rng.randint(2, 40)
                issued = int(filed * rng.uniform(0.6, 0.95))
                completed = int(issued * rng.uniform(0.3, 0.8))
                proposed = (
                    filed * rng.randint(1, 6)
                    if transition != "renovation_same_use"
                    else 0
                )
                net = proposed - (filed if transition == "renovation_same_use" else 0)
                cost_per_unit = (
                    rng.uniform(250_000, 750_000)
                    if transition == "new_residential"
                    else None
                )
                rows.append(
                    {
                        "filed_month": month,
                        "neighborhood": nbhd,
                        "supervisor_district": district,
                        "use_transition": transition,
                        "permits_filed": filed,
                        "permits_issued": issued,
                        "permits_completed": completed,
                        "permits_expired": rng.randint(0, 3),
                        "proposed_units": proposed,
                        "net_units_added": net,
                        "total_project_cost": round(filed * rng.uniform(1e5, 2e6), 2),
                        "avg_days_to_issue": round(rng.uniform(30, 220), 1),
                        "median_days_to_issue": round(rng.uniform(25, 180), 1),
                        "avg_cost_per_unit": round(cost_per_unit, 2)
                        if cost_per_unit
                        else None,
                        "median_cost_per_unit": round(cost_per_unit * 0.95, 2)
                        if cost_per_unit
                        else None,
                    }
                )
    return pl.DataFrame(rows)


def permit_pipeline() -> pl.DataFrame:
    rows = []
    for nbhd, district in NEIGHBORHOODS.items():
        for lifecycle_stage in ["filed", "issued"]:
            for age_bucket in AGE_BUCKETS:
                count = rng.randint(1, 35)
                rows.append(
                    {
                        "neighborhood": nbhd,
                        "supervisor_district": district,
                        "lifecycle_stage": lifecycle_stage,
                        "age_bucket": age_bucket,
                        "snapshot_date": date.today(),
                        "permit_count": count,
                        "proposed_units": count * rng.randint(1, 5),
                        "avg_days_in_stage": round(rng.uniform(20, 420), 1),
                    }
                )
    return pl.DataFrame(rows)


def evictions() -> pl.DataFrame:
    rows = []
    for month in _months(24):
        for nbhd, district in NEIGHBORHOODS.items():
            for eviction_type in EVICTION_TYPES:
                count = rng.randint(0, 30)
                rows.append(
                    {
                        "filed_month": month,
                        "neighborhood": nbhd,
                        "supervisor_district": district,
                        "eviction_type": eviction_type,
                        "eviction_count": count,
                        "ellis_act_count": rng.randint(0, max(count, 1)),
                        "owner_move_in_count": rng.randint(0, max(count, 1)),
                        "non_payment_count": rng.randint(0, max(count, 1)),
                    }
                )
    return pl.DataFrame(rows)


def public_safety() -> pl.DataFrame:
    rows = []
    for month in _months(24):
        for nbhd, district in NEIGHBORHOODS.items():
            for category in INCIDENT_CATEGORIES:
                total = rng.randint(5, 220)
                resolved = int(total * rng.uniform(0.55, 0.9))
                open_count = int(total * rng.uniform(0.02, 0.18))
                rows.append(
                    {
                        "incident_month": month,
                        "neighborhood": nbhd,
                        "supervisor_district": district,
                        "police_district": f"District {district}",
                        "incident_category": category,
                        "total_incidents": total,
                        "resolved_count": resolved,
                        "open_count": open_count,
                        "unknown_count": max(total - resolved - open_count, 0),
                        "avg_report_lag_hours": round(rng.uniform(1, 48), 1),
                        "morning_incidents": rng.randint(0, total),
                        "afternoon_incidents": rng.randint(0, total),
                        "evening_incidents": rng.randint(0, total),
                        "night_incidents": rng.randint(0, total),
                    }
                )
    return pl.DataFrame(rows)


def pipeline_health() -> pl.DataFrame:
    rows = []
    datasets = ["permits", "evictions", "incidents"]
    for d in range(30):
        run_date = date.today() - timedelta(days=29 - d)
        for dataset in datasets:
            dag = f"ingest_{dataset}"
            failed = 1 if rng.random() < 0.05 else 0
            total = 1
            success = total - failed
            dur = rng.uniform(40, 360)
            records = rng.randint(0, 5000)
            rows.append(
                {
                    "run_date": run_date,
                    "dataset_name": dataset,
                    "dag_id": dag,
                    "total_runs": total,
                    "successful_runs": success,
                    "failed_runs": failed,
                    "empty_runs": 1 if records == 0 else 0,
                    "success_rate_pct": round(success * 100.0 / total, 1),
                    "avg_duration_seconds": round(dur, 1),
                    "p95_duration_seconds": round(dur * 1.2, 1),
                    "total_records_ingested": records,
                    "avg_records_per_run": records,
                    "latest_completed_at": datetime.combine(
                        run_date, datetime.min.time()
                    ),
                    "raw_objects_written": 0 if records == 0 else 1,
                    "bronze_files_written": 0 if records == 0 else 1,
                    "bronze_records_written": records,
                    "bronze_file_size_bytes": records * rng.randint(120, 420),
                    "latest_bronze_written_at": datetime.combine(
                        run_date, datetime.min.time()
                    ),
                    "intervals_missing_bronze": failed,
                }
            )
    return pl.DataFrame(rows)


def data_trust() -> pl.DataFrame:
    today = date.today()
    datasets = ["permits", "evictions", "incidents"]
    checks = [
        (
            "latest_ingest_status",
            "critical",
            "Latest ingest interval status is success or empty.",
        ),
        (
            "raw_object_recorded",
            "critical",
            "Non-empty extracts have a raw NDJSON object.",
        ),
        (
            "bronze_manifest_recorded",
            "critical",
            "Non-empty extracts have a bronze file-manifest event.",
        ),
        (
            "bronze_record_count_matches_extract",
            "critical",
            "Bronze records match extracted records.",
        ),
        (
            "metadata_freshness",
            "warning",
            "Latest metadata completed within the freshness threshold.",
        ),
    ]
    out = []
    for dataset in datasets:
        for check_name, severity, expected_rule in checks:
            status = "pass" if rng.random() > 0.08 else "warn"
            out.append(
                {
                    "dataset_name": dataset,
                    "dag_id": f"ingest_{dataset}",
                    "data_interval_start": datetime.combine(
                        today - timedelta(days=1),
                        datetime.min.time(),
                    ),
                    "data_interval_end": datetime.combine(today, datetime.min.time()),
                    "check_name": check_name,
                    "check_status": status,
                    "severity": severity,
                    "observed_value": "fallback sample",
                    "expected_rule": expected_rule,
                    "latest_completed_at": datetime.combine(today, datetime.min.time()),
                    "latest_bronze_written_at": datetime.combine(
                        today, datetime.min.time()
                    ),
                    "checked_at": datetime.combine(today, datetime.min.time()),
                }
            )
    return pl.DataFrame(out)


def main() -> None:
    OUT_DIR.mkdir(parents=True, exist_ok=True)
    for name, df in {
        "housing_production": housing(),
        "permit_pipeline": permit_pipeline(),
        "evictions": evictions(),
        "public_safety": public_safety(),
        "pipeline_health": pipeline_health(),
        "data_trust": data_trust(),
    }.items():
        out = OUT_DIR / f"{name}.parquet"
        df.write_parquet(out)
        print(f"wrote {name} ({df.height} rows) -> {out}")


if __name__ == "__main__":
    main()
