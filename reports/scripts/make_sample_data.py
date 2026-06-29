"""Generate a small synthetic snapshot for the Evidence demo build.

Writes parquet files for housing, data-trust, and pipeline-health marts so the
static site builds without a warehouse. Values are illustrative only — not real
SF civic data.

    uv run --group dashboard python reports/scripts/make_sample_data.py
"""

from __future__ import annotations

import random
from datetime import date, datetime, timedelta
from pathlib import Path

import polars as pl

OUT_DIR = Path(__file__).resolve().parent.parent / "sources" / "sf_urban_health" / "data"
rng = random.Random(42)  # deterministic so the committed sample is stable

NEIGHBORHOODS = {
    "Mission": "9",
    "South Of Market": "6",
    "Bayview Hunters Point": "10",
    "Sunset/Parkside": "4",
    "Financial District/South Beach": "3",
}
USE_TRANSITIONS = ["new_residential", "unit_addition", "renovation_same_use"]


def _months(n: int) -> list[date]:
    start = date(date.today().year - 2, 1, 1)
    return [date(start.year + (start.month - 1 + i) // 12, (start.month - 1 + i) % 12 + 1, 1) for i in range(n)]


def housing() -> pl.DataFrame:
    rows = []
    for month in _months(24):
        for nbhd, district in NEIGHBORHOODS.items():
            for transition in USE_TRANSITIONS:
                filed = rng.randint(2, 40)
                issued = int(filed * rng.uniform(0.6, 0.95))
                completed = int(issued * rng.uniform(0.3, 0.8))
                proposed = filed * rng.randint(1, 6) if transition != "renovation_same_use" else 0
                net = proposed - (filed if transition == "renovation_same_use" else 0)
                cost_per_unit = rng.uniform(250_000, 750_000) if transition == "new_residential" else None
                rows.append({
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
                    "avg_cost_per_unit": round(cost_per_unit, 2) if cost_per_unit else None,
                    "median_cost_per_unit": round(cost_per_unit * 0.95, 2) if cost_per_unit else None,
                })
    return pl.DataFrame(rows)


def pipeline_health() -> pl.DataFrame:
    rows = []
    dags = ["ingest_permits", "ingest_evictions", "ingest_incidents", "promote_raw_to_bronze"]
    for d in range(30):
        run_date = date.today() - timedelta(days=29 - d)
        for dag in dags:
            failed = 1 if rng.random() < 0.05 else 0
            total = 1
            success = total - failed
            dur = rng.uniform(40, 360)
            rows.append({
                "run_date": run_date,
                "dag_id": dag,
                "total_runs": total,
                "successful_runs": success,
                "failed_runs": failed,
                "success_rate_pct": round(success * 100.0 / total, 1),
                "avg_duration_seconds": round(dur, 1),
                "p95_duration_seconds": round(dur * 1.2, 1),
                "total_records_ingested": rng.randint(0, 5000) if dag != "promote_raw_to_bronze" else 0,
                "avg_records_per_run": rng.randint(0, 5000) if dag != "promote_raw_to_bronze" else 0,
            })
    return pl.DataFrame(rows)


def data_trust() -> pl.DataFrame:
    today = date.today()
    rows = [
        ("Permits", "ingest_permits", "stg_permits"),
        ("Evictions", "ingest_evictions", "stg_evictions"),
        ("Incidents", "ingest_incidents", "stg_incidents"),
    ]
    out = []
    for name, dag, model in rows:
        pass_rate = round(rng.uniform(92, 100), 1)
        freshness = "fresh"
        trust = round(0.4 * 100 + 0.6 * pass_rate)
        out.append({
            "dataset_name": name,
            "dag_id": dag,
            "model_name": model,
            "last_loaded_date": today,
            "days_since_last_load": 0,
            "freshness_status": freshness,
            "test_pass_rate_7d": pass_rate,
            "total_tests_7d": rng.randint(8, 20),
            "failed_tests_7d": 0,
            "last_test_failure_at": datetime(today.year, 1, 1),
            "trust_score": trust,
            "trust_status": "trusted" if trust >= 90 else "degraded",
        })
    return pl.DataFrame(out)


def main() -> None:
    OUT_DIR.mkdir(parents=True, exist_ok=True)
    for name, df in {
        "mart_housing_production": housing(),
        "mart_pipeline_health": pipeline_health(),
        "mart_data_trust": data_trust(),
    }.items():
        out = OUT_DIR / f"{name}.parquet"
        df.write_parquet(out)
        print(f"wrote {name} ({df.height} rows) -> {out}")


if __name__ == "__main__":
    main()
