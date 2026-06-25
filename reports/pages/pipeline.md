---
title: Pipeline Health & Data Trust
---

<Alert status=info>
This static page reads committed Parquet snapshots exported from the Snowflake observability marts when credentials are available, with sample data as the build fallback.
</Alert>

## Data trust by dataset

A composite score (40% freshness + 60% 7-day test pass rate) from the `mart_data_trust` mart.

```sql trust
select
    dataset_name,
    freshness_status,
    test_pass_rate_7d,
    trust_score,
    trust_status
from sf_urban_health.mart_data_trust
order by trust_score desc
```

<DataTable data={trust}>
    <Column id=dataset_name title="Dataset"/>
    <Column id=freshness_status title="Freshness"/>
    <Column id=test_pass_rate_7d title="Test pass rate (7d)" fmt="0.0\%"/>
    <Column id=trust_score title="Trust score"/>
    <Column id=trust_status title="Status"/>
</DataTable>

## DAG success rate (last 30 days)

```sql success_trend
select run_date, dag_id, success_rate_pct
from sf_urban_health.mart_pipeline_health
order by run_date
```

<LineChart data={success_trend} x=run_date y=success_rate_pct series=dag_id yAxisTitle="Success rate %" yMax=100/>

## Records ingested per day

```sql records
select run_date, sum(total_records_ingested) as records_ingested
from sf_urban_health.mart_pipeline_health
group by 1
order by 1
```

<BarChart data={records} x=run_date y=records_ingested/>
