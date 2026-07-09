---
title: Pipeline Health & Data Trust
---

<Alert status=info>
This page reads `pipeline_health` and `data_trust` snapshots exported from lakehouse gold. Data trust checks describe operational metadata completeness and freshness, not independent validation of source-system truth.
</Alert>

## Pipeline health

```sql health_kpis
select
    max(run_date) as latest_run_date,
    round(avg(success_rate_pct), 1) as avg_success_rate_pct,
    sum(total_records_ingested) as total_records_ingested,
    sum(intervals_missing_bronze) as intervals_missing_bronze
from sf_urban_health.pipeline_health
```

<BigValue data={health_kpis} value=latest_run_date title="Latest run date"/>
<BigValue data={health_kpis} value=avg_success_rate_pct title="Average success rate" fmt="0.0\%"/>
<BigValue data={health_kpis} value=total_records_ingested title="Records ingested" fmt="#,##0"/>
<BigValue data={health_kpis} value=intervals_missing_bronze title="Missing bronze intervals" fmt="#,##0"/>

```sql success_trend
select
    run_date,
    dataset_name,
    avg(success_rate_pct) as success_rate_pct
from sf_urban_health.pipeline_health
group by 1, 2
order by 1, 2
```

<LineChart data={success_trend} x=run_date y=success_rate_pct series=dataset_name xAxisTitle="Run date" yAxisTitle="Success rate %" yMax=100/>

```sql records
select
    run_date,
    dataset_name,
    sum(total_records_ingested) as records_ingested
from sf_urban_health.pipeline_health
group by 1, 2
order by 1, 2
```

<BarChart data={records} x=run_date y=records_ingested series=dataset_name type=stacked xAxisTitle="Run date" yAxisTitle="Records ingested"/>

## Data trust

```sql trust_kpis
select
    sum(case when check_status = 'pass' then 1 else 0 end) as passing_checks,
    sum(case when check_status = 'warn' then 1 else 0 end) as warning_checks,
    sum(case when check_status = 'fail' then 1 else 0 end) as failing_checks,
    count(*) as total_checks
from sf_urban_health.data_trust
```

<BigValue data={trust_kpis} value=passing_checks title="Passing checks" fmt="#,##0"/>
<BigValue data={trust_kpis} value=warning_checks title="Warnings" fmt="#,##0"/>
<BigValue data={trust_kpis} value=failing_checks title="Failures" fmt="#,##0"/>
<BigValue data={trust_kpis} value=total_checks title="Total checks" fmt="#,##0"/>

```sql trust_by_dataset
select
    dataset_name,
    check_status,
    count(*) as checks
from sf_urban_health.data_trust
group by 1, 2
order by
    dataset_name,
    case check_status
        when 'pass' then 1
        when 'warn' then 2
        when 'fail' then 3
        else 4
    end
```

<BarChart data={trust_by_dataset} x=dataset_name y=checks series=check_status sort=false xAxisTitle="Dataset" yAxisTitle="Checks"/>

```sql trust_detail
select
    dataset_name,
    check_name,
    check_status,
    severity,
    observed_value,
    expected_rule,
    data_interval_end,
    checked_at
from sf_urban_health.data_trust
order by dataset_name, check_name
```

<DataTable data={trust_detail}>
    <Column id=dataset_name title="Dataset"/>
    <Column id=check_name title="Check"/>
    <Column id=check_status title="Status"/>
    <Column id=severity title="Severity"/>
    <Column id=observed_value title="Observed"/>
    <Column id=expected_rule title="Expected rule"/>
    <Column id=data_interval_end title="Interval end"/>
    <Column id=checked_at title="Checked at"/>
</DataTable>
