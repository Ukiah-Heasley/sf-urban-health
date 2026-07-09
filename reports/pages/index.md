---
title: SF Urban Health Gold
---

<Alert status=info>
This static site reads committed Parquet snapshots exported from lakehouse gold. Regenerate them from the selected Spark/Iceberg catalog with `make export-evidence-snapshots`.
</Alert>

Domain views from the `housing_production`, `permit_pipeline`, `evictions`, and `public_safety` gold tables.

```sql nbhd_options
select neighborhood from sf_urban_health.housing_production group by 1 order by 1
```

<Dropdown data={nbhd_options} name=nbhd value=neighborhood title="Neighborhood">
    <DropdownOption value="%" valueLabel="All neighborhoods"/>
</Dropdown>

```sql housing_kpis
select
    sum(permits_filed) as permits_filed,
    sum(net_units_added) as net_units_added,
    sum(total_project_cost) as total_project_cost
from sf_urban_health.housing_production
where neighborhood like '${inputs.nbhd.value}'
```

<BigValue data={housing_kpis} value=permits_filed title="Housing permits filed" fmt="#,##0"/>
<BigValue data={housing_kpis} value=net_units_added title="Net new units" fmt="#,##0"/>
<BigValue data={housing_kpis} value=total_project_cost title="Residential project cost" fmt="usd0"/>

## Housing production

```sql housing_monthly
with filtered as (
    select filed_month, net_units_added
    from sf_urban_health.housing_production
    where neighborhood like '${inputs.nbhd.value}'
),
bounds as (
    select max(filed_month) as max_month
    from filtered
)
select filtered.filed_month, sum(filtered.net_units_added) as net_units
from filtered
cross join bounds
where filtered.filed_month >= bounds.max_month - interval '36 months'
group by 1
order by 1
```

<LineChart data={housing_monthly} x=filed_month y=net_units xAxisTitle="Filed month" yAxisTitle="Net new units"/>

```sql housing_transition
with filtered as (
    select filed_month, use_transition, net_units_added
    from sf_urban_health.housing_production
    where neighborhood like '${inputs.nbhd.value}'
),
bounds as (
    select max(filed_month) as max_month
    from filtered
)
select
    filtered.filed_month,
    filtered.use_transition,
    sum(filtered.net_units_added) as net_units
from filtered
cross join bounds
where filtered.filed_month >= bounds.max_month - interval '36 months'
group by 1, 2
order by 1, 2
```

<BarChart data={housing_transition} x=filed_month y=net_units series=use_transition type=stacked xAxisTitle="Filed month" yAxisTitle="Net units"/>

## Permit pipeline

```sql permit_backlog
select
    age_bucket,
    lifecycle_stage,
    sum(permit_count) as permit_count,
    sum(proposed_units) as proposed_units
from sf_urban_health.permit_pipeline
where neighborhood like '${inputs.nbhd.value}'
group by 1, 2
order by
    case age_bucket
        when '<90d' then 1
        when '90-180d' then 2
        when '180-365d' then 3
        when '>365d' then 4
        else 5
    end,
    case lifecycle_stage
        when 'filed' then 1
        when 'issued' then 2
        else 3
    end
```

<BarChart data={permit_backlog} x=age_bucket y=permit_count series=lifecycle_stage sort=false xAxisTitle="Age bucket" yAxisTitle="Permits"/>

## Evictions

```sql evictions_monthly
with filtered as (
    select filed_month, eviction_type, eviction_count
    from sf_urban_health.evictions
    where neighborhood like '${inputs.nbhd.value}'
),
bounds as (
    select max(filed_month) as max_month
    from filtered
)
select
    filtered.filed_month,
    filtered.eviction_type,
    sum(filtered.eviction_count) as eviction_count
from filtered
cross join bounds
where filtered.filed_month >= bounds.max_month - interval '36 months'
group by 1, 2
order by 1, 2
```

<LineChart data={evictions_monthly} x=filed_month y=eviction_count series=eviction_type xAxisTitle="Filed month" yAxisTitle="Eviction notices"/>

## Public safety

```sql incidents_monthly
with filtered as (
    select incident_month, total_incidents
    from sf_urban_health.public_safety
    where neighborhood like '${inputs.nbhd.value}'
),
bounds as (
    select max(incident_month) as max_month
    from filtered
)
select filtered.incident_month, sum(filtered.total_incidents) as total_incidents
from filtered
cross join bounds
where filtered.incident_month >= bounds.max_month - interval '36 months'
group by 1
order by 1
```

<LineChart data={incidents_monthly} x=incident_month y=total_incidents xAxisTitle="Incident month" yAxisTitle="Incidents"/>

```sql incident_categories
select incident_category, sum(total_incidents) as total_incidents
from sf_urban_health.public_safety
where neighborhood like '${inputs.nbhd.value}'
group by 1
order by total_incidents desc
limit 10
```

<BarChart data={incident_categories} x=incident_category y=total_incidents swapXY=true/>
