---
title: SF Housing Production
---

<Alert status=info>
This static page reads a committed Parquet snapshot. The Pages workflow replaces it from Snowflake when repository credentials are available and otherwise keeps the sample data.
</Alert>

Monthly residential building-permit activity across San Francisco, from the `mart_housing_production` mart.

```sql nbhd_options
select neighborhood from sf_urban_health.mart_housing_production group by 1 order by 1
```

<Dropdown data={nbhd_options} name=nbhd value=neighborhood title="Neighborhood">
    <DropdownOption value="%" valueLabel="All neighborhoods"/>
</Dropdown>

```sql kpis
select
    sum(permits_filed)      as permits_filed,
    sum(net_units_added)    as net_units_added,
    sum(total_project_cost) as total_project_cost
from sf_urban_health.mart_housing_production
where neighborhood like '${inputs.nbhd.value}'
```

<BigValue data={kpis} value=permits_filed title="Permits filed" fmt="#,##0"/>
<BigValue data={kpis} value=net_units_added title="Net new units" fmt="#,##0"/>
<BigValue data={kpis} value=total_project_cost title="Project cost" fmt="usd0"/>

## Net new units by month

```sql monthly
select filed_month, sum(net_units_added) as net_units
from sf_urban_health.mart_housing_production
where neighborhood like '${inputs.nbhd.value}'
group by 1
order by 1
```

<LineChart data={monthly} x=filed_month y=net_units yAxisTitle="Net new units"/>

## Net new units by permit type

```sql by_transition
select filed_month, use_transition, sum(net_units_added) as net_units
from sf_urban_health.mart_housing_production
where neighborhood like '${inputs.nbhd.value}'
group by 1, 2
order by 1
```

<BarChart data={by_transition} x=filed_month y=net_units series=use_transition type=stacked/>

## Top neighborhoods by net new units

```sql top_nbhd
select neighborhood, sum(net_units_added) as net_units
from sf_urban_health.mart_housing_production
group by 1
order by net_units desc
limit 10
```

<BarChart data={top_nbhd} x=neighborhood y=net_units swapXY=true/>

See also the [pipeline health & data trust](/pipeline) page.
