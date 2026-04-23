{{ config(materialized='table') }}

with timelines as (
    select * from {{ ref('int_permit_timelines') }}
),

monthly as (
    select
        date_trunc('month', filed_at)::date                      as filed_month,
        {{ normalize_neighborhood('neighborhood') }}             as neighborhood,
        supervisor_district,
        count(*)                                                 as permits_filed,
        count_if(issued_at is not null)                          as permits_issued,
        count_if(completed_at is not null)                       as permits_completed,
        sum(net_units_added)                                     as net_units_added,
        sum(project_cost)                                        as total_project_cost,
        avg(days_to_issue)                                       as avg_days_to_issue,
        median(days_to_issue)                                    as median_days_to_issue
    from timelines
    where filed_at is not null
    group by 1, 2, 3
)

select * from monthly
