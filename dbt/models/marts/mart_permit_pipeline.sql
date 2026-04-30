{{ config(materialized='table') }}

-- Daily snapshot of in-flight residential permits (filed or issued, not yet
-- completed/expired/cancelled). Shows where the backlog is accumulating.

with timelines as (
    select * from {{ ref('int_permit_timelines') }}
),

in_flight as (
    select *
    from timelines
    where (coalesce(existing_units, 0) > 0 or coalesce(proposed_units, 0) > 0)
      and lifecycle_stage in ('filed', 'issued')
      and (
          current_status is null
          or lower(current_status) not in ('expired', 'cancelled', 'withdrawn', 'disapproved', 'revoked')
      )
      and filed_at is not null
),

bucketed as (
    select
        {{ normalize_neighborhood('neighborhood') }}             as neighborhood,
        supervisor_district,
        lifecycle_stage,
        datediff('day', filed_at, current_date)                 as days_since_filed,
        case
            when datediff('day', filed_at, current_date) < 90   then '<90d'
            when datediff('day', filed_at, current_date) < 180  then '90-180d'
            when datediff('day', filed_at, current_date) < 365  then '180-365d'
            else '>365d'
        end                                                      as age_bucket,
        coalesce(proposed_units, 0)                             as proposed_units,
        days_since_filed                                         as days_in_stage
    from in_flight
)

select
    neighborhood,
    supervisor_district,
    lifecycle_stage,
    age_bucket,
    current_date                                                 as snapshot_date,
    count(*)                                                     as permit_count,
    sum(proposed_units)                                          as proposed_units,
    avg(days_in_stage)                                           as avg_days_in_stage
from bucketed
group by 1, 2, 3, 4, 5
