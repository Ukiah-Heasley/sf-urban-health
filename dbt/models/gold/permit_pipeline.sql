with permits as (
    select * from {{ ref('permits_current') }}
),

timelines as (
    select
        neighborhood,
        supervisor_district,
        current_status,
        filed_at,
        issued_at,
        completed_at,
        existing_units,
        coalesce(proposed_units, 0) as proposed_units,
        case
            when completed_at is not null then 'completed'
            when issued_at is not null then 'issued'
            when filed_at is not null then 'filed'
        end as lifecycle_stage
    from permits
),

in_flight as (
    select *
    from timelines
    where (coalesce(existing_units, 0) > 0 or coalesce(proposed_units, 0) > 0)
      and lifecycle_stage in ('filed', 'issued')
      and (
          current_status is null
          or lower(current_status) not in (
              'expired', 'cancelled', 'withdrawn', 'disapproved', 'revoked'
          )
      )
      and filed_at is not null
),

bucketed as (
    select
        {{ normalize_neighborhood('neighborhood') }} as neighborhood,
        supervisor_district,
        lifecycle_stage,
        datediff(current_date(), filed_at) as days_since_filed,
        case
            when datediff(current_date(), filed_at) < 90 then '<90d'
            when datediff(current_date(), filed_at) < 180 then '90-180d'
            when datediff(current_date(), filed_at) < 365 then '180-365d'
            else '>365d'
        end as age_bucket,
        proposed_units
    from in_flight
)

select
    neighborhood,
    supervisor_district,
    lifecycle_stage,
    age_bucket,
    current_date() as snapshot_date,
    count(*) as permit_count,
    sum(proposed_units) as proposed_units,
    avg(days_since_filed) as avg_days_in_stage
from bucketed
group by 1, 2, 3, 4, 5
