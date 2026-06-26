with permits as (
    select * from {{ ref('permits_current') }}
),

timelines as (
    select
        permit_number,
        neighborhood,
        supervisor_district,
        existing_units,
        proposed_units,
        existing_use,
        proposed_use,
        current_status,
        filed_at,
        issued_at,
        completed_at,
        coalesce(proposed_units, 0) - coalesce(existing_units, 0) as net_units_added,
        coalesce(revised_cost, estimated_cost) as project_cost,
        datediff(issued_at, filed_at) as days_to_issue,
        case
            when completed_at is not null then 'completed'
            when issued_at is not null then 'issued'
            when filed_at is not null then 'filed'
        end as lifecycle_stage,
        case
            when coalesce(existing_units, 0) = 0 and coalesce(proposed_units, 0) > 0
                then 'new_residential'
            when coalesce(existing_units, 0) > 0 and coalesce(proposed_units, 0) = 0
                then 'demolition'
            when coalesce(proposed_units, 0) > coalesce(existing_units, 0)
                then 'unit_addition'
            when (
                lower(existing_use) like '%commercial%'
                or lower(existing_use) like '%retail%'
                or lower(existing_use) like '%office%'
            )
                and (
                    lower(proposed_use) like '%residential%'
                    or lower(proposed_use) like '%dwelling%'
                    or lower(proposed_use) like '%apartment%'
                )
                then 'commercial_to_residential'
            when lower(existing_use) like '%single%'
                and (
                    lower(proposed_use) like '%multi%'
                    or lower(proposed_use) like '%apartment%'
                    or lower(proposed_use) like '%dwelling%'
                )
                then 'sfr_to_multifamily'
            when existing_use = proposed_use or proposed_use is null
                then 'renovation_same_use'
            else 'other'
        end as use_transition,
        case
            when coalesce(proposed_units, 0) - coalesce(existing_units, 0) > 0
                then coalesce(revised_cost, estimated_cost)
                    / (coalesce(proposed_units, 0) - coalesce(existing_units, 0))
        end as cost_per_unit
    from permits
),

residential as (
    select *
    from timelines
    where coalesce(existing_units, 0) > 0
       or coalesce(proposed_units, 0) > 0
),

monthly as (
    select
        cast(date_trunc('month', filed_at) as date) as filed_month,
        {{ normalize_neighborhood('neighborhood') }} as neighborhood,
        supervisor_district,
        use_transition,
        count(*) as permits_filed,
        count_if(issued_at is not null) as permits_issued,
        count_if(completed_at is not null) as permits_completed,
        count_if(lower(current_status) = 'expired') as permits_expired,
        sum(coalesce(proposed_units, 0)) as proposed_units,
        sum(net_units_added) as net_units_added,
        sum(project_cost) as total_project_cost,
        avg(days_to_issue) as avg_days_to_issue,
        percentile_approx(days_to_issue, 0.5) as median_days_to_issue,
        avg(cost_per_unit) as avg_cost_per_unit,
        percentile_approx(cost_per_unit, 0.5) as median_cost_per_unit
    from residential
    where filed_at is not null
    group by 1, 2, 3, 4
)

select * from monthly
