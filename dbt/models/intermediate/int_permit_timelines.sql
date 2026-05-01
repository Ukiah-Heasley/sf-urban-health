with permits as (
    select * from {{ ref('stg_permits') }}
),

timelines as (
    select
        permit_number,
        permit_type,
        current_status,
        neighborhood,
        supervisor_district,
        zipcode,
        existing_units,
        proposed_units,
        coalesce(proposed_units, 0) - coalesce(existing_units, 0) as net_units_added,
        coalesce(revised_cost, estimated_cost) as project_cost,
        filed_at,
        issued_at,
        completed_at,
        datediff('day', filed_at, issued_at)                     as days_to_issue,
        datediff('day', issued_at, completed_at)                 as days_issue_to_complete,
        datediff('day', filed_at, completed_at)                  as days_total,
        case
            when completed_at is not null then 'completed'
            when issued_at is not null then 'issued'
            when filed_at is not null then 'filed'
        end as lifecycle_stage,

        -- Classify the structural change represented by the permit
        case
            when coalesce(existing_units, 0) = 0 and coalesce(proposed_units, 0) > 0
                then 'new_residential'
            when coalesce(existing_units, 0) > 0 and coalesce(proposed_units, 0) = 0
                then 'demolition'
            when coalesce(proposed_units, 0) > coalesce(existing_units, 0)
                then 'unit_addition'
            when lower(existing_use) like any ('%commercial%', '%retail%', '%office%')
                and lower(proposed_use) like any ('%residential%', '%dwelling%', '%apartment%')
                then 'commercial_to_residential'
            when lower(existing_use) like '%single%'
                and lower(proposed_use) like any ('%multi%', '%apartment%', '%dwelling%')
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
)

select * from timelines
