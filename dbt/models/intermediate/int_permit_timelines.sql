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
        end as lifecycle_stage
    from permits
)

select * from timelines
