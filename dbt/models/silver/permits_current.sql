with bronze as (
    select * from {{ ref('bronze_permits') }}
),

ranked as (
    select
        *,
        row_number() over (
            partition by permit_number
            order by
                _loaded_at desc,
                _extracted_at desc,
                _data_interval_end desc,
                _record_hash asc
        ) as _row_num
    from bronze
),

latest as (
    select * from ranked where _row_num = 1
)

select
    permit_number,
    permit_type_code,
    permit_type,
    lower(status) as current_status,
    filed_at,
    issued_at,
    case
        when lower(status) = 'complete' then status_date
    end as completed_at,
    status_date as current_status_at,
    approved_at,
    last_activity_at,
    is_adu,
    estimated_cost,
    revised_cost,
    existing_units,
    proposed_units,
    existing_stories,
    proposed_stories,
    street_number,
    street_name,
    zipcode,
    supervisor_district,
    neighborhood,
    existing_use,
    proposed_use,
    _loaded_at
from latest
where permit_number is not null
