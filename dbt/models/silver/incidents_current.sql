with bronze as (
    select * from {{ ref('bronze_incidents') }}
),

ranked as (
    select
        *,
        row_number() over (
            partition by row_id
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
    row_id,
    incident_id,
    incident_number,
    cad_number,
    report_type_code,
    report_type,
    filed_online,
    incident_at,
    incident_date,
    incident_time,
    incident_year,
    incident_day_of_week,
    reported_at,
    incident_code,
    incident_category,
    incident_subcategory,
    incident_description,
    resolution,
    intersection,
    police_district,
    neighborhood,
    supervisor_district,
    latitude,
    longitude,
    _loaded_at
from latest
where row_id is not null
