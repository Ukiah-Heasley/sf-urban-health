with source as (
    select
        $1 as payload,
        _loaded_at
    from {{ source('raw', 'incidents') }}
),

deduped as (
    select *,
        row_number() over (
            partition by payload:row_id::string
            order by _loaded_at desc
        ) as rn
    from source
),

latest as (
    select * from deduped where rn = 1
),

unpacked as (
    select
        payload:row_id::string                          as row_id,
        payload:incident_id::string                     as incident_id,
        payload:incident_number::string                 as incident_number,
        payload:cad_number::string                      as cad_number,
        payload:report_type_code::string                as report_type_code,
        payload:report_type_description::string         as report_type,
        payload:filed_online::boolean                   as filed_online,
        payload:incident_datetime::timestamp_ntz        as incident_at,
        payload:incident_date::date                     as incident_date,
        payload:incident_time::string                   as incident_time,
        payload:incident_year::integer                  as incident_year,
        payload:incident_day_of_week::string            as incident_day_of_week,
        payload:report_datetime::timestamp_ntz          as reported_at,
        payload:incident_code::string                   as incident_code,
        payload:incident_category::string               as incident_category,
        payload:incident_subcategory::string            as incident_subcategory,
        payload:incident_description::string            as incident_description,
        payload:resolution::string                      as resolution,
        payload:intersection::string                    as intersection,
        payload:police_district::string                 as police_district,
        payload:analysis_neighborhood::string           as neighborhood,
        payload:supervisor_district::string             as supervisor_district,
        payload:latitude::float                         as latitude,
        payload:longitude::float                        as longitude,
        _loaded_at
    from latest
)

select *
from unpacked
where row_id is not null
