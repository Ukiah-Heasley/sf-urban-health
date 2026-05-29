{{ config(materialized='table') }}

with timelines as (
    select * from {{ ref('int_incident_timelines') }}
),

monthly as (
    select
        incident_month,
        normalized_neighborhood                               as neighborhood,
        supervisor_district,
        police_district,
        incident_category,
        count(*)                                              as total_incidents,
        count_if(is_resolved)                                 as resolved_count,
        count_if(resolution_status = 'Open or Active')        as open_count,
        count_if(resolution_status = 'Unknown')               as unknown_count,
        avg(report_lag_hours)                                as avg_report_lag_hours,
        count_if(time_bucket = 'Morning')                    as morning_incidents,
        count_if(time_bucket = 'Afternoon')                  as afternoon_incidents,
        count_if(time_bucket = 'Evening')                    as evening_incidents,
        count_if(time_bucket = 'Night')                      as night_incidents
    from timelines
    where incident_month is not null
    group by 1, 2, 3, 4, 5
)

select * from monthly
