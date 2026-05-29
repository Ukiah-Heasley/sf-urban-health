with incidents as (
    select * from {{ ref('stg_incidents') }}
),

enriched as (
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

        {{ normalize_neighborhood('neighborhood') }}          as normalized_neighborhood,
        extract(hour from incident_at)                        as hour_of_day,
        case
            when extract(hour from incident_at) between 6  and 11 then 'Morning'
            when extract(hour from incident_at) between 12 and 16 then 'Afternoon'
            when extract(hour from incident_at) between 17 and 20 then 'Evening'
            else 'Night'
        end                                                   as time_bucket,
        case
            when incident_day_of_week in ('Saturday', 'Sunday') then 'Weekend'
            else 'Weekday'
        end                                                   as day_type,
        datediff('hour', incident_at, reported_at)            as report_lag_hours,
        -- DataSF leaves resolution null for both "open" and "not yet recorded";
        -- keep them distinct so downstream counts don't conflate the two.
        coalesce(resolution, 'Unknown')                       as resolution_status,
        coalesce(resolution, 'Unknown')
            not in ('Open or Active', 'Unknown')              as is_resolved,
        date_trunc('month', incident_date)::date              as incident_month
    from incidents
)

select * from enriched
