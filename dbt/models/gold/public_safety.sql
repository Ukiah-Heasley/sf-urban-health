with incidents as (
    select * from {{ ref('incidents_current') }}
),

enriched as (
    select
        *,
        {{ normalize_neighborhood('neighborhood') }} as normalized_neighborhood,
        case
            when hour(incident_at) between 6 and 11 then 'Morning'
            when hour(incident_at) between 12 and 16 then 'Afternoon'
            when hour(incident_at) between 17 and 20 then 'Evening'
            else 'Night'
        end as time_bucket,
        (unix_timestamp(reported_at) - unix_timestamp(incident_at)) / 3600.0 as report_lag_hours,
        coalesce(resolution, 'Unknown') as resolution_status,
        coalesce(resolution, 'Unknown') not in ('Open or Active', 'Unknown') as is_resolved,
        cast(date_trunc('month', incident_date) as date) as incident_month
    from incidents
),

monthly as (
    select
        incident_month,
        normalized_neighborhood as neighborhood,
        supervisor_district,
        police_district,
        incident_category,
        count(*) as total_incidents,
        count_if(is_resolved) as resolved_count,
        count_if(resolution_status = 'Open or Active') as open_count,
        count_if(resolution_status = 'Unknown') as unknown_count,
        avg(report_lag_hours) as avg_report_lag_hours,
        count_if(time_bucket = 'Morning') as morning_incidents,
        count_if(time_bucket = 'Afternoon') as afternoon_incidents,
        count_if(time_bucket = 'Evening') as evening_incidents,
        count_if(time_bucket = 'Night') as night_incidents
    from enriched
    where incident_month is not null
    group by 1, 2, 3, 4, 5
)

select * from monthly
