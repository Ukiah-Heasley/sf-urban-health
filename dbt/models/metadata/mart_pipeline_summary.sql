{{ config(materialized='table') }}

with base as (
    select * from {{ ref('mart_pipeline_health') }}
),

per_dag as (
    select
        dag_id,
        round(avg(case when run_date >= current_date - 6  then success_rate_pct end), 1)                        as success_rate_7d,
        round(avg(case when run_date >= current_date - 29 then success_rate_pct end), 1)                        as success_rate_30d,
        sum(case  when run_date >= current_date - 6  then total_runs   else 0 end)                              as total_runs_7d,
        sum(case  when run_date >= current_date - 6  then failed_runs  else 0 end)                              as failures_7d,
        round(avg(case when run_date >= current_date - 6  then avg_duration_seconds end), 1)                    as avg_duration_7d_seconds,
        round(avg(case when run_date between current_date - 13 and current_date - 7
                       then avg_duration_seconds end), 1)                                                       as avg_duration_prior_7d_seconds,
        sum(case  when run_date >= current_date - 6  then total_records_ingested else 0 end)                    as total_records_7d,
        sum(case  when run_date >= current_date - 29 then total_records_ingested else 0 end)                    as total_records_30d,
        max(run_date)                                                                                            as last_run_date
    from base
    group by dag_id
)

select
    dag_id,
    success_rate_7d,
    success_rate_30d,
    total_runs_7d,
    failures_7d,
    avg_duration_7d_seconds,
    avg_duration_prior_7d_seconds,
    round(
        case
            when avg_duration_prior_7d_seconds is null or avg_duration_prior_7d_seconds = 0 then null
            else (avg_duration_7d_seconds - avg_duration_prior_7d_seconds)
                 / avg_duration_prior_7d_seconds * 100
        end,
        1
    )                                                                                                            as duration_change_pct,
    total_records_7d,
    total_records_30d,
    last_run_date,
    case
        when success_rate_7d = 100 then 'healthy'
        when success_rate_7d > 0   then 'degraded'
        else                            'failing'
    end                                                                                                          as last_run_status
from per_dag
