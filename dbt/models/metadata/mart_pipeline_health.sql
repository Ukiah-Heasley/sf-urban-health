{{ config(materialized='table') }}

with dag_runs as (
    select * from {{ ref('stg_airflow_dag_runs') }}
    where dag_id != 'ingest_pipeline_metadata'
),

task_instances as (
    select
        run_id,
        sum(records_fetched)   as total_records_fetched
    from {{ ref('stg_airflow_task_instances') }}
    where is_extract_task
    group by 1
),

daily as (
    select
        r.run_date,
        r.dag_id,
        count(*)                                                               as total_runs,
        count_if(r.is_success)                                                 as successful_runs,
        count_if(r.is_failed)                                                  as failed_runs,
        round(count_if(r.is_success) * 100.0 / nullif(count(*), 0), 1)        as success_rate_pct,
        round(avg(r.duration_seconds), 1)                                      as avg_duration_seconds,
        round(
            percentile_cont(0.95) within group (order by r.duration_seconds),
            1
        )                                                                      as p95_duration_seconds,
        sum(coalesce(ti.total_records_fetched, 0))                             as total_records_ingested,
        round(avg(coalesce(ti.total_records_fetched, 0)), 0)                   as avg_records_per_run
    from dag_runs r
    left join task_instances ti on r.run_id = ti.run_id
    group by r.run_date, r.dag_id
)

select * from daily
