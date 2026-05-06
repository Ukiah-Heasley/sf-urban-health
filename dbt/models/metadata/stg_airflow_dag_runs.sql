with source as (
    select * from {{ source('metadata', 'airflow_dag_runs') }}
),

cleaned as (
    select
        dag_id,
        run_id,
        lower(state)                          as state,
        execution_date::date                  as run_date,
        execution_date,
        start_date,
        end_date,
        duration_seconds,
        lower(run_type)                       as run_type,
        state = 'success'                     as is_success,
        state = 'failed'                      as is_failed
    from source
)

select * from cleaned
