with source as (
    select * from {{ source('metadata', 'airflow_task_instances') }}
),

cleaned as (
    select
        dag_id,
        run_id,
        task_id,
        lower(state)                                   as state,
        start_date,
        end_date,
        duration_seconds,
        try_number,
        records_fetched,
        max_watermark,
        s3_path,
        task_id like 'extract_%'                       as is_extract_task,
        state = 'success'                              as is_success,
        state = 'failed'                               as is_failed
    from source
)

select * from cleaned
