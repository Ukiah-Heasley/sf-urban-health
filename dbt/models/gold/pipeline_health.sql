with ingest_runs as (
    select * from {{ ref('metadata_ingest_runs') }}
),

file_manifest as (
    select * from {{ ref('metadata_file_manifest') }}
),

bronze_manifests as (
    select
        table_name as dataset_name,
        data_interval_start,
        data_interval_end,
        count(*) as bronze_file_count,
        sum(record_count) as bronze_record_count,
        sum(file_size_bytes) as bronze_file_size_bytes,
        max(written_at) as latest_bronze_written_at
    from file_manifest
    where layer = 'bronze'
      and data_interval_start is not null
      and data_interval_end is not null
    group by 1, 2, 3
),

joined as (
    select
        ingest_runs.ingest_run_id,
        ingest_runs.dag_id,
        ingest_runs.dataset_name,
        ingest_runs.data_interval_start,
        ingest_runs.data_interval_end,
        ingest_runs.raw_s3_path,
        ingest_runs.records_fetched,
        ingest_runs.bytes_written,
        ingest_runs.duration_seconds,
        ingest_runs.status,
        ingest_runs.started_at,
        ingest_runs.completed_at,
        coalesce(bronze_manifests.bronze_file_count, 0) as bronze_file_count,
        coalesce(bronze_manifests.bronze_record_count, 0) as bronze_record_count,
        coalesce(bronze_manifests.bronze_file_size_bytes, 0) as bronze_file_size_bytes,
        bronze_manifests.latest_bronze_written_at
    from ingest_runs
    left join bronze_manifests
        on ingest_runs.dataset_name = bronze_manifests.dataset_name
       and ingest_runs.data_interval_start = bronze_manifests.data_interval_start
       and ingest_runs.data_interval_end = bronze_manifests.data_interval_end
)

select
    cast(date_trunc('day', coalesce(completed_at, started_at)) as date) as run_date,
    dataset_name,
    dag_id,
    count(*) as total_runs,
    count_if(status in ('success', 'empty')) as successful_runs,
    count_if(status = 'failed') as failed_runs,
    count_if(status = 'empty') as empty_runs,
    round(100.0 * count_if(status in ('success', 'empty')) / count(*), 1) as success_rate_pct,
    sum(records_fetched) as total_records_ingested,
    avg(records_fetched) as avg_records_per_run,
    avg(duration_seconds) as avg_duration_seconds,
    percentile_approx(duration_seconds, 0.95) as p95_duration_seconds,
    max(completed_at) as latest_completed_at,
    count_if(raw_s3_path is not null) as raw_objects_written,
    sum(bronze_file_count) as bronze_files_written,
    sum(bronze_record_count) as bronze_records_written,
    sum(bronze_file_size_bytes) as bronze_file_size_bytes,
    max(latest_bronze_written_at) as latest_bronze_written_at,
    count_if(records_fetched > 0 and bronze_file_count = 0) as intervals_missing_bronze
from joined
group by 1, 2, 3
