with ingest_runs as (
    select * from {{ ref('metadata_ingest_runs') }}
),

file_manifest as (
    select * from {{ ref('metadata_file_manifest') }}
),

daily_interval_coverage as (
    select * from {{ ref('metadata_daily_interval_coverage') }}
),

bronze_manifests as (
    select
        table_name as dataset_name,
        data_interval_start,
        data_interval_end,
        count(*) as bronze_file_count,
        sum(record_count) as bronze_record_count,
        max(written_at) as latest_bronze_written_at
    from file_manifest
    where layer = 'bronze'
      and data_interval_start is not null
      and data_interval_end is not null
    group by 1, 2, 3
),

latest_ranked as (
    select
        ingest_runs.dataset_name,
        ingest_runs.dag_id,
        ingest_runs.data_interval_start,
        ingest_runs.data_interval_end,
        ingest_runs.raw_s3_path,
        ingest_runs.records_fetched,
        ingest_runs.status,
        ingest_runs.completed_at,
        coalesce(bronze_manifests.bronze_file_count, 0) as bronze_file_count,
        coalesce(bronze_manifests.bronze_record_count, 0) as bronze_record_count,
        bronze_manifests.latest_bronze_written_at,
        row_number() over (
            partition by ingest_runs.dataset_name
            order by ingest_runs.data_interval_end desc, ingest_runs.completed_at desc
        ) as row_num
    from ingest_runs
    left join bronze_manifests
        on ingest_runs.dataset_name = bronze_manifests.dataset_name
       and ingest_runs.data_interval_start = bronze_manifests.data_interval_start
       and ingest_runs.data_interval_end = bronze_manifests.data_interval_end
),

latest as (
    select *
    from latest_ranked
    where row_num = 1
),

checks as (
    select
        dataset_name,
        dag_id,
        data_interval_start,
        data_interval_end,
        'latest_ingest_status' as check_name,
        case when status in ('success', 'empty') then 'pass' else 'fail' end as check_status,
        'critical' as severity,
        status as observed_value,
        'Latest ingest interval status is success or empty.' as expected_rule,
        completed_at as latest_completed_at,
        latest_bronze_written_at
    from latest

    union all

    select
        dataset_name,
        dag_id,
        data_interval_start,
        data_interval_end,
        'raw_object_recorded' as check_name,
        case
            when records_fetched = 0 then 'pass'
            when raw_s3_path is not null then 'pass'
            else 'fail'
        end as check_status,
        'critical' as severity,
        case
            when raw_s3_path is not null then raw_s3_path
            when records_fetched = 0 then 'empty extract'
            else 'missing raw object'
        end as observed_value,
        'Non-empty extracts have a raw NDJSON object; empty extracts are explicitly allowed.' as expected_rule,
        completed_at as latest_completed_at,
        latest_bronze_written_at
    from latest

    union all

    select
        dataset_name,
        dag_id,
        data_interval_start,
        data_interval_end,
        'bronze_manifest_recorded' as check_name,
        case
            when records_fetched = 0 then 'pass'
            when bronze_file_count > 0 then 'pass'
            else 'fail'
        end as check_status,
        'critical' as severity,
        cast(bronze_file_count as string) as observed_value,
        'Non-empty extracts have at least one bronze file-manifest event.' as expected_rule,
        completed_at as latest_completed_at,
        latest_bronze_written_at
    from latest

    union all

    select
        dataset_name,
        dag_id,
        data_interval_start,
        data_interval_end,
        'bronze_record_count_matches_extract' as check_name,
        case
            when records_fetched = bronze_record_count then 'pass'
            else 'fail'
        end as check_status,
        'critical' as severity,
        concat(
            'records_fetched=',
            cast(records_fetched as string),
            ', bronze_record_count=',
            cast(bronze_record_count as string)
        ) as observed_value,
        'Bronze manifest record counts match extracted source records.' as expected_rule,
        completed_at as latest_completed_at,
        latest_bronze_written_at
    from latest

    union all

    select
        dataset_name,
        dag_id,
        data_interval_start,
        data_interval_end,
        'metadata_freshness' as check_name,
        case
            when datediff(current_date(), cast(coalesce(completed_at, data_interval_end) as date)) <= 2
                then 'pass'
            when datediff(current_date(), cast(coalesce(completed_at, data_interval_end) as date)) <= 7
                then 'warn'
            else 'fail'
        end as check_status,
        'warning' as severity,
        cast(datediff(current_date(), cast(coalesce(completed_at, data_interval_end) as date)) as string) as observed_value,
        'Latest metadata event completed within 2 days; 3-7 days is a warning.' as expected_rule,
        completed_at as latest_completed_at,
        latest_bronze_written_at
    from latest

    union all

    select
        latest.dataset_name,
        latest.dag_id,
        latest.data_interval_start,
        latest.data_interval_end,
        'interval_coverage' as check_name,
        case
            when daily_interval_coverage.first_daily_interval_start is null then 'warn'
            when daily_interval_coverage.missing_daily_interval_count = 0 then 'pass'
            when latest.dataset_name = 'evictions' then 'warn'
            else 'fail'
        end as check_status,
        case
            when daily_interval_coverage.first_daily_interval_start is null then 'warning'
            when latest.dataset_name = 'evictions' then 'warning'
            else 'critical'
        end as severity,
        case
            when daily_interval_coverage.first_daily_interval_start is null
                then 'daily interval baseline not established'
            when daily_interval_coverage.missing_daily_interval_count = 0
                then 'missing_daily_intervals=0'
            else concat(
                'missing_daily_intervals=',
                cast(daily_interval_coverage.missing_daily_interval_count as string),
                ', oldest_missing_interval_start=',
                cast(daily_interval_coverage.oldest_missing_daily_interval_start as string)
            )
        end as observed_value,
        case
            when daily_interval_coverage.first_daily_interval_start is null then
                'Coverage begins at the first observed daily interval; no daily baseline exists yet.'
            when latest.dataset_name = 'evictions' then
                'Every daily interval since the first observed daily interval is recorded after a 30-hour grace period; missing eviction intervals are warnings because later full snapshots restore current content.'
            else
                'Every daily interval since the first observed daily interval is recorded after a 30-hour grace period; wide genesis full-load intervals are excluded.'
        end as expected_rule,
        latest.completed_at as latest_completed_at,
        latest.latest_bronze_written_at
    from latest
    left join daily_interval_coverage
        on latest.dataset_name = daily_interval_coverage.dataset_name
)

select
    dataset_name,
    dag_id,
    data_interval_start,
    data_interval_end,
    check_name,
    check_status,
    severity,
    observed_value,
    expected_rule,
    latest_completed_at,
    latest_bronze_written_at,
    current_timestamp() as checked_at
from checks
