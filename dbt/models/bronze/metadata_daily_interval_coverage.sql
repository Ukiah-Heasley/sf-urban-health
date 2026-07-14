{{ config(
    materialized='ephemeral',
    tags=['lakehouse', 'metadata']
) }}

{% set coverage_cutoff_epoch_override = env_var('DBT_INTERVAL_COVERAGE_CUTOFF_EPOCH', '') %}

with ingest_runs as (
    select * from {{ ref('metadata_ingest_runs') }}
),

coverage_cutoff as (
    select
        {% if coverage_cutoff_epoch_override %}
            cast({{ coverage_cutoff_epoch_override }} as bigint)
        {% else %}
            unix_timestamp(current_timestamp() - interval 30 hours)
        {% endif %}
        as coverage_cutoff_epoch
),

dataset_names as (
    select distinct dataset_name
    from ingest_runs
),

observed_daily_intervals as (
    select distinct
        dataset_name,
        unix_timestamp(data_interval_start) as data_interval_start_epoch
    from ingest_runs
    where unix_timestamp(data_interval_end) - unix_timestamp(data_interval_start) = 86400
),

daily_anchors as (
    select
        dataset_name,
        min(data_interval_start_epoch) as first_daily_interval_start_epoch
    from observed_daily_intervals
    group by 1
),

eligible_anchors as (
    select
        daily_anchors.dataset_name,
        daily_anchors.first_daily_interval_start_epoch,
        coverage_cutoff.coverage_cutoff_epoch
    from daily_anchors
    cross join coverage_cutoff
    where daily_anchors.first_daily_interval_start_epoch + 86400
        <= coverage_cutoff.coverage_cutoff_epoch
),

expected_daily_intervals as (
    select
        eligible_anchors.dataset_name,
        expected_intervals.expected_interval_start_epoch
    from eligible_anchors
    lateral view explode(
        sequence(
            first_daily_interval_start_epoch,
            coverage_cutoff_epoch - 86400,
            86400
        )
    ) expected_intervals as expected_interval_start_epoch
),

missing_daily_intervals as (
    select
        expected.dataset_name,
        expected.expected_interval_start_epoch
    from expected_daily_intervals as expected
    left anti join observed_daily_intervals as observed
        on expected.dataset_name = observed.dataset_name
       and expected.expected_interval_start_epoch = observed.data_interval_start_epoch
),

interval_coverage as (
    select
        dataset_names.dataset_name,
        daily_anchors.first_daily_interval_start_epoch,
        coverage_cutoff.coverage_cutoff_epoch,
        count(missing_daily_intervals.expected_interval_start_epoch) as missing_daily_interval_count,
        min(missing_daily_intervals.expected_interval_start_epoch) as oldest_missing_daily_interval_start_epoch
    from dataset_names
    cross join coverage_cutoff
    left join daily_anchors
        on dataset_names.dataset_name = daily_anchors.dataset_name
    left join missing_daily_intervals
        on dataset_names.dataset_name = missing_daily_intervals.dataset_name
    group by 1, 2, 3
)

select
    dataset_name,
    timestamp_seconds(first_daily_interval_start_epoch) as first_daily_interval_start,
    timestamp_seconds(coverage_cutoff_epoch) as coverage_cutoff,
    missing_daily_interval_count,
    timestamp_seconds(oldest_missing_daily_interval_start_epoch) as oldest_missing_daily_interval_start
from interval_coverage
