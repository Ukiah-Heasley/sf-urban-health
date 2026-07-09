select
    * replace (
        cast(data_interval_start as timestamp) as data_interval_start,
        cast(data_interval_end as timestamp) as data_interval_end,
        cast(latest_completed_at as timestamp) as latest_completed_at,
        cast(latest_bronze_written_at as timestamp) as latest_bronze_written_at,
        cast(checked_at as timestamp) as checked_at
    )
from read_parquet('sources/sf_urban_health/data/data_trust.parquet')
