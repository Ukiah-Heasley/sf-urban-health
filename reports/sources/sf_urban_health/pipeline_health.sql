select
    * replace (
        cast(run_date as date) as run_date,
        cast(latest_completed_at as timestamp) as latest_completed_at,
        cast(latest_bronze_written_at as timestamp) as latest_bronze_written_at
    )
from read_parquet('sources/sf_urban_health/data/pipeline_health.parquet')
