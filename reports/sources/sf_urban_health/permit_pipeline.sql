select
    * replace (cast(snapshot_date as date) as snapshot_date)
from read_parquet('sources/sf_urban_health/data/permit_pipeline.parquet')
