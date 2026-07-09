select
    * replace (cast(filed_month as date) as filed_month)
from read_parquet('sources/sf_urban_health/data/housing_production.parquet')
