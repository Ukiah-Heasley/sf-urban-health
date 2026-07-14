{{ config(severity='error', tags=['lakehouse']) }}

select *
from {{ ref('data_trust') }}
where check_name = 'interval_coverage'
  and dataset_name in ('permits', 'incidents')
  and check_status = 'fail'
