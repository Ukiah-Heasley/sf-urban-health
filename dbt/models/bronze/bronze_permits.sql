{{ config(
    materialized='ephemeral',
    tags=['lakehouse', 'bronze']
) }}

{% set bronze_base_uri = env_var('LAKEHOUSE_BRONZE_BASE_URI', 's3a://lakehouse/lake/parquet/bronze') %}
{% set bronze_prefix = bronze_base_uri.rstrip('/') ~ '/permits/' %}

select
    _ingest_run_id,
    _raw_s3_path,
    _raw_s3_key,
    _data_interval_start,
    _data_interval_end,
    _effective_start,
    _loaded_at,
    _extracted_at,
    _source_dataset_id,
    _record_hash,
    _raw_payload,
    permit_number,
    permit_type_code,
    permit_type,
    status,
    filed_at,
    issued_at,
    status_date,
    approved_at,
    last_activity_at,
    is_adu,
    estimated_cost,
    revised_cost,
    existing_units,
    proposed_units,
    cast(
        get_json_object(_raw_payload, '$.number_of_existing_stories') as double
    ) as existing_stories,
    cast(
        get_json_object(_raw_payload, '$.number_of_proposed_stories') as double
    ) as proposed_stories,
    street_number,
    street_name,
    zipcode,
    supervisor_district,
    neighborhood,
    existing_use,
    proposed_use
from parquet.`{{ bronze_prefix }}`
