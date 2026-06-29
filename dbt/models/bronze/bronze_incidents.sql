{{ config(
    materialized='ephemeral',
    tags=['lakehouse', 'bronze']
) }}

{% set bronze_base_uri = env_var('LAKEHOUSE_BRONZE_BASE_URI', 's3a://lakehouse/lake/parquet/bronze') %}
{% set bronze_prefix = bronze_base_uri.rstrip('/') ~ '/incidents/' %}

select * from parquet.`{{ bronze_prefix }}`
