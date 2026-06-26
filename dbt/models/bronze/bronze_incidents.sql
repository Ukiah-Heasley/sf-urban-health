{{ config(
    materialized='ephemeral',
    tags=['lakehouse', 'bronze']
) }}

{% set bucket = env_var('LAKEHOUSE_BUCKET', 'lakehouse') %}
{% set bronze_prefix = 's3a://' ~ bucket ~ '/lake/parquet/bronze/incidents/' %}

select * from parquet.`{{ bronze_prefix }}`
