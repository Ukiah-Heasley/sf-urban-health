{{ config(
    materialized='ephemeral',
    tags=['lakehouse', 'metadata']
) }}

{% set bronze_base_uri = env_var('LAKEHOUSE_BRONZE_BASE_URI', 's3a://lakehouse/lake/parquet/bronze') %}
{% set default_metadata_base_uri = bronze_base_uri.rstrip('/') %}
{% if default_metadata_base_uri.endswith('/bronze') %}
    {% set default_metadata_base_uri = default_metadata_base_uri[:-7] ~ '/metadata' %}
{% else %}
    {% set default_metadata_base_uri = 's3a://lakehouse/lake/parquet/metadata' %}
{% endif %}
{% set metadata_base_uri = env_var('LAKEHOUSE_METADATA_BASE_URI', default_metadata_base_uri) %}
{% set file_manifest_prefix = metadata_base_uri.rstrip('/') ~ '/file_manifest/' %}

select * from parquet.`{{ file_manifest_prefix }}`
