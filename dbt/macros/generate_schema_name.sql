{#
  Routes models to schemas based on dbt target:
    - prod: bare names (staging, intermediate, marts)
    - dev (or any non-prod target): prefixed with the personal schema
      (e.g. dbt_ukiah_staging, dbt_ukiah_marts) so devs don't trample prod or each other
#}
{% macro generate_schema_name(custom_schema_name, node) -%}
    {%- if target.name == 'prod' or custom_schema_name is none -%}
        {{ (custom_schema_name or target.schema) | trim }}
    {%- else -%}
        {{ target.schema }}_{{ custom_schema_name | trim }}
    {%- endif -%}
{%- endmacro %}
