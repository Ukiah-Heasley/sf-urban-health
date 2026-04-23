{#
    Normalizes a neighborhood string to a consistent casing and handles the
    DataSF "unknown" / null pattern. Used by every mart that groups by
    neighborhood so taxonomy stays consistent across domains (permits, 311,
    transit).
#}
{% macro normalize_neighborhood(column_name) %}
    case
        when {{ column_name }} is null then 'Unknown'
        when trim({{ column_name }}) = '' then 'Unknown'
        when lower(trim({{ column_name }})) in ('none', 'n/a', 'unknown') then 'Unknown'
        else initcap(trim({{ column_name }}))
    end
{% endmacro %}
