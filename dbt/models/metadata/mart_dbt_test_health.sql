{{ config(materialized='table') }}

with test_results as (
    select * from {{ ref('elementary', 'elementary_test_results') }}
),

with_windows as (
    select
        detected_at::date                                      as run_date,
        invocation_id                                          as run_id,
        test_name,
        table_name                                             as model_name,
        column_name,
        coalesce(test_sub_type, test_type, 'unknown')          as test_type,
        lower(status)                                          as status,
        lower(status) = 'pass'                                 as is_passing,
        coalesce(failures::integer, 0)                         as failures,
        round(
            avg((lower(status) = 'pass')::integer) over (
                partition by test_name
                order by detected_at::date
                rows between 6 preceding and current row
            ) * 100.0, 1
        )                                                      as pass_rate_7d,
        round(
            avg((lower(status) = 'pass')::integer) over (
                partition by test_name
                order by detected_at::date
                rows between 29 preceding and current row
            ) * 100.0, 1
        )                                                      as pass_rate_30d,
        max(case when lower(status) != 'pass' then detected_at end) over (
            partition by test_name
        )                                                      as last_failure_at
    from test_results
)

select * from with_windows
