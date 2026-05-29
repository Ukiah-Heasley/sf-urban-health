{{ config(materialized='table') }}

with dataset_map as (
    -- static mapping: dag_id → display name and staging model name
    select *
    from (values
        ('ingest_permits',   'Permits',   'stg_permits'),
        ('ingest_evictions', 'Evictions', 'stg_evictions'),
        ('ingest_incidents', 'Incidents', 'stg_incidents')
    ) as t(dag_id, dataset_name, model_name)
),

last_load as (
    select
        dag_id,
        max(run_date) as last_loaded_date
    from {{ ref('mart_pipeline_health') }}
    where successful_runs > 0
    group by dag_id
),

-- aggregate test health per staging model over the last 7 days
test_health as (
    select
        model_name,
        round(avg(pass_rate_7d), 1)                                         as test_pass_rate_7d,
        count(distinct {{ dbt_utils.generate_surrogate_key(['run_date', 'test_name']) }}) as total_tests_7d,
        count_if(not is_passing and run_date >= current_date - 6)           as failed_tests_7d,
        max(last_failure_at)                                                as last_test_failure_at
    from {{ ref('mart_dbt_test_health') }}
    where run_date >= current_date - 6
    group by model_name
),

joined as (
    select
        dm.dataset_name,
        dm.dag_id,
        dm.model_name,
        coalesce(ll.last_loaded_date, '1970-01-01'::date)              as last_loaded_date,
        datediff('day', ll.last_loaded_date, current_date)             as days_since_last_load,
        case
            when ll.last_loaded_date is null                                          then 'critical'
            when datediff('day', ll.last_loaded_date, current_date) <= 1             then 'fresh'
            when datediff('day', ll.last_loaded_date, current_date) <= 2             then 'stale'
            else                                                                          'critical'
        end                                                                            as freshness_status,
        coalesce(th.test_pass_rate_7d, 100.0)                                         as test_pass_rate_7d,
        coalesce(th.total_tests_7d, 0)                                                as total_tests_7d,
        coalesce(th.failed_tests_7d, 0)                                               as failed_tests_7d,
        th.last_test_failure_at
    from dataset_map dm
    left join last_load ll  on dm.dag_id     = ll.dag_id
    left join test_health th on dm.model_name = th.model_name
),

scored as (
    select
        *,
        -- trust score: 40% freshness weight + 60% test pass rate
        round(
            0.4 * case freshness_status
                    when 'fresh'    then 100
                    when 'stale'    then 50
                    else                 0
                  end
            + 0.6 * test_pass_rate_7d,
            0
        )::integer                                                                     as trust_score
    from joined
)

select
    *,
    case
        when trust_score >= 90 then 'trusted'
        when trust_score >= 70 then 'degraded'
        else                        'untrusted'
    end                                                                                as trust_status
from scored
