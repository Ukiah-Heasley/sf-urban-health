with evictions as (
    select * from {{ ref('evictions_current') }}
),

monthly as (
    select
        cast(date_trunc('month', filed_at) as date) as filed_month,
        {{ normalize_neighborhood('neighborhood') }} as neighborhood,
        supervisor_district,
        eviction_type,
        count(*) as eviction_count,
        count_if(ellis_act_withdrawal) as ellis_act_count,
        count_if(owner_move_in) as owner_move_in_count,
        count_if(non_payment) as non_payment_count
    from evictions
    where filed_at is not null
    group by 1, 2, 3, 4
)

select * from monthly
