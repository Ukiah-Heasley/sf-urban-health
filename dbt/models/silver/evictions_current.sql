with bronze as (
    select * from {{ ref('bronze_evictions') }}
),

ranked as (
    select
        *,
        row_number() over (
            partition by eviction_id
            order by
                _loaded_at desc,
                _extracted_at desc,
                _data_interval_end desc,
                _record_hash asc
        ) as _row_num
    from bronze
),

latest as (
    select * from ranked where _row_num = 1
)

select
    eviction_id,
    filed_at,
    address,
    zipcode,
    supervisor_district,
    neighborhood,
    case
        when coalesce(owner_move_in, false)
          or coalesce(demolition, false)
          or coalesce(capital_improvement, false)
          or coalesce(substantial_rehab, false)
          or coalesce(ellis_act_withdrawal, false)
          or coalesce(condo_conversion, false)
          or coalesce(lead_remediation, false)
          or coalesce(development, false)
          or coalesce(good_samaritan_ends, false)
        then 'no_fault'
        else 'at_fault'
    end as eviction_type,
    coalesce(non_payment, false) as non_payment,
    coalesce(breach, false) as breach,
    coalesce(nuisance, false) as nuisance,
    coalesce(illegal_use, false) as illegal_use,
    coalesce(failure_to_sign_renewal, false) as failure_to_sign_renewal,
    coalesce(access_denial, false) as access_denial,
    coalesce(unapproved_subtenant, false) as unapproved_subtenant,
    coalesce(late_payments, false) as late_payments,
    coalesce(roommate_same_unit, false) as roommate_same_unit,
    coalesce(other_cause, false) as other_cause,
    coalesce(owner_move_in, false) as owner_move_in,
    coalesce(demolition, false) as demolition,
    coalesce(capital_improvement, false) as capital_improvement,
    coalesce(substantial_rehab, false) as substantial_rehab,
    coalesce(ellis_act_withdrawal, false) as ellis_act_withdrawal,
    coalesce(condo_conversion, false) as condo_conversion,
    coalesce(lead_remediation, false) as lead_remediation,
    coalesce(development, false) as development,
    coalesce(good_samaritan_ends, false) as good_samaritan_ends,
    _loaded_at
from latest
where eviction_id is not null
