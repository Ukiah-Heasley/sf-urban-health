with source as (
    select
        $1 as payload,
        _loaded_at
    from {{ source('raw', 'evictions') }}
),

deduped as (
    select *,
        row_number() over (
            partition by payload:eviction_id::string
            order by _loaded_at desc
        ) as rn
    from source
),

latest as (
    select * from deduped where rn = 1
),

unpacked as (
    select
        payload:eviction_id::string                                as eviction_id,
        payload:file_date::date                                    as filed_at,
        payload:address::string                                    as address,
        payload:zip::string                                        as zipcode,
        payload:supervisor_district::string                        as supervisor_district,
        payload:neighborhood::string                               as neighborhood,
        -- at-fault reason flags
        coalesce(payload:non_payment::boolean, false)              as non_payment,
        coalesce(payload:breach::boolean, false)                   as breach,
        coalesce(payload:nuisance::boolean, false)                 as nuisance,
        coalesce(payload:illegal_use::boolean, false)              as illegal_use,
        coalesce(payload:failure_to_sign_renewal::boolean, false)  as failure_to_sign_renewal,
        coalesce(payload:access_denial::boolean, false)            as access_denial,
        coalesce(payload:unapproved_subtenant::boolean, false)     as unapproved_subtenant,
        coalesce(payload:late_payments::boolean, false)            as late_payments,
        coalesce(payload:roommate_same_unit::boolean, false)       as roommate_same_unit,
        coalesce(payload:other_cause::boolean, false)              as other_cause,
        -- no-fault reason flags
        coalesce(payload:owner_move_in::boolean, false)            as owner_move_in,
        coalesce(payload:demolition::boolean, false)               as demolition,
        coalesce(payload:capital_improvement::boolean, false)      as capital_improvement,
        coalesce(payload:substantial_rehab::boolean, false)        as substantial_rehab,
        coalesce(payload:ellis_act_withdrawal::boolean, false)     as ellis_act_withdrawal,
        coalesce(payload:condo_conversion::boolean, false)         as condo_conversion,
        coalesce(payload:lead_remediation::boolean, false)         as lead_remediation,
        coalesce(payload:development::boolean, false)              as development,
        coalesce(payload:good_samaritan_ends::boolean, false)      as good_samaritan_ends,
        -- derived: no-fault if any no-fault flag is set, else at-fault
        case
            when coalesce(payload:owner_move_in::boolean, false)
              or coalesce(payload:demolition::boolean, false)
              or coalesce(payload:capital_improvement::boolean, false)
              or coalesce(payload:substantial_rehab::boolean, false)
              or coalesce(payload:ellis_act_withdrawal::boolean, false)
              or coalesce(payload:condo_conversion::boolean, false)
              or coalesce(payload:lead_remediation::boolean, false)
              or coalesce(payload:development::boolean, false)
              or coalesce(payload:good_samaritan_ends::boolean, false)
            then 'no_fault'
            else 'at_fault'
        end                                                        as eviction_type,
        _loaded_at
    from latest
)

select *
from unpacked
where eviction_id is not null
