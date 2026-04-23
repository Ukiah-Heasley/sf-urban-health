with source as (
    select
        $1 as payload,
        _loaded_at
    from {{ source('raw', 'permits') }}
),

unpacked as (
    select
        payload:permit_number::string                          as permit_number,
        payload:permit_type::string                            as permit_type_code,
        payload:permit_type_definition::string                 as permit_type,
        payload:current_status::string                         as current_status,
        payload:filed_date::timestamp_ntz                      as filed_at,
        payload:issued_date::timestamp_ntz                     as issued_at,
        payload:completed_date::timestamp_ntz                  as completed_at,
        payload:first_construction_document_date::timestamp_ntz as first_construction_doc_at,
        payload:current_status_date::timestamp_ntz             as current_status_at,
        payload:estimated_cost::number(18, 2)                  as estimated_cost,
        payload:revised_cost::number(18, 2)                    as revised_cost,
        payload:existing_units::integer                        as existing_units,
        payload:proposed_units::integer                        as proposed_units,
        payload:number_of_existing_stories::integer            as existing_stories,
        payload:number_of_proposed_stories::integer            as proposed_stories,
        payload:street_number::string                          as street_number,
        payload:street_name::string                            as street_name,
        payload:zipcode::string                                as zipcode,
        payload:supervisor_district::string                    as supervisor_district,
        payload:neighborhoods_analysis_boundaries::string      as neighborhood,
        payload:existing_use::string                           as existing_use,
        payload:proposed_use::string                           as proposed_use,
        _loaded_at
    from source
)

select *
from unpacked
where permit_number is not null
