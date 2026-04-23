with source as (
    select
        $1 as payload,
        _loaded_at
    from {{ source('raw', 'transcript_chunks') }}
),

unpacked as (
    select
        payload:chunk_id::string           as chunk_id,
        payload:meeting_date::date         as meeting_date,
        payload:meeting_type::string       as meeting_type,
        payload:agenda_item::string        as agenda_item,
        payload:title::string              as title,
        payload:text::string               as text,
        payload:source_url::string         as source_url,
        _loaded_at
    from source
)

select *
from unpacked
where chunk_id is not null
  and text is not null
