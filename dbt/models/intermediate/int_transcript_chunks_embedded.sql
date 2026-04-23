{{
    config(
        materialized='incremental',
        unique_key='chunk_id',
        on_schema_change='append_new_columns'
    )
}}

-- Incremental so Cortex EMBED_TEXT_768 only fires for new chunks.
-- Embeddings consume Cortex credits; regenerating on every build is wasteful.

with chunks as (
    select * from {{ ref('stg_transcript_chunks') }}

    {% if is_incremental() %}
        where chunk_id not in (select chunk_id from {{ this }})
    {% endif %}
)

select
    chunk_id,
    meeting_date,
    meeting_type,
    agenda_item,
    title,
    text,
    source_url,
    snowflake.cortex.embed_text_768('e5-base-v2', text) as embedding,
    current_timestamp() as _embedded_at
from chunks
