{{ config(materialized='table') }}

-- BI/agent-facing semantic layer. Retrievers embed a query via Cortex and run
-- VECTOR_COSINE_SIMILARITY against the `embedding` column here.

select
    chunk_id,
    meeting_date,
    meeting_type,
    agenda_item,
    title,
    text,
    source_url,
    embedding
from {{ ref('int_transcript_chunks_embedded') }}
