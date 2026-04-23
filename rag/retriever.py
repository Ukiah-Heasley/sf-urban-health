"""Semantic retrieval over BOS meeting minutes.

Embeds a query via Snowflake Cortex and ranks chunks in MARTS.mart_transcript_search
by cosine similarity. Returns Python dicts; callers decide how to surface them
(CLI, notebook, agent tool, future dashboard).

Skeleton — to be implemented.
"""
from __future__ import annotations

from dataclasses import dataclass
from datetime import date


@dataclass
class RetrievalHit:
    chunk_id: str
    meeting_date: date
    meeting_type: str
    title: str | None
    text: str
    source_url: str
    score: float


RETRIEVAL_SQL = """
select
    chunk_id,
    meeting_date,
    meeting_type,
    title,
    text,
    source_url,
    vector_cosine_similarity(
        embedding,
        snowflake.cortex.embed_text_768('e5-base-v2', %(query)s)
    ) as score
from {database}.marts.mart_transcript_search
where (%(meeting_type)s is null or meeting_type = %(meeting_type)s)
  and (%(date_from)s is null or meeting_date >= %(date_from)s)
  and (%(date_to)s is null or meeting_date <= %(date_to)s)
order by score desc
limit %(top_k)s
"""


def search(
    query: str,
    top_k: int = 5,
    meeting_type: str | None = None,
    date_from: date | None = None,
    date_to: date | None = None,
) -> list[RetrievalHit]:
    """Run semantic search against mart_transcript_search.

    TODO: open a Snowflake connection (reuse Airflow conn or snowflake-connector-python
    with env creds), execute RETRIEVAL_SQL, map rows to RetrievalHit.
    """
    raise NotImplementedError
