"""Parse meeting-minutes PDFs into agenda-item chunks.

Chunks are the natural semantic unit of a government meeting: each agenda item
gets its own discussion, vote, and outcome. Fixed-token chunking would split
across these boundaries.

Skeleton — to be implemented.
"""
from __future__ import annotations

from dataclasses import dataclass
from datetime import date


@dataclass
class TranscriptChunk:
    chunk_id: str          # e.g. "bos_2026_03_15_item_4"
    meeting_date: date
    meeting_type: str
    agenda_item: str | None
    title: str | None
    text: str
    source_url: str


def parse_pdf(pdf_bytes: bytes) -> str:
    """Extract full text from a PDF. Uses pdfplumber.

    TODO: handle multi-column layouts and table-of-contents pages.
    """
    raise NotImplementedError


def split_by_agenda_item(
    full_text: str,
    meeting_date: date,
    meeting_type: str,
    source_url: str,
) -> list[TranscriptChunk]:
    """Split a minutes document into per-agenda-item chunks.

    Heuristic: regex on "Item N." or "N. <Title>" headers at the start of a line.
    Falls back to fixed-size chunks if no agenda structure detected (unusual
    formats, special sessions).

    TODO: implement regex + fallback.
    """
    raise NotImplementedError
