"""Scrape SF Board of Supervisors meeting minutes PDFs and land them in S3.

Writes PDFs to s3://$AWS_S3_BUCKET/raw/transcripts/bos/YYYY/MM/<filename>.pdf
and emits one NDJSON record per chunk to raw/transcripts/bos/YYYY/MM/DD/chunks.json
for the COPY INTO that follows.

Skeleton — to be implemented.
"""
from __future__ import annotations

from dataclasses import dataclass
from datetime import date


BOS_MINUTES_LISTING = "https://sfbos.org/meeting-minutes"


@dataclass
class MeetingPDF:
    url: str
    meeting_date: date
    meeting_type: str  # "board_of_supervisors" for now


def list_new_pdfs(since: date) -> list[MeetingPDF]:
    """Scrape the BOS minutes listing and return PDFs dated >= since that we
    haven't seen yet. Dedupe against RAW.TRANSCRIPT_INGESTS in Snowflake.

    TODO: inspect sfbos.org page HTML, extract anchor hrefs + parse dates.
    """
    raise NotImplementedError


def download_pdf(pdf: MeetingPDF) -> bytes:
    """Fetch the PDF bytes. Retries on transient errors (mirror permits._session)."""
    raise NotImplementedError


def run(run_date: date | None = None, lookback_days: int = 90) -> str:
    """Entry point. Scrape → download → chunk → land in S3.

    Lookback is generous (90d) because BOS minutes post weeks after meetings.
    """
    raise NotImplementedError


if __name__ == "__main__":
    run()
