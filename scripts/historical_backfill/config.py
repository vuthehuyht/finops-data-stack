"""Shared configuration for the historical backfill scripts."""

import datetime

from src.ingest.pipeline.base import DEFAULT_TICKER_SYMBOLS

__all__ = ["DEFAULT_TICKER_SYMBOLS", "default_date_range"]


def default_date_range() -> tuple[str, str]:
    """Return (start_date, end_date) ISO strings spanning the last 5 years."""
    end = datetime.date.today()
    start = end - datetime.timedelta(days=5 * 365)
    return start.isoformat(), end.isoformat()
