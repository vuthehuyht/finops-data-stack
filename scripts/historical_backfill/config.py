"""Shared configuration for the historical backfill scripts."""

import datetime

from src.ingest.pipeline.base import DEFAULT_TICKER_SYMBOLS

__all__ = [
    "DEFAULT_TICKER_SYMBOLS",
    "VNSTOCK_REQUEST_DELAY_SECONDS",
    "default_date_range",
]

# vnstock hard-exits the whole process (not a catchable exception) once a
# rate ceiling is breached. With a registered API key (free tier: 60 req/min,
# see `vnstock.register_user()`) 1.5s spacing keeps us at ~40 req/min, well
# under the ceiling even accounting for extra internal calls per symbol.
VNSTOCK_REQUEST_DELAY_SECONDS = 1.5


def default_date_range() -> tuple[str, str]:
    """Return (start_date, end_date) ISO strings spanning the last 5 years."""
    end = datetime.date.today()
    start = end - datetime.timedelta(days=5 * 365)
    return start.isoformat(), end.isoformat()
