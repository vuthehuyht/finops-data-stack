"""Gap tracking for source-limited history in the historical backfill scripts."""

from pathlib import Path

import pandas as pd

_COLUMNS = ["table", "symbol", "expected_range", "missing_reason"]


class GapLogger:
    """Accumulates gap records in memory and flushes them to disk once."""

    def __init__(self) -> None:
        """Initialize an empty in-memory gap record buffer."""
        self._rows: list[dict] = []

    def log(self, table: str, symbol: str, expected_range: str, reason: str) -> None:
        """Record a gap for (table, symbol) covering expected_range."""
        self._rows.append(
            {
                "table": table,
                "symbol": symbol,
                "expected_range": expected_range,
                "missing_reason": reason,
            }
        )

    def flush(self, output_dir: Path) -> Path:
        """Write all recorded gaps to {output_dir}/_gap_report.csv, return its path."""
        path = output_dir / "_gap_report.csv"
        path.parent.mkdir(parents=True, exist_ok=True)
        pd.DataFrame(self._rows, columns=_COLUMNS).to_csv(path, index=False)
        return path
