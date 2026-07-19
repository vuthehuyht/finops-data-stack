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
        """Merge recorded gaps into {output_dir}/_gap_report.csv, return its path.

        Resumed runs skip (table, symbol) pairs already marked done, so a
        gap logged in an earlier run would otherwise be silently dropped
        when a later run overwrites the report. Merging with whatever is
        already on disk keeps the report a true union across every run.
        """
        path = output_dir / "_gap_report.csv"
        path.parent.mkdir(parents=True, exist_ok=True)

        new_df = pd.DataFrame(self._rows, columns=_COLUMNS)
        if path.exists():
            existing_df = pd.read_csv(path)
            combined = pd.concat([existing_df, new_df], ignore_index=True)
        else:
            combined = new_df

        combined = combined.drop_duplicates(
            subset=["table", "symbol"], keep="last"
        ).sort_values(["table", "symbol"])
        combined.to_csv(path, index=False)
        return path
