"""Resume-safe, append-only CSV writer for the historical backfill scripts."""

from pathlib import Path

import pandas as pd


def marker_path(output_dir: Path, table: str, key: str) -> Path:
    """Return the completion marker file path for a (table, key) unit of work."""
    safe_key = key.replace("/", "_")
    return output_dir / table / ".markers" / f"{safe_key}.done"


def is_done(output_dir: Path, table: str, key: str) -> bool:
    """Return True if (table, key) was already fully processed in a prior run."""
    return marker_path(output_dir, table, key).exists()


def mark_done(output_dir: Path, table: str, key: str) -> None:
    """Create the completion marker file for (table, key)."""
    path = marker_path(output_dir, table, key)
    path.parent.mkdir(parents=True, exist_ok=True)
    path.touch()


def append_csv(output_dir: Path, table: str, filename: str, df: pd.DataFrame) -> None:
    """Append df rows to {output_dir}/{table}/{filename}.csv, creating it if needed."""
    if df.empty:
        return
    path = output_dir / table / f"{filename}.csv"
    path.parent.mkdir(parents=True, exist_ok=True)
    write_header = not path.exists()
    df.to_csv(path, mode="a", header=write_header, index=False)
