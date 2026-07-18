"""Backfill RAW_ANALYST_REPORTS — analyst reports per symbol over a date range."""

import os
from pathlib import Path

import pandas as pd

from scripts.historical_backfill import writer
from scripts.historical_backfill.gap_log import GapLogger
from src.ingest.client.fireant_client import FireAntClient

TABLE_NAME = "analyst_reports"


def run(
    symbols: list[str],
    start_date: str,
    end_date: str,
    output_dir: Path,
    gap_logger: GapLogger,
) -> None:
    """Fetch analyst reports for each symbol over [start_date, end_date]."""
    client = FireAntClient(
        email=os.environ["FIREANT_EMAIL"], password=os.environ["FIREANT_PASSWORD"]
    )

    for symbol in symbols:
        if writer.is_done(output_dir, TABLE_NAME, symbol):
            continue

        reports = client.get_reports(
            symbol=symbol, start_date=start_date, end_date=end_date
        )
        if not reports:
            gap_logger.log(
                TABLE_NAME, symbol, f"{start_date}..{end_date}", "empty API response"
            )
            writer.mark_done(output_dir, TABLE_NAME, symbol)
            continue

        rows = [
            {
                "report_id": r.get("reportID"),
                "ticker": r.get("symbol") or symbol,
                "brokerage_firm": r.get("sourceName"),
                "publish_date": r.get("date"),
                "title": r.get("title"),
                "description": r.get("description"),
                "file_name": r.get("fileName"),
            }
            for r in reports
        ]
        df = pd.DataFrame(rows)
        df["publish_date"] = df["publish_date"].astype(str).str.slice(0, 10)

        for publish_date, group in df.groupby("publish_date"):
            writer.append_csv(output_dir, TABLE_NAME, publish_date, group)

        writer.mark_done(output_dir, TABLE_NAME, symbol)
