"""Backfill RAW_FOREIGN_TRADING — daily foreign trading flow per symbol over a range."""

import os
from pathlib import Path

import pandas as pd

from scripts.historical_backfill import writer
from scripts.historical_backfill.gap_log import GapLogger
from src.ingest.client.fireant_client import FireAntClient

TABLE_NAME = "foreign_trading"


def run(
    symbols: list[str],
    start_date: str,
    end_date: str,
    output_dir: Path,
    gap_logger: GapLogger,
) -> None:
    """Fetch foreign trading flow for each symbol over [start_date, end_date]."""
    client = FireAntClient(
        email=os.environ["FIREANT_EMAIL"], password=os.environ["FIREANT_PASSWORD"]
    )

    for symbol in symbols:
        if writer.is_done(output_dir, TABLE_NAME, symbol):
            continue

        quotes = client.get_historical_quotes(symbol, start_date, end_date)
        if not quotes:
            gap_logger.log(
                TABLE_NAME, symbol, f"{start_date}..{end_date}", "empty API response"
            )
            writer.mark_done(output_dir, TABLE_NAME, symbol)
            continue

        # FireAnt returns: date, buyForeignQuantity, buyForeignValue,
        # sellForeignQuantity, sellForeignValue
        rows = []
        for q in quotes:
            rows.append(
                {
                    "ticker": symbol,
                    "trading_date": q.get("date", "")[:10],
                    "buyForeignQuantity": q.get("buyForeignQuantity", 0),
                    "buyForeignValue": q.get("buyForeignValue", 0),
                    "sellForeignQuantity": q.get("sellForeignQuantity", 0),
                    "sellForeignValue": q.get("sellForeignValue", 0),
                }
            )

        df = pd.DataFrame(rows)
        if df.empty:
            writer.mark_done(output_dir, TABLE_NAME, symbol)
            continue

        for trading_date, group in df.groupby("trading_date"):
            writer.append_csv(output_dir, TABLE_NAME, trading_date, group)

        writer.mark_done(output_dir, TABLE_NAME, symbol)
