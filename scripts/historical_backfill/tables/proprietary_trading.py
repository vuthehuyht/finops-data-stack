"""Backfill RAW_PROPRIETARY_TRADING — historical proprietary trading from FireAnt."""

import os
from pathlib import Path

import pandas as pd

from scripts.historical_backfill import writer
from scripts.historical_backfill.gap_log import GapLogger
from src.ingest.client.fireant_client import FireAntClient

TABLE_NAME = "proprietary_trading"


def run(
    symbols: list[str],
    start_date: str,
    end_date: str,
    output_dir: Path,
    gap_logger: GapLogger,
) -> None:
    """Fetch real historical proprietary trading rows per symbol from FireAnt."""
    client = FireAntClient(
        email=os.environ["FIREANT_EMAIL"], password=os.environ["FIREANT_PASSWORD"]
    )

    for symbol in symbols:
        if writer.is_done(output_dir, TABLE_NAME, symbol):
            continue

        quotes = client.get_historical_quotes(symbol, start_date, end_date)
        if not quotes:
            gap_logger.log(
                TABLE_NAME,
                symbol,
                f"{start_date}..{end_date}",
                "no prop trading history available",
            )
            writer.mark_done(output_dir, TABLE_NAME, symbol)
            continue

        rows = []
        for q in quotes:
            net_val = q.get("propTradingNetValue", 0)
            if net_val == 0:
                continue
            rows.append(
                {
                    "ticker": symbol,
                    "trading_date": q.get("date", "")[:10],
                    "buy_vol": "0",  # FireAnt only gives net value
                    "sell_vol": "0",  # FireAnt only gives net value
                    "net_val": str(net_val),
                }
            )

        df = pd.DataFrame(rows)
        if df.empty:
            gap_logger.log(
                TABLE_NAME,
                symbol,
                f"{start_date}..{end_date}",
                "no prop trading history available",
            )
            writer.mark_done(output_dir, TABLE_NAME, symbol)
            continue

        for trading_date, group in df.groupby("trading_date"):
            writer.append_csv(output_dir, TABLE_NAME, trading_date, group)

        writer.mark_done(output_dir, TABLE_NAME, symbol)
