"""Backfill RAW_STOCK_PRICE_EOD — daily EOD prices per symbol over a date range."""

from pathlib import Path

from scripts.historical_backfill import writer
from scripts.historical_backfill.config import VNSTOCK_REQUEST_DELAY_SECONDS
from scripts.historical_backfill.gap_log import GapLogger
from src.ingest.client.vnstock_client import VnStockClient

TABLE_NAME = "stock_price_eod"


def run(
    symbols: list[str],
    start_date: str,
    end_date: str,
    output_dir: Path,
    gap_logger: GapLogger,
) -> None:
    """Fetch EOD prices for each symbol over [start_date, end_date] as daily CSVs."""
    client = VnStockClient(request_delay_seconds=VNSTOCK_REQUEST_DELAY_SECONDS)

    for symbol in symbols:
        if writer.is_done(output_dir, TABLE_NAME, symbol):
            continue

        df = client.get_stock_price_eod(
            symbol=symbol, start_date=start_date, end_date=end_date
        )
        if df.empty:
            gap_logger.log(
                TABLE_NAME, symbol, f"{start_date}..{end_date}", "empty API response"
            )
            writer.mark_done(output_dir, TABLE_NAME, symbol)
            continue

        df = df.rename(columns={"time": "trading_date"})
        df["ticker"] = symbol
        if "value" not in df.columns:
            df["value"] = None
        if "adjusted_close" not in df.columns:
            df["adjusted_close"] = None
        df["trading_date"] = df["trading_date"].astype(str).str.slice(0, 10)

        for trading_date, group in df.groupby("trading_date"):
            writer.append_csv(output_dir, TABLE_NAME, trading_date, group)

        writer.mark_done(output_dir, TABLE_NAME, symbol)
