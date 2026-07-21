"""Backfill RAW_INDEX_PRICE_EOD — daily market index prices over a date range."""

from pathlib import Path

from scripts.historical_backfill import writer
from scripts.historical_backfill.config import VNSTOCK_REQUEST_DELAY_SECONDS
from scripts.historical_backfill.gap_log import GapLogger
from src.ingest.client.vnstock_client import VnStockClient

TABLE_NAME = "index_price_eod"
DEFAULT_INDICES = ["VNINDEX", "HNXINDEX"]


def run(
    symbols: list[str],
    start_date: str,
    end_date: str,
    output_dir: Path,
    gap_logger: GapLogger,
) -> None:
    """Fetch daily index prices for the fixed VN market indices over the range."""
    client = VnStockClient(request_delay_seconds=VNSTOCK_REQUEST_DELAY_SECONDS)

    for index_name in DEFAULT_INDICES:
        if writer.is_done(output_dir, TABLE_NAME, index_name):
            continue

        df = client.get_stock_price_eod(
            symbol=index_name, start_date=start_date, end_date=end_date
        )
        if df.empty:
            gap_logger.log(
                TABLE_NAME,
                index_name,
                f"{start_date}..{end_date}",
                "empty API response",
            )
            writer.mark_done(output_dir, TABLE_NAME, index_name)
            continue

        df = df.rename(columns={"time": "trading_date"})
        df["index_name"] = index_name
        df["trading_date"] = df["trading_date"].astype(str).str.slice(0, 10)

        for trading_date, group in df.groupby("trading_date"):
            writer.append_csv(output_dir, TABLE_NAME, trading_date, group)

        writer.mark_done(output_dir, TABLE_NAME, index_name)
