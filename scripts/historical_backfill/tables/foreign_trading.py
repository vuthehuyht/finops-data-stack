"""Backfill RAW_FOREIGN_TRADING — daily foreign trading flow per symbol over a range."""

from pathlib import Path

from scripts.historical_backfill import writer
from scripts.historical_backfill.config import VNSTOCK_REQUEST_DELAY_SECONDS
from scripts.historical_backfill.gap_log import GapLogger
from src.ingest.client.vnstock_client import VnStockClient

TABLE_NAME = "foreign_trading"


def run(
    symbols: list[str],
    start_date: str,
    end_date: str,
    output_dir: Path,
    gap_logger: GapLogger,
) -> None:
    """Fetch foreign trading flow for each symbol over [start_date, end_date]."""
    client = VnStockClient(request_delay_seconds=VNSTOCK_REQUEST_DELAY_SECONDS)

    for symbol in symbols:
        if writer.is_done(output_dir, TABLE_NAME, symbol):
            continue

        try:
            stock_obj = client.client.stock(symbol=symbol, source="TCBS")
        except ValueError as e:
            gap_logger.log(
                TABLE_NAME,
                symbol,
                f"{start_date}..{end_date}",
                f"source unavailable: {e}",
            )
            writer.mark_done(output_dir, TABLE_NAME, symbol)
            continue
        if not hasattr(stock_obj, "trading"):
            gap_logger.log(
                TABLE_NAME, symbol, f"{start_date}..{end_date}", "no trading endpoint"
            )
            writer.mark_done(output_dir, TABLE_NAME, symbol)
            continue

        df = client.call_api_with_retry(
            stock_obj.trading.foreign_flow, start=start_date, end=end_date
        )
        if df.empty:
            gap_logger.log(
                TABLE_NAME, symbol, f"{start_date}..{end_date}", "empty API response"
            )
            writer.mark_done(output_dir, TABLE_NAME, symbol)
            continue

        if "ticker" not in df.columns and "TICKER" not in df.columns:
            df["ticker"] = symbol
        date_col = "trading_date" if "trading_date" in df.columns else "date"
        df[date_col] = df[date_col].astype(str).str.slice(0, 10)

        for trading_date, group in df.groupby(date_col):
            writer.append_csv(output_dir, TABLE_NAME, trading_date, group)

        writer.mark_done(output_dir, TABLE_NAME, symbol)
