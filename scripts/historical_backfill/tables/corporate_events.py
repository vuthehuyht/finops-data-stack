"""Backfill RAW_CORPORATE_EVENTS — best-effort corporate events per symbol."""

from pathlib import Path

from vnstock import Company

from scripts.historical_backfill import writer
from scripts.historical_backfill.gap_log import GapLogger
from src.ingest.client.vnstock_client import VnStockClient

TABLE_NAME = "corporate_events"

_RENAME_MAP = {
    "id": "event_id",
    "category": "event_type",
    "exright_date": "ex_right_date",
    "event_title_vi": "event_details",
}


def run(
    symbols: list[str],
    start_date: str,
    end_date: str,
    output_dir: Path,
    gap_logger: GapLogger,
) -> None:
    """Fetch whatever event history the source returns per symbol, filtered to range."""
    client = VnStockClient()

    for symbol in symbols:
        if writer.is_done(output_dir, TABLE_NAME, symbol):
            continue

        df = client.call_api_with_retry(
            lambda s=symbol: Company(source="VCI", symbol=s).events()
        )
        if df.empty:
            gap_logger.log(
                TABLE_NAME, symbol, f"{start_date}..{end_date}", "empty API response"
            )
            writer.mark_done(output_dir, TABLE_NAME, symbol)
            continue

        df = df.rename(columns=_RENAME_MAP)
        if "ticker" not in df.columns:
            df["ticker"] = symbol
        else:
            df["ticker"] = df["ticker"].fillna(symbol)

        if "ex_right_date" not in df.columns:
            gap_logger.log(
                TABLE_NAME,
                symbol,
                f"{start_date}..{end_date}",
                "no ex_right_date column",
            )
            writer.mark_done(output_dir, TABLE_NAME, symbol)
            continue

        df["ex_right_date"] = df["ex_right_date"].astype(str).str.slice(0, 10)
        df = df[(df["ex_right_date"] >= start_date) & (df["ex_right_date"] <= end_date)]
        if df.empty:
            gap_logger.log(
                TABLE_NAME,
                symbol,
                f"{start_date}..{end_date}",
                "source only returned events outside requested range",
            )
            writer.mark_done(output_dir, TABLE_NAME, symbol)
            continue

        for ex_right_date, group in df.groupby("ex_right_date"):
            writer.append_csv(output_dir, TABLE_NAME, ex_right_date, group)

        writer.mark_done(output_dir, TABLE_NAME, symbol)
