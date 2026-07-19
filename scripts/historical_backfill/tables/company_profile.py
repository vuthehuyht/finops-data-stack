"""Backfill RAW_COMPANY_PROFILE — current snapshot per symbol (no historical range)."""

from pathlib import Path

from vnstock import Company

from scripts.historical_backfill import writer
from scripts.historical_backfill.config import VNSTOCK_REQUEST_DELAY_SECONDS
from scripts.historical_backfill.gap_log import GapLogger
from src.ingest.client.vnstock_client import VnStockClient

TABLE_NAME = "company_profile"

_RENAME_MAP = {
    "symbol": "ticker",
    "organ_name": "company_name",
    "sector": "industry",
    "com_group_code": "exchange",
    "company_profile": "description",
}
_KEEP_COLS = ["ticker", "company_name", "industry", "exchange", "description"]


def run(
    symbols: list[str],
    start_date: str,
    end_date: str,
    output_dir: Path,
    gap_logger: GapLogger,
) -> None:
    """Fetch the current company profile snapshot for each symbol, once each."""
    client = VnStockClient(request_delay_seconds=VNSTOCK_REQUEST_DELAY_SECONDS)

    for symbol in symbols:
        if writer.is_done(output_dir, TABLE_NAME, symbol):
            continue

        df = client.call_api_with_retry(
            lambda s=symbol: Company(source="VCI", symbol=s).info()
        )
        if df.empty:
            gap_logger.log(TABLE_NAME, symbol, "snapshot", "empty API response")
            writer.mark_done(output_dir, TABLE_NAME, symbol)
            continue

        df = df.rename(columns=_RENAME_MAP)
        keep_cols = [c for c in _KEEP_COLS if c in df.columns]
        df = df[keep_cols]

        writer.append_csv(output_dir, TABLE_NAME, "snapshot", df)
        writer.mark_done(output_dir, TABLE_NAME, symbol)
