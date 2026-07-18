"""Backfill RAW_INTEREST_RATES — daily benchmark interest rates over a date range."""

from pathlib import Path

from scripts.historical_backfill import writer
from scripts.historical_backfill.gap_log import GapLogger
from src.ingest.client.yahoo_finance_client import YahooFinanceClient

TABLE_NAME = "interest_rates"

RATE_MAPPING: dict[str, str] = {
    "us_fed_funds_rate": "^IRX",
    "us_10y_treasury": "^TNX",
    "us_5y_treasury": "^FVX",
}


def run(
    symbols: list[str],
    start_date: str,
    end_date: str,
    output_dir: Path,
    gap_logger: GapLogger,
) -> None:
    """Fetch daily interest rate history for each benchmark over the date range."""
    client = YahooFinanceClient()

    for rate_type, ticker in RATE_MAPPING.items():
        if writer.is_done(output_dir, TABLE_NAME, rate_type):
            continue

        df = client.get_ticker_history(ticker, start_date, end_date)
        if df.empty:
            gap_logger.log(
                TABLE_NAME, rate_type, f"{start_date}..{end_date}", "empty API response"
            )
            writer.mark_done(output_dir, TABLE_NAME, rate_type)
            continue

        df = df.reset_index()
        df["date"] = df["Date"].astype(str).str.slice(0, 10)
        df["rate_type"] = rate_type
        df["rate_value"] = df["Close"].astype(str)
        df = df[["rate_type", "date", "rate_value"]]

        for row_date, group in df.groupby("date"):
            writer.append_csv(output_dir, TABLE_NAME, row_date, group)

        writer.mark_done(output_dir, TABLE_NAME, rate_type)
