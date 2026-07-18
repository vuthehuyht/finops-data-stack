"""Backfill RAW_EXCHANGE_RATES — daily currency exchange rates over a date range."""

from pathlib import Path

from scripts.historical_backfill import writer
from scripts.historical_backfill.gap_log import GapLogger
from src.ingest.client.yahoo_finance_client import YahooFinanceClient

TABLE_NAME = "exchange_rates"

CURRENCY_MAPPING: dict[str, str] = {
    "USD/VND": "USDVND=X",
    "EUR/VND": "EURVND=X",
    "GBP/VND": "GBPVND=X",
    "JPY/VND": "JPYVND=X",
    "CNY/VND": "CNYVND=X",
}


def run(
    symbols: list[str],
    start_date: str,
    end_date: str,
    output_dir: Path,
    gap_logger: GapLogger,
) -> None:
    """Fetch daily exchange rate history for each currency pair over the date range."""
    client = YahooFinanceClient()

    for pair, ticker in CURRENCY_MAPPING.items():
        if writer.is_done(output_dir, TABLE_NAME, pair):
            continue

        df = client.get_ticker_history(ticker, start_date, end_date)
        if df.empty:
            gap_logger.log(
                TABLE_NAME, pair, f"{start_date}..{end_date}", "empty API response"
            )
            writer.mark_done(output_dir, TABLE_NAME, pair)
            continue

        df = df.reset_index()
        date_col = next(
            (c for c in df.columns if str(c).lower() in ("date", "datetime")), None
        )
        if date_col is None:
            gap_logger.log(
                TABLE_NAME,
                pair,
                f"{start_date}..{end_date}",
                "no date column in response",
            )
            writer.mark_done(output_dir, TABLE_NAME, pair)
            continue

        df["date"] = df[date_col].astype(str).str.slice(0, 10)
        df["pair"] = pair
        df["exchange_rate"] = df["Close"].astype(str)
        df = df[["pair", "date", "exchange_rate"]]

        for row_date, group in df.groupby("date"):
            writer.append_csv(output_dir, TABLE_NAME, row_date, group)

        writer.mark_done(output_dir, TABLE_NAME, pair)
