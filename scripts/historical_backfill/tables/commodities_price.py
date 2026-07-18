"""Backfill RAW_COMMODITIES_PRICE — daily commodity prices over a date range."""

from pathlib import Path

from scripts.historical_backfill import writer
from scripts.historical_backfill.gap_log import GapLogger
from src.ingest.client.yahoo_finance_client import YahooFinanceClient

TABLE_NAME = "commodities_price"

COMMODITIES_MAPPING: dict[str, str] = {
    "Brent Crude": "BZ=F",
    "WTI": "CL=F",
    "Gasoline Singapore (92/95)": "RB=F",
    "Baltic Dirty Tanker Index": "^BDTI",
    "Gold": "GC=F",
    "Steel HRC": "HR=F",
}


def run(
    symbols: list[str],
    start_date: str,
    end_date: str,
    output_dir: Path,
    gap_logger: GapLogger,
) -> None:
    """Fetch daily commodity price history for each commodity over the date range."""
    client = YahooFinanceClient()

    for commodity_name, ticker in COMMODITIES_MAPPING.items():
        if writer.is_done(output_dir, TABLE_NAME, commodity_name):
            continue

        df = client.get_ticker_history(ticker, start_date, end_date)
        if df.empty:
            gap_logger.log(
                TABLE_NAME,
                commodity_name,
                f"{start_date}..{end_date}",
                "empty API response",
            )
            writer.mark_done(output_dir, TABLE_NAME, commodity_name)
            continue

        df = df.reset_index()
        date_col = next(
            (c for c in df.columns if str(c).lower() in ("date", "datetime")), None
        )
        if date_col is None:
            gap_logger.log(
                TABLE_NAME,
                commodity_name,
                f"{start_date}..{end_date}",
                "no date column in response",
            )
            writer.mark_done(output_dir, TABLE_NAME, commodity_name)
            continue

        df["date"] = df[date_col].astype(str).str.slice(0, 10)
        df["commodity_name"] = commodity_name
        df["price"] = df["Close"].astype(str)
        df = df[["commodity_name", "date", "price"]]

        for row_date, group in df.groupby("date"):
            writer.append_csv(output_dir, TABLE_NAME, row_date, group)

        writer.mark_done(output_dir, TABLE_NAME, commodity_name)
