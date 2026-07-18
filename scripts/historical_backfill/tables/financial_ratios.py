"""Backfill RAW_FINANCIAL_RATIOS — quarterly shares outstanding & market cap per symbol.

TCBS (the original source) no longer exists as a supported vnstock source, and
it only ever exposed a flat daily series. VCI exposes the same two metrics
through finance.ratio(), but as long-format quarterly data — so this table's
grain changed from daily to quarterly (ticker, period, year).
"""

from pathlib import Path

import pandas as pd
from vnstock import Vnstock as VnstockV4

from scripts.historical_backfill import writer
from scripts.historical_backfill.gap_log import GapLogger
from src.ingest.client.vnstock_client import VnStockClient

TABLE_NAME = "financial_ratios"

COL_MAP: dict[str, str] = {
    "Outstanding Shares (mil)": "shares_outstanding",
    "Market Cap": "market_cap",
}


def _pivot_all_periods(df: pd.DataFrame, symbol: str) -> pd.DataFrame:
    """Unpivot VCI long-format ratio data periods to one row per quarter."""
    period_cols = [c for c in df.columns if c not in ("item", "item_en", "item_id")]
    items_needed = set(COL_MAP.keys())
    filtered = df[df["item_en"].isin(items_needed)].set_index("item_en")
    if filtered.empty:
        return pd.DataFrame()

    rows = []
    for period in period_cols:
        row = filtered[period].rename(COL_MAP).to_frame().T.reset_index(drop=True)
        year_str, quarter = period.split("-")
        row["ticker"] = symbol
        row["period"] = quarter
        row["year"] = year_str
        rows.append(row)

    return pd.concat(rows, ignore_index=True)


def run(
    symbols: list[str],
    start_date: str,
    end_date: str,
    output_dir: Path,
    gap_logger: GapLogger,
) -> None:
    """Fetch quarterly financial ratios for each symbol, filtered to the date range."""
    client = VnStockClient()
    start_year = int(start_date.split("-")[0])
    end_year = int(end_date.split("-")[0])

    for symbol in symbols:
        if writer.is_done(output_dir, TABLE_NAME, symbol):
            continue

        df = client.call_api_with_retry(
            lambda s=symbol: (
                VnstockV4()
                .stock(symbol=s, source="VCI")
                .finance.ratio(period="quarter")
            )
        )
        if df.empty:
            gap_logger.log(
                TABLE_NAME, symbol, f"{start_date}..{end_date}", "empty API response"
            )
            writer.mark_done(output_dir, TABLE_NAME, symbol)
            continue

        rows = _pivot_all_periods(df, symbol)
        if rows.empty:
            gap_logger.log(
                TABLE_NAME,
                symbol,
                f"{start_date}..{end_date}",
                "no matching line items in response",
            )
            writer.mark_done(output_dir, TABLE_NAME, symbol)
            continue

        rows["year"] = rows["year"].astype(int)
        rows = rows[(rows["year"] >= start_year) & (rows["year"] <= end_year)]
        if rows.empty:
            gap_logger.log(
                TABLE_NAME,
                symbol,
                f"{start_date}..{end_date}",
                "no periods within requested range",
            )
            writer.mark_done(output_dir, TABLE_NAME, symbol)
            continue

        period_label = rows["year"].astype(str) + "-" + rows["period"].astype(str)
        for label, group in rows.groupby(period_label):
            writer.append_csv(output_dir, TABLE_NAME, label, group)

        writer.mark_done(output_dir, TABLE_NAME, symbol)
