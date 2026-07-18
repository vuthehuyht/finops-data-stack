"""Backfill RAW_BALANCE_SHEET — quarterly balance sheets per symbol across years."""

from pathlib import Path

import pandas as pd
from vnstock import Vnstock as VnstockV4

from scripts.historical_backfill import writer
from scripts.historical_backfill.gap_log import GapLogger
from src.ingest.client.vnstock_client import VnStockClient

TABLE_NAME = "balance_sheet"

COL_MAP: dict[str, str] = {
    "Total Assets": "total_assets",
    "CURRENT ASSETS": "current_assets",
    "Cash and cash equivalents": "cash",
    "Inventories, Net": "inventory",
    "Liabilities": "total_liabilities",
    "Short-term borrowings": "short_term_debt",
    "Long-term borrowings": "long_term_debt",
    "Capital and reserves": "equity",
}

# Bank stocks (ACB, VCB, TCB...) report under different item_en labels — no
# current/non-current split, no inventory, no simple short/long-term debt line.
BANK_COL_MAP: dict[str, str] = {
    "TOTAL ASSETS": "total_assets",
    "Cash and precious metals": "cash",
    "TOTAL LIABILITIES": "total_liabilities",
    "OWNER'S EQUITY": "equity",
}


def _select_col_map(df: pd.DataFrame) -> dict[str, str] | None:
    """Pick whichever of COL_MAP/BANK_COL_MAP matches more item_en values.

    Some labels (e.g. "Net profit/(loss) after tax") are shared between
    corporate and bank statements, so a bare "any match" check would wrongly
    pick the corporate map for a bank stock. Comparing match counts avoids
    that.
    """
    best_col_map = None
    best_count = 0
    for col_map in (COL_MAP, BANK_COL_MAP):
        count = df["item_en"].isin(set(col_map.keys())).sum()
        if count > best_count:
            best_count = count
            best_col_map = col_map
    return best_col_map


def _pivot_all_periods(df: pd.DataFrame, symbol: str) -> pd.DataFrame:
    """Unpivot every period column of VCI long-format data into one row per quarter."""
    col_map = _select_col_map(df)
    if col_map is None:
        return pd.DataFrame()

    period_cols = [c for c in df.columns if c not in ("item", "item_en", "item_id")]
    filtered = df[df["item_en"].isin(set(col_map.keys()))].set_index("item_en")

    rows = []
    for period in period_cols:
        row = filtered[period].rename(col_map).to_frame().T.reset_index(drop=True)
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
    """Fetch quarterly balance sheets for each symbol across every year in range."""
    client = VnStockClient()
    start_year = int(start_date.split("-")[0])
    end_year = int(end_date.split("-")[0])

    for symbol in symbols:
        for year in range(start_year, end_year + 1):
            marker_key = f"{symbol}_{year}"
            if writer.is_done(output_dir, TABLE_NAME, marker_key):
                continue

            df = client.call_api_with_retry(
                lambda s=symbol, y=year: (
                    VnstockV4()
                    .stock(symbol=s, source="VCI")
                    .finance.balance_sheet(period="quarter", year=y)
                )
            )
            if df.empty:
                gap_logger.log(TABLE_NAME, symbol, str(year), "empty API response")
                writer.mark_done(output_dir, TABLE_NAME, marker_key)
                continue

            rows = _pivot_all_periods(df, symbol)
            if rows.empty:
                gap_logger.log(
                    TABLE_NAME, symbol, str(year), "no matching line items in response"
                )
                writer.mark_done(output_dir, TABLE_NAME, marker_key)
                continue

            period_label = rows["year"].astype(str) + "-" + rows["period"].astype(str)
            for label, group in rows.groupby(period_label):
                writer.append_csv(output_dir, TABLE_NAME, label, group)

            writer.mark_done(output_dir, TABLE_NAME, marker_key)
