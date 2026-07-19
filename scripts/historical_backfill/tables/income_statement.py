"""Backfill RAW_INCOME_STATEMENT — quarterly income statements per symbol."""

from pathlib import Path

import pandas as pd
from vnstock import Vnstock as VnstockV4

from scripts.historical_backfill import writer
from scripts.historical_backfill.config import VNSTOCK_REQUEST_DELAY_SECONDS
from scripts.historical_backfill.gap_log import GapLogger
from src.ingest.client.vnstock_client import VnStockClient

TABLE_NAME = "income_statement"

# Full declared output schema — every row is reindexed to this column set so
# corporate rows (COL_MAP) and bank rows (BANK_COL_MAP) never write a
# different number of columns into the same shared CSV file.
SCHEMA_COLUMNS = [
    "ticker",
    "period",
    "year",
    "revenue",
    "cogs",
    "gross_profit",
    "operating_expenses",
    "operating_profit",
    "financial_income",
    "financial_expenses",
    "net_profit_after_tax",
]

COL_MAP: dict[str, str] = {
    "Sales": "revenue",
    "Cost of sales": "cogs",
    "Gross Profit": "gross_profit",
    "General and admin expenses": "operating_expenses",
    "Operating profit/(loss)": "operating_profit",
    "Financial income": "financial_income",
    "Financial expenses": "financial_expenses",
    "Net profit/(loss) after tax": "net_profit_after_tax",
}

# Bank stocks (ACB, VCB, TCB...) report income under different item_en labels —
# no cost-of-sales/gross-profit concept, interest income/expense instead of
# generic financial income/expense.
BANK_COL_MAP: dict[str, str] = {
    "Total Operating Income": "revenue",
    "General and Admin Expenses": "operating_expenses",
    "Net Operating Profit Before Allowance for Credit Loss": "operating_profit",
    "Interest and Similar Income": "financial_income",
    "Interest and Similar Expenses": "financial_expenses",
    "Net profit/(loss) after tax": "net_profit_after_tax",
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

    # VCI sometimes mixes in an annual-only column (e.g. "2018") alongside
    # quarterly ones even when period="quarter" was requested — "-" filters
    # those malformed labels out before the "YYYY-Qn" split below.
    period_cols = [
        c for c in df.columns if c not in ("item", "item_en", "item_id") and "-" in c
    ]
    filtered = df[df["item_en"].isin(set(col_map.keys()))].set_index("item_en")

    rows = []
    for period in period_cols:
        row = filtered[period].rename(col_map).to_frame().T.reset_index(drop=True)
        year_str, quarter = period.split("-")
        row["ticker"] = symbol
        row["period"] = quarter
        row["year"] = year_str
        rows.append(row.reindex(columns=SCHEMA_COLUMNS))

    return pd.concat(rows, ignore_index=True)


def run(
    symbols: list[str],
    start_date: str,
    end_date: str,
    output_dir: Path,
    gap_logger: GapLogger,
) -> None:
    """Fetch quarterly income statements for each symbol across every year in range."""
    client = VnStockClient(request_delay_seconds=VNSTOCK_REQUEST_DELAY_SECONDS)
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
                    .finance.income_statement(period="quarter", year=y)
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
