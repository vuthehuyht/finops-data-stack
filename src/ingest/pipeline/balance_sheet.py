"""Ingestion pipeline for RAW_BALANCE_SHEET."""

import pandas as pd
from vnstock import Vnstock as VnstockV4

from src.ingest.client.vnstock_client import VnStockClient
from src.ingest.pipeline.base import DEFAULT_TICKER_SYMBOLS, BaseIngestPipeline

# Maps VCI item_en labels to schema column names.
_COL_MAP: dict[str, str] = {
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
_BANK_COL_MAP: dict[str, str] = {
    "TOTAL ASSETS": "total_assets",
    "Cash and precious metals": "cash",
    "TOTAL LIABILITIES": "total_liabilities",
    "OWNER'S EQUITY": "equity",
}


class BalanceSheetPipeline(BaseIngestPipeline):
    """Pipeline to ingest corporate balance sheets into S3 Bronze."""

    @property
    def table_name(self) -> str:
        return "RAW_BALANCE_SHEET"

    @property
    def source_uri_prefix(self) -> str:
        return "api://vnstock/balance_sheet"

    @property
    def schema_columns(self) -> list[str]:
        return [
            "ticker",
            "period",
            "year",
            "total_assets",
            "current_assets",
            "cash",
            "inventory",
            "total_liabilities",
            "short_term_debt",
            "long_term_debt",
            "equity",
        ]

    def fetch(self) -> pd.DataFrame:
        """Fetch quarterly balance sheets for symbols on the batch date."""
        client = VnStockClient()
        all_dfs = []
        target_year = int(self.batch_date.split("-")[0])
        targets = self.symbols or DEFAULT_TICKER_SYMBOLS

        for symbol in targets:
            try:
                df = client.call_api_with_retry(
                    lambda s=symbol, y=target_year: (
                        VnstockV4()
                        .stock(symbol=s, source="VCI")
                        .finance.balance_sheet(period="quarter", year=y)
                    )
                )
                if not df.empty:
                    row = _pivot_to_row(df, _select_col_map(df), symbol)
                    if row is not None:
                        all_dfs.append(row)
            except Exception as e:
                self.logger.error("Failed to fetch balance sheet for %s: %s", symbol, e)
                raise e

        if not all_dfs:
            return pd.DataFrame()

        return pd.concat(all_dfs, ignore_index=True)


def _select_col_map(df: pd.DataFrame) -> dict[str, str] | None:
    """Pick whichever of _COL_MAP/_BANK_COL_MAP matches more item_en values.

    Some labels (e.g. "Cash and cash equivalents" vs bank-only lines) overlap
    only partially between corporate and bank statements, so a bare "any
    match" check would wrongly pick the corporate map for a bank stock.
    Comparing match counts avoids that.
    """
    best_col_map = None
    best_count = 0
    for col_map in (_COL_MAP, _BANK_COL_MAP):
        count = df["item_en"].isin(set(col_map.keys())).sum()
        if count > best_count:
            best_count = count
            best_col_map = col_map
    return best_col_map


def _pivot_to_row(
    df: pd.DataFrame, col_map: dict[str, str] | None, symbol: str
) -> pd.DataFrame | None:
    """Unpivot long-format VCI financial data into a single wide-format row."""
    if col_map is None:
        return None

    period_cols = [c for c in df.columns if c not in ("item", "item_en", "item_id")]
    if not period_cols:
        return None

    latest = period_cols[0]

    items_needed = set(col_map.keys())
    filtered = df[df["item_en"].isin(items_needed)].set_index("item_en")
    if filtered.empty:
        return None

    row = filtered[latest].rename(col_map).to_frame().T.reset_index(drop=True)

    year_str, quarter = latest.split("-")
    row["ticker"] = symbol
    row["period"] = quarter
    row["year"] = year_str

    return row
