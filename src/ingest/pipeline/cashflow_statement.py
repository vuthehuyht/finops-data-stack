"""Ingestion pipeline for RAW_CASHFLOW_STATEMENT."""

import pandas as pd
from vnstock import Vnstock as VnstockV4

from src.ingest.client.vnstock_client import VnStockClient
from src.ingest.pipeline.base import DEFAULT_TICKER_SYMBOLS, BaseIngestPipeline

# Maps VCI item_en labels to schema column names.
_COL_MAP: dict[str, str] = {
    "Net cash inflows/(outflows) from operating activities": "cfo",
    "Net cash inflows/(outflows) from investing activities": "cfi",
    "Net cash inflows/(outflows) from financing activities": "cff",
    "Net increase in cash and cash equivalents": "net_cash_flow",
    "Purchases of fixed assets and other long term assets": "capex",
}

# Bank stocks (ACB, VCB, TCB...) report cashflow under different item_en labels.
# No aggregate "financing activities" line was found for banks — cff stays None.
_BANK_COL_MAP: dict[str, str] = {
    "Net cash from operating activities": "cfo",
    "Net cash from investing activities": "cfi",
    "Net Increase/(Decrease) in cash and cash equivalents": "net_cash_flow",
    "Purchases of fixed assets and other long term assets": "capex",
}


class CashflowStatementPipeline(BaseIngestPipeline):
    """Pipeline to ingest corporate cashflow statements into S3 Bronze."""

    @property
    def table_name(self) -> str:
        return "RAW_CASHFLOW_STATEMENT"

    @property
    def source_uri_prefix(self) -> str:
        return "api://vnstock/cashflow_statement"

    @property
    def schema_columns(self) -> list[str]:
        return [
            "ticker",
            "period",
            "year",
            "cfo",
            "cfi",
            "cff",
            "net_cash_flow",
            "capex",
        ]

    def fetch(self) -> pd.DataFrame:
        """Fetch quarterly cashflow statements for symbols on the batch date."""
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
                        .finance.cash_flow(period="quarter", year=y)
                    )
                )
                if not df.empty:
                    row = _pivot_to_row(df, _select_col_map(df), symbol)
                    if row is not None:
                        all_dfs.append(row)
            except Exception as e:
                self.logger.error(
                    "Failed to fetch cashflow statement for %s: %s", symbol, e
                )
                raise e

        if not all_dfs:
            return pd.DataFrame()

        return pd.concat(all_dfs, ignore_index=True)


def _select_col_map(df: pd.DataFrame) -> dict[str, str] | None:
    """Pick whichever of _COL_MAP/_BANK_COL_MAP matches more item_en values.

    Some labels (e.g. capex) are shared between corporate and bank
    statements, so a bare "any match" check would wrongly pick the corporate
    map for a bank stock. Comparing match counts avoids that.
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

    # VCI sometimes mixes in an annual-only column (e.g. "2018") alongside
    # quarterly ones — "-" filters those malformed labels out before picking
    # the latest quarter below.
    period_cols = [
        c for c in df.columns if c not in ("item", "item_en", "item_id") and "-" in c
    ]
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
