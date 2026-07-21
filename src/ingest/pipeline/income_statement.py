"""Ingestion pipeline for RAW_INCOME_STATEMENT."""

import pandas as pd
from vnstock import Vnstock as VnstockV4

from src.ingest.client.vnstock_client import VnStockClient
from src.ingest.pipeline.base import DEFAULT_TICKER_SYMBOLS, BaseIngestPipeline

# Maps VCI item_en labels to schema column names.
_COL_MAP: dict[str, str] = {
    "Sales": "revenue",
    "Cost of sales": "cogs",
    "Gross Profit": "gross_profit",
    "General and admin expenses": "operating_expenses",
    "Operating profit/(loss)": "operating_profit",
    "Financial income": "financial_income",
    "Financial expenses": "financial_expenses",
    "Net profit/(loss) after tax": "net_profit_after_tax",
}

# Bank stocks (ACB, VCB, TCB...) report income under different item_en labels.
_BANK_COL_MAP: dict[str, str] = {
    "Total Operating Income": "revenue",
    "General and Admin Expenses": "operating_expenses",
    "Net Operating Profit Before Allowance for Credit Loss": "operating_profit",
    "Interest and Similar Income": "financial_income",
    "Interest and Similar Expenses": "financial_expenses",
    "Net profit/(loss) after tax": "net_profit_after_tax",
}


class IncomeStatementPipeline(BaseIngestPipeline):
    """Pipeline to ingest corporate income statements into S3 Bronze."""

    @property
    def table_name(self) -> str:
        return "RAW_INCOME_STATEMENT"

    @property
    def source_uri_prefix(self) -> str:
        return "api://vnstock/income_statement"

    @property
    def schema_columns(self) -> list[str]:
        return [
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

    def fetch(self) -> pd.DataFrame:
        """Fetch quarterly income statements for symbols on the batch date."""
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
                        .finance.income_statement(period="quarter", year=y)
                    )
                )
                if not df.empty:
                    row = _pivot_to_row(df, _select_col_map(df), symbol)
                    if row is not None:
                        all_dfs.append(row)
            except Exception as e:
                self.logger.error(
                    "Failed to fetch income statement for %s: %s", symbol, e
                )
                raise e

        if not all_dfs:
            return pd.DataFrame()

        return pd.concat(all_dfs, ignore_index=True)


def _select_col_map(df: pd.DataFrame) -> dict[str, str] | None:
    """Pick whichever of _COL_MAP/_BANK_COL_MAP matches more item_en values.

    Some labels (e.g. "Net profit/(loss) after tax") are shared between
    corporate and bank statements, so a bare "any match" check would wrongly
    pick the corporate map for a bank stock. Comparing match counts avoids
    that.
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
    """Unpivot long-format VCI financial data into a single wide-format row.

    VCI returns rows as financial line items and columns as periods (e.g. 2025-Q4).
    This function selects the most recent period, maps item_en labels to schema
    column names, and returns a single-row DataFrame with ticker/period/year added.
    """
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

    latest = period_cols[0]  # VCI returns periods in descending order

    # Filter to only the items we need, then pivot to wide format
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
