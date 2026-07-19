"""Ingestion pipeline for RAW_FINANCIAL_RATIOS."""

import pandas as pd
from vnstock import Vnstock as VnstockV4

from src.ingest.client.vnstock_client import VnStockClient
from src.ingest.pipeline.base import DEFAULT_TICKER_SYMBOLS, BaseIngestPipeline

# Maps VCI item_en labels to schema column names.
_COL_MAP: dict[str, str] = {
    "Outstanding Shares (mil)": "shares_outstanding",
    "Market Cap": "market_cap",
}


class FinancialRatiosPipeline(BaseIngestPipeline):
    """Pipeline to ingest corporate financial ratios into S3 Bronze."""

    @property
    def table_name(self) -> str:
        return "RAW_FINANCIAL_RATIOS"

    @property
    def source_uri_prefix(self) -> str:
        return "api://vnstock/financial_ratios"

    @property
    def schema_columns(self) -> list[str]:
        return [
            "ticker",
            "period",
            "year",
            "shares_outstanding",
            "market_cap",
        ]

    def fetch(self) -> pd.DataFrame:
        """Fetch the latest quarterly financial ratios for symbols from VCI."""
        client = VnStockClient()
        all_dfs = []
        targets = self.symbols or DEFAULT_TICKER_SYMBOLS

        for symbol in targets:
            try:
                df = client.call_api_with_retry(
                    lambda s=symbol: (
                        VnstockV4()
                        .stock(symbol=s, source="VCI")
                        .finance.ratio(period="quarter")
                    )
                )
                if not df.empty:
                    row = _pivot_to_row(df, _COL_MAP, symbol)
                    if row is not None:
                        all_dfs.append(row)
            except Exception as e:
                self.logger.error(
                    "Failed to fetch financial ratios for %s: %s", symbol, e
                )
                raise e

        if not all_dfs:
            return pd.DataFrame()

        return pd.concat(all_dfs, ignore_index=True)


def _pivot_to_row(
    df: pd.DataFrame, col_map: dict[str, str], symbol: str
) -> pd.DataFrame | None:
    """Unpivot the most recent period of long-format VCI ratio data into one row."""
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
