"""Ingestion pipeline for RAW_BALANCE_SHEET."""

import pandas as pd

from src.ingest.client.vnstock_client import VnStockClient
from src.ingest.pipeline.base import DEFAULT_TICKER_SYMBOLS, BaseIngestPipeline


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
                stock_obj = client.client.stock(symbol=symbol, source="TCBS")
                if not hasattr(stock_obj, "finance"):
                    continue

                df = client.call_api_with_retry(
                    stock_obj.finance.balance_sheet,
                    period="quarter",
                    year=target_year,
                )
                if not df.empty:
                    if "TICKER" not in df.columns and "symbol" not in df.columns:
                        df["ticker"] = symbol
                    all_dfs.append(df)
            except Exception as e:
                self.logger.error(
                    "Failed to fetch balance sheet for %s: %s", symbol, e
                )
                raise e

        if not all_dfs:
            return pd.DataFrame()

        return pd.concat(all_dfs, ignore_index=True)
