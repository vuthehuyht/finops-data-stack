"""Ingestion pipeline for RAW_FINANCIAL_RATIOS."""

import pandas as pd

from src.ingest.client.vnstock_client import VnStockClient
from src.ingest.pipeline.base import BaseIngestPipeline


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
            "date",
            "shares_outstanding",
            "market_cap",
        ]

    def fetch(self) -> pd.DataFrame:
        """Fetch financial ratios for symbols on the batch date."""
        client = VnStockClient()
        all_dfs = []

        for symbol in self.symbols:
            try:
                stock_obj = client.client.stock(symbol=symbol, source="TCBS")
                if not hasattr(stock_obj, "finance"):
                    continue

                df = client.call_api_with_retry(stock_obj.finance.ratio)
                if not df.empty:
                    if "TICKER" not in df.columns and "symbol" not in df.columns:
                        df["ticker"] = symbol
                    all_dfs.append(df)
            except Exception as e:
                self.logger.error(
                    "Failed to fetch financial ratios for %s: %s", symbol, e
                )
                raise e

        if not all_dfs:
            return pd.DataFrame()

        return pd.concat(all_dfs, ignore_index=True)
