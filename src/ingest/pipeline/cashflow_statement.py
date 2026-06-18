"""Ingestion pipeline for RAW_CASHFLOW_STATEMENT."""

import pandas as pd

from src.ingest.client.vnstock_client import VnStockClient
from src.ingest.pipeline.base import BaseIngestPipeline


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

        for symbol in self.symbols:
            try:
                stock_obj = client.client.stock(symbol=symbol, source="TCBS")
                if not hasattr(stock_obj, "finance"):
                    continue

                df = client.call_api_with_retry(
                    stock_obj.finance.cash_flow,
                    period="quarter",
                    year=target_year,
                )
                if not df.empty:
                    if "TICKER" not in df.columns and "symbol" not in df.columns:
                        df["ticker"] = symbol
                    all_dfs.append(df)
            except Exception as e:
                self.logger.error(
                    "Failed to fetch cashflow statement for %s: %s", symbol, e
                )
                raise e

        if not all_dfs:
            return pd.DataFrame()

        return pd.concat(all_dfs, ignore_index=True)
