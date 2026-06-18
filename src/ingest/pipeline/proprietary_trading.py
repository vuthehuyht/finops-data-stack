"""Ingestion pipeline for RAW_PROPRIETARY_TRADING."""

import pandas as pd

from src.ingest.client.vnstock_client import VnStockClient
from src.ingest.pipeline.base import BaseIngestPipeline


class ProprietaryTradingPipeline(BaseIngestPipeline):
    """Pipeline to ingest proprietary trading data into S3 Bronze."""

    @property
    def table_name(self) -> str:
        return "RAW_PROPRIETARY_TRADING"

    @property
    def source_uri_prefix(self) -> str:
        return "api://vnstock/proprietary_trading"

    @property
    def schema_columns(self) -> list[str]:
        return [
            "ticker",
            "trading_date",
            "buy_vol",
            "sell_vol",
            "net_val",
        ]

    def fetch(self) -> pd.DataFrame:
        """Fetch proprietary trading logs for symbols on the batch date."""
        client = VnStockClient()
        all_dfs = []

        for symbol in self.symbols:
            try:
                stock_obj = client.client.stock(symbol=symbol, source="TCBS")
                if not hasattr(stock_obj, "trading"):
                    continue

                df = client.call_api_with_retry(
                    stock_obj.trading.proprietary_flow,
                    start=self.batch_date,
                    end=self.batch_date,
                )
                if not df.empty:
                    if "TICKER" not in df.columns and "symbol" not in df.columns:
                        df["ticker"] = symbol
                    all_dfs.append(df)
            except Exception as e:
                self.logger.error(
                    "Failed to fetch proprietary trading for %s: %s",
                    symbol,
                    e,
                )
                raise e

        if not all_dfs:
            return pd.DataFrame()

        return pd.concat(all_dfs, ignore_index=True)
