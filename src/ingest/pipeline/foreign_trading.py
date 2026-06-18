"""Ingestion pipeline for RAW_FOREIGN_TRADING."""

import pandas as pd

from src.ingest.client.vnstock_client import VnStockClient
from src.ingest.pipeline.base import DEFAULT_TICKER_SYMBOLS, BaseIngestPipeline


class ForeignTradingPipeline(BaseIngestPipeline):
    """Pipeline to ingest foreign trading data into S3 Bronze."""

    @property
    def table_name(self) -> str:
        return "RAW_FOREIGN_TRADING"

    @property
    def source_uri_prefix(self) -> str:
        return "api://vnstock/foreign_trading"

    @property
    def schema_columns(self) -> list[str]:
        return [
            "ticker",
            "trading_date",
            "buy_vol",
            "sell_vol",
            "buy_val",
            "sell_val",
            "net_val",
        ]

    def fetch(self) -> pd.DataFrame:
        """Fetch foreign trading logs for symbols on the batch date."""
        client = VnStockClient()
        all_dfs = []
        targets = self.symbols or DEFAULT_TICKER_SYMBOLS

        for symbol in targets:
            try:
                stock_obj = client.client.stock(symbol=symbol, source="TCBS")
                if not hasattr(stock_obj, "trading"):
                    continue

                # Query foreign flow directly via client retry runner
                df = client.call_api_with_retry(
                    stock_obj.trading.foreign_flow,
                    start=self.batch_date,
                    end=self.batch_date,
                )
                if not df.empty:
                    if "TICKER" not in df.columns and "symbol" not in df.columns:
                        df["ticker"] = symbol
                    all_dfs.append(df)
            except Exception as e:
                self.logger.error(
                    "Failed to fetch foreign trading for %s: %s", symbol, e
                )
                raise e

        if not all_dfs:
            return pd.DataFrame()

        return pd.concat(all_dfs, ignore_index=True)
