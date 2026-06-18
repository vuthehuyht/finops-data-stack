"""Ingestion pipeline for RAW_STOCK_PRICE_EOD."""

import pandas as pd

from src.ingest.client.vnstock_client import VnStockClient
from src.ingest.pipeline.base import BaseIngestPipeline


class StockPriceEodPipeline(BaseIngestPipeline):
    """Pipeline to ingest EOD stock prices into S3 Bronze."""

    @property
    def table_name(self) -> str:
        return "RAW_STOCK_PRICE_EOD"

    @property
    def source_uri_prefix(self) -> str:
        return "api://vnstock/stock_price"

    @property
    def schema_columns(self) -> list[str]:
        return [
            "ticker",
            "trading_date",
            "open",
            "high",
            "low",
            "close",
            "volume",
            "value",
            "adjusted_close",
        ]

    def fetch(self) -> pd.DataFrame:
        """Fetch EOD prices for all configured symbols on the partition date."""
        client = VnStockClient()
        all_dfs = []

        self.logger.info(
            "Fetching EOD price for %d symbols for batch_date %s",
            len(self.symbols),
            self.batch_date,
        )

        for symbol in self.symbols:
            try:
                # Price is for a single day: start_date=batch_date, end_date=batch_date
                df = client.get_stock_price_eod(
                    symbol=symbol,
                    start_date=self.batch_date,
                    end_date=self.batch_date,
                )
                if not df.empty:
                    # Inject symbol column if not returned in target schema
                    if "TICKER" not in df.columns and "symbol" not in df.columns:
                        df["ticker"] = symbol
                    all_dfs.append(df)
            except Exception as e:
                self.logger.error("Failed to fetch EOD price for %s: %s", symbol, e)
                raise e

        if not all_dfs:
            return pd.DataFrame()

        return pd.concat(all_dfs, ignore_index=True)
