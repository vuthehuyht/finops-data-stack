"""Ingestion pipeline for RAW_INDEX_PRICE_EOD."""

import pandas as pd

from src.ingest.client.vnstock_client import VnStockClient
from src.ingest.pipeline.base import BaseIngestPipeline


class IndexPriceEodPipeline(BaseIngestPipeline):
    """Pipeline to ingest market index prices (e.g. VNINDEX, HNX) into S3 Bronze."""

    @property
    def table_name(self) -> str:
        return "RAW_INDEX_PRICE_EOD"

    @property
    def source_uri_prefix(self) -> str:
        return "api://vnstock/index_price"

    @property
    def schema_columns(self) -> list[str]:
        return [
            "index_name",
            "trading_date",
            "open",
            "high",
            "low",
            "close",
            "volume",
        ]

    def fetch(self) -> pd.DataFrame:
        """Fetch daily index prices for the batch date."""
        client = VnStockClient()
        all_dfs = []

        # Default indices to fetch
        indices = self.symbols if self.symbols else ["VNINDEX", "HNXINDEX"]

        for index in indices:
            try:
                # Reuse EOD pricing client interface
                df = client.get_stock_price_eod(
                    symbol=index,
                    start_date=self.batch_date,
                    end_date=self.batch_date,
                )
                if not df.empty:
                    if (
                        "INDEX_NAME" not in df.columns
                        and "index_name" not in df.columns
                    ):
                        df["index_name"] = index
                    all_dfs.append(df)
            except Exception as e:
                self.logger.error(
                    "Failed to fetch index EOD price for %s: %s", index, e
                )
                raise e

        if not all_dfs:
            return pd.DataFrame()

        return pd.concat(all_dfs, ignore_index=True)
