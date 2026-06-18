"""Ingestion pipeline for RAW_CORPORATE_EVENTS."""

import pandas as pd

from src.ingest.client.vnstock_client import VnStockClient
from src.ingest.pipeline.base import BaseIngestPipeline


class CorporateEventsPipeline(BaseIngestPipeline):
    """Pipeline to ingest corporate calendar events into S3 Bronze."""

    @property
    def table_name(self) -> str:
        return "RAW_CORPORATE_EVENTS"

    @property
    def source_uri_prefix(self) -> str:
        return "api://vnstock/corporate_events"

    @property
    def schema_columns(self) -> list[str]:
        return [
            "event_id",
            "ticker",
            "event_type",
            "ex_right_date",
            "record_date",
            "event_details",
        ]

    def fetch(self) -> pd.DataFrame:
        """Fetch corporate events for symbols on the batch date."""
        client = VnStockClient()
        all_dfs = []

        for symbol in self.symbols:
            try:
                stock_obj = client.client.stock(symbol=symbol, source="TCBS")
                if not hasattr(stock_obj, "company"):
                    continue

                df = client.call_api_with_retry(stock_obj.company.events)
                if not df.empty:
                    if "TICKER" not in df.columns and "symbol" not in df.columns:
                        df["ticker"] = symbol
                    all_dfs.append(df)
            except Exception as e:
                self.logger.error(
                    "Failed to fetch corporate events for %s: %s", symbol, e
                )
                raise e

        if not all_dfs:
            return pd.DataFrame()

        return pd.concat(all_dfs, ignore_index=True)
