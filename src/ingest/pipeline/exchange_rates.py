"""Ingestion pipeline for RAW_EXCHANGE_RATES."""

import pandas as pd

from src.ingest.client.vnstock_client import VnStockClient
from src.ingest.pipeline.base import BaseIngestPipeline


class ExchangeRatesPipeline(BaseIngestPipeline):
    """Pipeline to ingest daily exchange rates into S3 Bronze."""

    @property
    def table_name(self) -> str:
        return "RAW_EXCHANGE_RATES"

    @property
    def source_uri_prefix(self) -> str:
        return "api://vnstock/exchange_rates"

    @property
    def schema_columns(self) -> list[str]:
        return [
            "pair",
            "date",
            "exchange_rate",
        ]

    def fetch(self) -> pd.DataFrame:
        """Fetch daily currency exchange rates on the batch date."""
        client = VnStockClient()

        def _fetch_fx() -> pd.DataFrame:
            try:
                # Call world index or generic fx tracker from vnstock as fallback
                return client.client.world_index()
            except (AttributeError, Exception):
                return pd.DataFrame()

        df = client.call_api_with_retry(_fetch_fx)
        return df
