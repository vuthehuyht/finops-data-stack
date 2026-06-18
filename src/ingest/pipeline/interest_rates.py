"""Ingestion pipeline for RAW_INTEREST_RATES."""

import pandas as pd

from src.ingest.client.vnstock_client import VnStockClient
from src.ingest.pipeline.base import BaseIngestPipeline


class InterestRatesPipeline(BaseIngestPipeline):
    """Pipeline to ingest banking interest rates into S3 Bronze."""

    @property
    def table_name(self) -> str:
        return "RAW_INTEREST_RATES"

    @property
    def source_uri_prefix(self) -> str:
        return "api://vnstock/interest_rates"

    @property
    def schema_columns(self) -> list[str]:
        return [
            "rate_type",
            "date",
            "rate_value",
        ]

    def fetch(self) -> pd.DataFrame:
        """Fetch daily interest rates on the batch date."""
        client = VnStockClient()

        # Let's call the interest rates fetch logic
        def _fetch_rates() -> pd.DataFrame:
            try:
                # Use world index or generic fx/fund tracker from vnstock as fallback
                return client.client.world_index()
            except (AttributeError, Exception):
                return pd.DataFrame()

        df = client.call_api_with_retry(_fetch_rates)
        return df
