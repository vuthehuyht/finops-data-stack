"""Ingestion pipeline for RAW_MACRO_INDICATORS."""

import pandas as pd

from src.ingest.client.vnstock_client import VnStockClient
from src.ingest.pipeline.base import BaseIngestPipeline


class MacroIndicatorsPipeline(BaseIngestPipeline):
    """Pipeline to ingest macro indicators into S3 Bronze."""

    @property
    def table_name(self) -> str:
        return "RAW_MACRO_INDICATORS"

    @property
    def source_uri_prefix(self) -> str:
        return "api://vnstock/macro_indicators"

    @property
    def schema_columns(self) -> list[str]:
        return [
            "indicator_name",
            "report_date",
            "value",
            "unit",
        ]

    def fetch(self) -> pd.DataFrame:
        """Fetch macro indicator logs on the batch date."""
        client = VnStockClient()

        # Macro data is usually fetched globally, not by company symbols
        def _fetch_macro() -> pd.DataFrame:
            try:
                # Placeholder for vnstock macro indices or world indices
                return client.client.world_index()
            except (AttributeError, Exception):
                # Return empty frame if no world indices available
                return pd.DataFrame()

        df = client.call_api_with_retry(_fetch_macro)
        return df
