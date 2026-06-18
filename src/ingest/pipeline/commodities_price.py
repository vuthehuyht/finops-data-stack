"""Ingestion pipeline for RAW_COMMODITIES_PRICE."""

import pandas as pd

from src.ingest.pipeline.base import BaseIngestPipeline


class CommoditiesPricePipeline(BaseIngestPipeline):
    """Pipeline to ingest commodity prices into S3 Bronze."""

    @property
    def table_name(self) -> str:
        return "RAW_COMMODITIES_PRICE"

    @property
    def source_uri_prefix(self) -> str:
        return "api://vnstock/commodities_price"

    @property
    def schema_columns(self) -> list[str]:
        return [
            "commodity_name",
            "date",
            "price",
        ]

    def fetch(self) -> pd.DataFrame:
        """Fetch daily commodities price on the batch date."""
        raise NotImplementedError(
            "CommoditiesPricePipeline.fetch() is not yet implemented. "
            "Provide the correct data source API call."
        )
