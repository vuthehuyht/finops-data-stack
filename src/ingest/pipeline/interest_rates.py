"""Ingestion pipeline for RAW_INTEREST_RATES."""

import pandas as pd

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
        raise NotImplementedError(
            "InterestRatesPipeline.fetch() is not yet implemented. "
            "Provide the correct data source API call."
        )
