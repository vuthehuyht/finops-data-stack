"""Ingestion pipeline for RAW_MACRO_INDICATORS."""

import pandas as pd

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
        raise NotImplementedError(
            "MacroIndicatorsPipeline.fetch() is not yet implemented. "
            "Provide the correct data source API call."
        )
