"""Ingestion pipeline for RAW_INSIDER_TRANSACTIONS."""

import pandas as pd

from src.ingest.pipeline.base import BaseIngestPipeline


class InsiderTransactionsPipeline(BaseIngestPipeline):
    """Pipeline to ingest corporate insider trading logs into S3 Bronze.

    NOTE: No reliable data source is currently available.
    - vnstock3 TCBS: 404 Not Found
    - vnstock3 VCI: 403 Forbidden
    - vnstock v4 KBS: returns empty data for all symbols
    Implement fetch() when a working source is identified.
    """

    @property
    def table_name(self) -> str:
        return "RAW_INSIDER_TRANSACTIONS"

    @property
    def source_uri_prefix(self) -> str:
        return "api://vnstock/insider_transactions"

    @property
    def schema_columns(self) -> list[str]:
        return [
            "ticker",
            "deal_announce_date",
            "deal_method",
            "deal_action",
            "deal_quantity",
            "deal_price",
            "deal_ratio",
        ]

    def fetch(self) -> pd.DataFrame:
        """Fetch insider transactions for symbols on the batch date."""
        raise NotImplementedError(
            "InsiderTransactionsPipeline.fetch() is not yet implemented. "
            "No working data source found: TCBS returns 404, VCI returns 403, "
            "KBS returns empty data for all VN30 symbols."
        )
