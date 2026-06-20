"""Ingestion pipeline for RAW_PROPRIETARY_TRADING."""

import pandas as pd

from src.ingest.pipeline.base import BaseIngestPipeline


class ProprietaryTradingPipeline(BaseIngestPipeline):
    """Pipeline to ingest proprietary trading data into S3 Bronze."""

    @property
    def table_name(self) -> str:
        return "RAW_PROPRIETARY_TRADING"

    @property
    def source_uri_prefix(self) -> str:
        return "api://vnstock/proprietary_trading"

    @property
    def schema_columns(self) -> list[str]:
        return [
            "ticker",
            "trading_date",
            "buy_vol",
            "sell_vol",
            "net_val",
        ]

    def fetch(self) -> pd.DataFrame:
        """Fetch proprietary trading logs for symbols on the batch date.

        Not implemented: vnstock v4 prop_trade() is a stub (not yet released),
        and TCBS public API endpoints for this data return 404.
        A paid source (FiinTrade / Vietstock) is required.
        """
        raise NotImplementedError(
            "ProprietaryTradingPipeline.fetch() requires a paid data source "
            "(FiinTrade or Vietstock). vnstock v4 prop_trade() is not yet implemented "
            "for any supported source (VCI, KBS)."
        )
