"""Ingestion pipeline for RAW_CORPORATE_EVENTS."""

import pandas as pd
from vnstock import Company

from src.ingest.client.vnstock_client import VnStockClient
from src.ingest.pipeline.base import DEFAULT_TICKER_SYMBOLS, BaseIngestPipeline


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
        """Fetch corporate events for symbols on the batch date from VCI."""
        client = VnStockClient()
        all_dfs = []
        targets = self.symbols or DEFAULT_TICKER_SYMBOLS

        for symbol in targets:
            try:
                # Use Company from the modern vnstock library with VCI source
                # call_api_with_retry ensures rate limit and retries
                df = client.call_api_with_retry(
                    lambda s=symbol: Company(source="VCI", symbol=s).events()
                )
                if not df.empty:
                    # Map raw columns to pipeline schema columns
                    df = df.rename(
                        columns={
                            "id": "event_id",
                            "category": "event_type",
                            "exright_date": "ex_right_date",
                            "event_title_vi": "event_details",
                        }
                    )

                    # Backfill ticker if missing or null in the response
                    if "ticker" not in df.columns:
                        df["ticker"] = symbol
                    else:
                        df["ticker"] = df["ticker"].fillna(symbol)

                    # Keep only columns defined in schema
                    available_cols = [
                        col for col in self.schema_columns if col in df.columns
                    ]
                    df = df[available_cols]
                    all_dfs.append(df)
            except Exception as e:
                self.logger.error(
                    "Failed to fetch corporate events for %s: %s", symbol, e
                )
                raise e

        if not all_dfs:
            return pd.DataFrame()

        return pd.concat(all_dfs, ignore_index=True)
