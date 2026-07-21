"""Ingestion pipeline for RAW_COMPANY_PROFILE."""

import pandas as pd
from vnstock import Company

from src.ingest.client.vnstock_client import VnStockClient
from src.ingest.pipeline.base import DEFAULT_TICKER_SYMBOLS, BaseIngestPipeline


class CompanyProfilePipeline(BaseIngestPipeline):
    """Pipeline to ingest corporate profiles into S3 Bronze."""

    @property
    def table_name(self) -> str:
        return "RAW_COMPANY_PROFILE"

    @property
    def source_uri_prefix(self) -> str:
        return "api://vnstock/company_profile"

    @property
    def schema_columns(self) -> list[str]:
        return [
            "ticker",
            "company_name",
            "industry",
            "exchange",
            "outstanding_share",
            "description",
        ]

    def fetch(self) -> pd.DataFrame:
        """Fetch company profiles for symbols from VCI source."""
        client = VnStockClient()
        all_dfs = []
        targets = self.symbols or DEFAULT_TICKER_SYMBOLS

        for symbol in targets:
            try:
                # Use Company from the modern vnstock library with VCI source
                # call_api_with_retry ensures rate limit and retries
                df = client.call_api_with_retry(
                    lambda s=symbol: Company(source="VCI", symbol=s).info()
                )
                if not df.empty:
                    # Map raw columns to pipeline schema columns
                    df = df.rename(
                        columns={
                            "symbol": "ticker",
                            "organ_name": "company_name",
                            "sector": "industry",
                            "com_group_code": "exchange",
                            "issue_share": "outstanding_share",
                            "company_profile": "description",
                        }
                    )
                    # Keep only columns defined in schema
                    available_cols = [
                        col for col in self.schema_columns if col in df.columns
                    ]
                    df = df[available_cols]
                    all_dfs.append(df)
            except Exception as e:
                self.logger.error(
                    "Failed to fetch company profile for %s: %s", symbol, e
                )
                raise e

        if not all_dfs:
            return pd.DataFrame()

        return pd.concat(all_dfs, ignore_index=True)
