"""Ingestion pipeline for RAW_PROPRIETARY_TRADING."""

import logging
import os

import pandas as pd

from src.ingest.client.fireant_client import FireAntClient
from src.ingest.pipeline.base import DEFAULT_TICKER_SYMBOLS, BaseIngestPipeline

logger = logging.getLogger(__name__)


class ProprietaryTradingPipeline(BaseIngestPipeline):
    """Pipeline to ingest proprietary trading data into S3 Bronze."""

    @property
    def table_name(self) -> str:
        return "RAW_PROPRIETARY_TRADING"

    @property
    def source_uri_prefix(self) -> str:
        return "api://fireant/historical-quotes/prop-trading"

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
        """Fetch proprietary trading logs for symbols on the batch date."""
        client = FireAntClient(
            email=os.environ["FIREANT_EMAIL"], password=os.environ["FIREANT_PASSWORD"]
        )
        targets = self.symbols or DEFAULT_TICKER_SYMBOLS
        results = []

        for symbol in targets:
            try:
                quotes = client.get_historical_quotes(
                    symbol, start_date=self.batch_date, end_date=self.batch_date
                )
                if not quotes:
                    continue

                rows = []
                for q in quotes:
                    net_val = q.get("propTradingNetValue", 0)
                    if net_val == 0:
                        continue
                    rows.append(
                        {
                            "ticker": symbol,
                            "trading_date": q.get("date", "")[:10],
                            "buy_vol": "0",  # FireAnt only gives net value
                            "sell_vol": "0",  # FireAnt only gives net value
                            "net_val": str(net_val),
                        }
                    )

                if rows:
                    results.append(pd.DataFrame(rows))
            except Exception as e:
                self.logger.error(
                    "Failed to fetch proprietary trading for %s: %s", symbol, e
                )
                raise e

        if not results:
            return pd.DataFrame()

        return pd.concat(results, ignore_index=True)

    def standardize(self, df: pd.DataFrame) -> pd.DataFrame:
        """Standardize the fetched dataframe."""
        df_clean = df.drop(columns=["_actual_source"], errors="ignore")
        return super().standardize(df_clean)
