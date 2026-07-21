"""Ingestion pipeline for RAW_FOREIGN_TRADING."""

import os

import pandas as pd

from src.ingest.client.fireant_client import FireAntClient
from src.ingest.pipeline.base import DEFAULT_TICKER_SYMBOLS, BaseIngestPipeline


class ForeignTradingPipeline(BaseIngestPipeline):
    """Pipeline to ingest foreign trading data into S3 Bronze."""

    @property
    def table_name(self) -> str:
        return "RAW_FOREIGN_TRADING"

    @property
    def source_uri_prefix(self) -> str:
        return "api://fireant/historical-quotes"

    @property
    def schema_columns(self) -> list[str]:
        return [
            "ticker",
            "trading_date",
            "buy_vol",
            "sell_vol",
            "buy_val",
            "sell_val",
            "net_val",
        ]

    def fetch(self) -> pd.DataFrame:
        """Fetch foreign trading logs for symbols on the batch date."""
        client = FireAntClient(
            email=os.environ["FIREANT_EMAIL"], password=os.environ["FIREANT_PASSWORD"]
        )
        all_dfs = []
        targets = self.symbols or DEFAULT_TICKER_SYMBOLS

        for symbol in targets:
            try:
                quotes = client.get_historical_quotes(
                    symbol, start_date=self.batch_date, end_date=self.batch_date
                )
                if not quotes:
                    continue

                rows = []
                for q in quotes:
                    buy_val = q.get("buyForeignValue", 0)
                    sell_val = q.get("sellForeignValue", 0)
                    net_val = buy_val - sell_val
                    rows.append(
                        {
                            "ticker": symbol,
                            "trading_date": q.get("date", "")[:10],
                            "buy_vol": q.get("buyForeignQuantity", 0),
                            "sell_vol": q.get("sellForeignQuantity", 0),
                            "buy_val": buy_val,
                            "sell_val": sell_val,
                            "net_val": net_val,
                        }
                    )

                if rows:
                    all_dfs.append(pd.DataFrame(rows))
            except Exception as e:
                self.logger.error(
                    "Failed to fetch foreign trading for %s: %s", symbol, e
                )
                raise e

        if not all_dfs:
            return pd.DataFrame()

        return pd.concat(all_dfs, ignore_index=True)
