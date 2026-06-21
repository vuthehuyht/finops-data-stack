"""Ingestion pipeline for RAW_COMMODITIES_PRICE."""

import datetime

import pandas as pd

from src.ingest.client.yahoo_finance_client import YahooFinanceClient
from src.ingest.pipeline.base import BaseIngestPipeline

DEFAULT_COMMODITIES_MAPPING: dict[str, str] = {
    "Brent Crude": "BZ=F",
    "WTI": "CL=F",
    "Gasoline Singapore (92/95)": "RB=F",
    "Baltic Dirty Tanker Index": "^BDTI",
    "Gold": "GC=F",
    "Steel HRC": "HR=F",
}


class CommoditiesPricePipeline(BaseIngestPipeline):
    """Pipeline to ingest commodity prices into S3 Bronze."""

    @property
    def table_name(self) -> str:
        return "RAW_COMMODITIES_PRICE"

    @property
    def source_uri_prefix(self) -> str:
        return "api://yahoo_finance/commodities_price"

    @property
    def schema_columns(self) -> list[str]:
        return [
            "commodity_name",
            "date",
            "price",
        ]

    def fetch(self) -> pd.DataFrame:
        """Fetch daily commodities price on the batch date."""
        client = YahooFinanceClient()
        all_dfs = []

        # Parse batch date to calculate the exclusive end date (batch_date + 1 day)
        start_dt = pd.to_datetime(self.batch_date).date()
        end_dt = start_dt + datetime.timedelta(days=1)
        start_str = start_dt.strftime("%Y-%m-%d")
        end_str = end_dt.strftime("%Y-%m-%d")

        self.logger.info(
            "Fetching prices for %d commodities for batch_date %s",
            len(DEFAULT_COMMODITIES_MAPPING),
            self.batch_date,
        )

        for name, ticker in DEFAULT_COMMODITIES_MAPPING.items():
            try:
                df = client.get_ticker_history(ticker, start_str, end_str)
                if not df.empty:
                    df = df.reset_index()

                    # Standardize Date column values and Close price
                    df["date"] = pd.to_datetime(df["Date"]).dt.strftime("%Y-%m-%d")
                    df["commodity_name"] = name
                    df["price"] = df["Close"].astype(str)

                    # Keep only required columns
                    df = df[["commodity_name", "date", "price"]]
                    all_dfs.append(df)
                else:
                    self.logger.warning(
                        "No data found for commodity %s (%s) on %s",
                        name,
                        ticker,
                        self.batch_date,
                    )
            except Exception as e:
                self.logger.error(
                    "Failed to fetch commodity price for %s (%s): %s",
                    name,
                    ticker,
                    e,
                )
                raise e

        if not all_dfs:
            return pd.DataFrame()

        return pd.concat(all_dfs, ignore_index=True)
