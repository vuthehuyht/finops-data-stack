"""Ingestion pipeline for RAW_INTEREST_RATES."""

import datetime

import pandas as pd

from src.ingest.client.yahoo_finance_client import YahooFinanceClient
from src.ingest.pipeline.base import BaseIngestPipeline

# Yahoo Finance tickers for benchmark interest rates relevant to VN market analysis.
# US rates are used as daily time-series inputs for the LSTM branch — they move daily
# and are tightly correlated with SBV monetary policy direction.
DEFAULT_RATE_MAPPING: dict[str, str] = {
    "us_fed_funds_rate": "^IRX",  # 13-week T-Bill — Fed benchmark proxy
    "us_10y_treasury": "^TNX",  # US 10Y — global risk-free rate
    "us_5y_treasury": "^FVX",  # US 5Y — mid-term benchmark
}


class InterestRatesPipeline(BaseIngestPipeline):
    """Pipeline to ingest banking interest rates into S3 Bronze."""

    @property
    def table_name(self) -> str:
        return "RAW_INTEREST_RATES"

    @property
    def source_uri_prefix(self) -> str:
        return "api://yahoo_finance/interest_rates"

    @property
    def schema_columns(self) -> list[str]:
        return [
            "rate_type",
            "date",
            "rate_value",
        ]

    def fetch(self) -> pd.DataFrame:
        """Fetch daily benchmark interest rates on the batch date."""
        client = YahooFinanceClient()
        all_dfs = []

        start_dt = pd.to_datetime(self.batch_date).date()
        end_dt = start_dt + datetime.timedelta(days=1)
        start_str = start_dt.strftime("%Y-%m-%d")
        end_str = end_dt.strftime("%Y-%m-%d")

        self.logger.info(
            "Fetching %d interest rate tickers for batch_date %s",
            len(DEFAULT_RATE_MAPPING),
            self.batch_date,
        )

        for rate_type, ticker in DEFAULT_RATE_MAPPING.items():
            try:
                df = client.get_ticker_history(ticker, start_str, end_str)
                if not df.empty:
                    df = df.reset_index()
                    df["date"] = pd.to_datetime(df["Date"]).dt.strftime("%Y-%m-%d")
                    df["rate_type"] = rate_type
                    df["rate_value"] = df["Close"].astype(str)
                    df = df[["rate_type", "date", "rate_value"]]
                    all_dfs.append(df)
                else:
                    self.logger.warning(
                        "No data found for rate_type %s (%s) on %s",
                        rate_type,
                        ticker,
                        self.batch_date,
                    )
            except Exception as e:
                self.logger.error(
                    "Failed to fetch interest rate for %s (%s): %s",
                    rate_type,
                    ticker,
                    e,
                )
                raise e

        if not all_dfs:
            return pd.DataFrame()

        return pd.concat(all_dfs, ignore_index=True)
