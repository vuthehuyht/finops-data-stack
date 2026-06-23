"""Ingestion pipeline for RAW_EXCHANGE_RATES."""

import datetime

import pandas as pd

from src.ingest.client.yahoo_finance_client import YahooFinanceClient
from src.ingest.pipeline.base import BaseIngestPipeline

DEFAULT_CURRENCY_MAPPING: dict[str, str] = {
    "USD/VND": "USDVND=X",
    "EUR/VND": "EURVND=X",
    "GBP/VND": "GBPVND=X",
    "JPY/VND": "JPYVND=X",
    "CNY/VND": "CNYVND=X",
}


class ExchangeRatesPipeline(BaseIngestPipeline):
    """Pipeline to ingest daily exchange rates into S3 Bronze."""

    @property
    def table_name(self) -> str:
        return "RAW_EXCHANGE_RATES"

    @property
    def source_uri_prefix(self) -> str:
        return "api://yahoo_finance/exchange_rates"

    @property
    def schema_columns(self) -> list[str]:
        return [
            "pair",
            "date",
            "exchange_rate",
        ]

    def fetch(self) -> pd.DataFrame:
        """Fetch daily currency exchange rates on the batch date."""
        client = YahooFinanceClient()
        all_dfs = []

        # Parse batch date to calculate the exclusive end date (batch_date + 1 day)
        start_dt = pd.to_datetime(self.batch_date).date()
        end_dt = start_dt + datetime.timedelta(days=1)
        start_str = start_dt.strftime("%Y-%m-%d")
        end_str = end_dt.strftime("%Y-%m-%d")

        for name, ticker in DEFAULT_CURRENCY_MAPPING.items():
            try:
                df = client.get_ticker_history(ticker, start_str, end_str)
                if not df.empty:
                    df = df.reset_index()
                    # yfinance >= 1.x renames the date index; detect it defensively
                    date_col = next(
                        (
                            c
                            for c in df.columns
                            if str(c).lower() in ("date", "datetime")
                        ),
                        None,
                    )
                    if date_col is None:
                        self.logger.error(
                            "Cannot find date column in %s. Available: %s",
                            ticker,
                            list(df.columns),
                        )
                        continue

                    df["date"] = pd.to_datetime(df[date_col]).dt.strftime("%Y-%m-%d")
                    df["pair"] = name
                    df["exchange_rate"] = df["Close"].astype(str)

                    df = df[["pair", "date", "exchange_rate"]]
                    all_dfs.append(df)
                else:
                    self.logger.warning(
                        "No data found for currency pair %s (%s) on %s",
                        name,
                        ticker,
                        self.batch_date,
                    )
            except Exception as e:
                self.logger.error(
                    "Failed to fetch exchange rate for %s (%s): %s",
                    name,
                    ticker,
                    e,
                )
                raise e

        if not all_dfs:
            return pd.DataFrame()

        result = pd.concat(all_dfs, ignore_index=True)
        self.logger.info(
            "Fetched %d rows across %d currency pairs for batch_date %s",
            len(result),
            len(all_dfs),
            self.batch_date,
        )
        return result
