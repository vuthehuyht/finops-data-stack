"""Yahoo Finance API Client logic."""

import pandas as pd
import yfinance as yf

from src.ingest.client.base_client import BaseClient


class YahooFinanceClient(BaseClient):
    """Client for Yahoo Finance API using yfinance."""

    def __init__(self, request_delay_seconds: float = 1.0) -> None:
        """Initialize Yahoo Finance client.

        Args:
            request_delay_seconds: Time to delay between requests.
        """
        super().__init__(request_delay_seconds=request_delay_seconds)

    def get_ticker_history(
        self, ticker: str, start_date: str, end_date: str
    ) -> pd.DataFrame:
        """Fetch historical price data for a ticker symbol.

        Args:
            ticker: The Yahoo Finance ticker symbol (e.g., 'BZ=F').
            start_date: Start date string (YYYY-MM-DD).
            end_date: End date string (YYYY-MM-DD, exclusive in yfinance).

        Returns:
            pd.DataFrame of historical stock data.
        """
        ticker_obj = yf.Ticker(ticker)
        return self.call_api_with_retry(
            ticker_obj.history,
            start=start_date,
            end=end_date,
            interval="1d",
        )
