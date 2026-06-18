"""VnStock client wrapper based on vnstock3 library."""

import logging

import pandas as pd
from vnstock3 import Vnstock

from src.ingest.client.base_client import BaseClient

logger = logging.getLogger(__name__)


class VnStockClient(BaseClient):
    """Client for fetching stock price and fundamental data from Vietnam market."""

    def __init__(self, request_delay_seconds: float = 1.0) -> None:
        """Initialize VnStockClient.

        Args:
            request_delay_seconds: Spacing delay between requests.
        """
        super().__init__(request_delay_seconds=request_delay_seconds)
        self.client = Vnstock()

    def get_stock_price_eod(
        self, symbol: str, start_date: str, end_date: str, source: str = "TCBS"
    ) -> pd.DataFrame:
        """Fetch daily historical price for a stock symbol.

        Args:
            symbol: Stock ticker (e.g. TCB).
            start_date: Start date string (YYYY-MM-DD).
            end_date: End date string (YYYY-MM-DD).
            source: Source engine ('TCBS' or 'SSI').

        Returns:
            DataFrame containing stock historical price.
        """
        stock_obj = self.client.stock(symbol=symbol, source=source)

        # We wrap the quote.history call in our retry framework
        def _fetch() -> pd.DataFrame:
            return stock_obj.quote.history(start=start_date, end=end_date)

        df = self.call_api_with_retry(_fetch)
        return df
