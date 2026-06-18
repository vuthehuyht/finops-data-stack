from unittest.mock import MagicMock, patch

import pandas as pd

from src.ingest.client.yahoo_finance_client import YahooFinanceClient


@patch("src.ingest.client.yahoo_finance_client.yf.Ticker")
def test_yahoo_finance_client_get_ticker_history(
    mock_ticker_class: MagicMock,
) -> None:
    """Verify get_ticker_history calls yfinance and returns dataframe."""
    mock_ticker = MagicMock()
    mock_ticker_class.return_value = mock_ticker
    mock_df = pd.DataFrame({"Close": [100.0]})
    mock_ticker.history.return_value = mock_df

    client = YahooFinanceClient(request_delay_seconds=0.1)
    result = client.get_ticker_history("XYZ", "2026-06-18", "2026-06-19")

    mock_ticker_class.assert_called_once_with("XYZ")
    mock_ticker.history.assert_called_once_with(
        start="2026-06-18", end="2026-06-19", interval="1d"
    )
    assert result.equals(mock_df)
