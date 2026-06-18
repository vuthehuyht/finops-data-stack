"""Unit tests for ExchangeRatesPipeline."""

from unittest.mock import MagicMock, patch

import pandas as pd
import pytest

from src.ingest.pipeline.exchange_rates import ExchangeRatesPipeline


@patch("src.ingest.pipeline.exchange_rates.YahooFinanceClient")
def test_exchange_rates_pipeline_fetch_success(
    mock_client_class: MagicMock,
) -> None:
    """Verify fetch queries all currency tickers and combines them."""
    mock_client = MagicMock()
    mock_client_class.return_value = mock_client

    mock_df = pd.DataFrame(
        {
            "Date": [pd.Timestamp("2026-06-18 00:00:00")],
            "Close": [25450.0],
        }
    )
    mock_client.get_ticker_history.return_value = mock_df

    pipeline = ExchangeRatesPipeline(batch_date="2026-06-18")
    result_df = pipeline.fetch()

    assert mock_client.get_ticker_history.call_count == 5
    assert len(result_df) == 5
    expected_pairs = {"USD/VND", "EUR/VND", "GBP/VND", "JPY/VND", "CNY/VND"}
    assert set(result_df["pair"]) == expected_pairs
    assert result_df["exchange_rate"].iloc[0] == "25450.0"


@patch("src.ingest.pipeline.exchange_rates.YahooFinanceClient")
def test_exchange_rates_pipeline_fetch_empty(
    mock_client_class: MagicMock,
) -> None:
    """Verify fetch returns empty DataFrame if no data found."""
    mock_client = MagicMock()
    mock_client_class.return_value = mock_client
    mock_client.get_ticker_history.return_value = pd.DataFrame()

    pipeline = ExchangeRatesPipeline(batch_date="2026-06-18")
    result_df = pipeline.fetch()

    assert result_df.empty


@patch("src.ingest.pipeline.exchange_rates.YahooFinanceClient")
def test_exchange_rates_pipeline_fetch_error(
    mock_client_class: MagicMock,
) -> None:
    """Verify fetch propagates exception if API client fails."""
    mock_client = MagicMock()
    mock_client_class.return_value = mock_client
    mock_client.get_ticker_history.side_effect = Exception("API connection error")

    pipeline = ExchangeRatesPipeline(batch_date="2026-06-18")
    with pytest.raises(Exception, match="API connection error"):
        pipeline.fetch()


@patch("src.ingest.pipeline.exchange_rates.YahooFinanceClient")
def test_exchange_rates_pipeline_partial_data_skips_empty_pairs(
    mock_client_class: MagicMock,
) -> None:
    """Verify pairs returning empty data are skipped; others are included."""
    mock_client = MagicMock()
    mock_client_class.return_value = mock_client

    mock_df = pd.DataFrame({"Date": [pd.Timestamp("2026-06-18")], "Close": [25450.0]})

    def side_effect(ticker: str, start: str, end: str) -> pd.DataFrame:
        if ticker == "USDVND=X":
            return mock_df
        return pd.DataFrame()

    mock_client.get_ticker_history.side_effect = side_effect

    pipeline = ExchangeRatesPipeline(batch_date="2026-06-18")
    result_df = pipeline.fetch()

    assert len(result_df) == 1
    assert result_df["pair"].iloc[0] == "USD/VND"


@patch("src.ingest.pipeline.exchange_rates.YahooFinanceClient")
def test_exchange_rates_pipeline_uses_correct_date_range(
    mock_client_class: MagicMock,
) -> None:
    """Verify fetch passes batch_date as start and batch_date+1 as exclusive end."""
    mock_client = MagicMock()
    mock_client_class.return_value = mock_client
    mock_client.get_ticker_history.return_value = pd.DataFrame()

    pipeline = ExchangeRatesPipeline(batch_date="2026-06-18")
    pipeline.fetch()

    for call in mock_client.get_ticker_history.call_args_list:
        assert call.args[1] == "2026-06-18"
        assert call.args[2] == "2026-06-19"


@patch("src.ingest.pipeline.exchange_rates.YahooFinanceClient")
def test_exchange_rates_pipeline_uses_correct_tickers(
    mock_client_class: MagicMock,
) -> None:
    """Verify fetch uses the Yahoo Finance ticker symbols for each currency pair."""
    mock_client = MagicMock()
    mock_client_class.return_value = mock_client
    mock_client.get_ticker_history.return_value = pd.DataFrame()

    pipeline = ExchangeRatesPipeline(batch_date="2026-06-18")
    pipeline.fetch()

    called_tickers = {
        call.args[0] for call in mock_client.get_ticker_history.call_args_list
    }
    assert called_tickers == {
        "USDVND=X",
        "EURVND=X",
        "GBPVND=X",
        "JPYVND=X",
        "CNYVND=X",
    }
