"""Unit tests for world-index-based ingestion pipelines (no symbol loop)."""

from unittest.mock import MagicMock, patch

import pandas as pd
import pytest

from src.ingest.pipeline.commodities_price import CommoditiesPricePipeline
from src.ingest.pipeline.exchange_rates import ExchangeRatesPipeline
from src.ingest.pipeline.interest_rates import InterestRatesPipeline
from src.ingest.pipeline.macro_indicators import MacroIndicatorsPipeline

# ---------------------------------------------------------------------------
# MacroIndicatorsPipeline
# ---------------------------------------------------------------------------


def test_macro_indicators_pipeline_fetch_raises_not_implemented() -> None:
    """Verify fetch raises NotImplementedError."""
    pipeline = MacroIndicatorsPipeline(batch_date="2026-06-18")

    with pytest.raises(NotImplementedError):
        pipeline.fetch()


# ---------------------------------------------------------------------------
# ExchangeRatesPipeline
# ---------------------------------------------------------------------------


@patch("src.ingest.pipeline.exchange_rates.YahooFinanceClient")
def test_exchange_rates_pipeline_fetch_success(
    mock_client_class: MagicMock,
) -> None:
    """Verify fetch queries all currency tickers and combines them."""
    mock_client = MagicMock()
    mock_client_class.return_value = mock_client

    # Mock history return value for each currency call
    mock_df = pd.DataFrame(
        {
            "Date": [pd.Timestamp("2026-06-18 00:00:00")],
            "Close": [25450.0],
        }
    )
    mock_client.get_ticker_history.return_value = mock_df

    pipeline = ExchangeRatesPipeline(batch_date="2026-06-18")
    result_df = pipeline.fetch()

    # Should call get_ticker_history 5 times
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


# ---------------------------------------------------------------------------
# InterestRatesPipeline
# ---------------------------------------------------------------------------


def test_interest_rates_pipeline_fetch_raises_not_implemented() -> None:
    """Verify fetch raises NotImplementedError."""
    pipeline = InterestRatesPipeline(batch_date="2026-06-18")

    with pytest.raises(NotImplementedError):
        pipeline.fetch()


# ---------------------------------------------------------------------------
# CommoditiesPricePipeline Tests
# ---------------------------------------------------------------------------


@patch("src.ingest.pipeline.commodities_price.YahooFinanceClient")
def test_commodities_price_pipeline_fetch_success(
    mock_client_class: MagicMock,
) -> None:
    """Verify fetch queries all 6 commodities and merges them correctly."""
    mock_client = MagicMock()
    mock_client_class.return_value = mock_client

    # Mock return value for a single commodity row
    mock_df = pd.DataFrame(
        {
            "Close": [78.50],
            "Open": [78.00],
            "High": [79.00],
            "Low": [77.50],
            "Volume": [1000],
        },
        index=pd.DatetimeIndex(["2026-06-18"]),
    )
    mock_df.index.name = "Date"
    mock_client.get_ticker_history.return_value = mock_df

    pipeline = CommoditiesPricePipeline(batch_date="2026-06-18")
    result_df = pipeline.fetch()

    # Verify get_ticker_history calls
    assert mock_client.get_ticker_history.call_count == 6
    mock_client.get_ticker_history.assert_any_call(
        "BZ=F", "2026-06-18", "2026-06-19"
    )

    # Verify dataframe compilation
    assert len(result_df) == 6
    assert set(result_df["commodity_name"]) == {
        "Brent Crude",
        "WTI",
        "Gasoline Singapore (92/95)",
        "Baltic Dirty Tanker Index",
        "Gold",
        "Steel HRC",
    }
    assert all(result_df["date"] == "2026-06-18")
    assert all(result_df["price"] == "78.5")


@patch("src.ingest.pipeline.commodities_price.YahooFinanceClient")
def test_commodities_price_pipeline_returns_empty_when_no_data(
    mock_client_class: MagicMock,
) -> None:
    """Verify fetch returns empty DataFrame when all symbols return empty data."""
    mock_client = MagicMock()
    mock_client_class.return_value = mock_client
    mock_client.get_ticker_history.return_value = pd.DataFrame()

    pipeline = CommoditiesPricePipeline(batch_date="2026-06-18")
    result_df = pipeline.fetch()

    assert result_df.empty


@patch("src.ingest.pipeline.commodities_price.YahooFinanceClient")
def test_commodities_price_pipeline_propagates_error(
    mock_client_class: MagicMock,
) -> None:
    """Verify that exceptions raised by the client are propagated."""
    mock_client = MagicMock()
    mock_client_class.return_value = mock_client
    mock_client.get_ticker_history.side_effect = RuntimeError("API rate limited")

    pipeline = CommoditiesPricePipeline(batch_date="2026-06-18")
    with pytest.raises(RuntimeError, match="API rate limited"):
        pipeline.fetch()

