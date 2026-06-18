"""Unit tests for CommoditiesPricePipeline."""

from unittest.mock import MagicMock, patch

import pandas as pd
import pytest

from src.ingest.pipeline.commodities_price import CommoditiesPricePipeline


@patch("src.ingest.pipeline.commodities_price.YahooFinanceClient")
def test_commodities_price_pipeline_fetch_success(
    mock_client_class: MagicMock,
) -> None:
    """Verify fetch queries all 6 commodities and merges them correctly."""
    mock_client = MagicMock()
    mock_client_class.return_value = mock_client

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

    assert mock_client.get_ticker_history.call_count == 6
    mock_client.get_ticker_history.assert_any_call("BZ=F", "2026-06-18", "2026-06-19")
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


@patch("src.ingest.pipeline.commodities_price.YahooFinanceClient")
def test_commodities_price_pipeline_partial_data_skips_empty_commodities(
    mock_client_class: MagicMock,
) -> None:
    """Verify commodities returning empty data are skipped; others are included."""
    mock_client = MagicMock()
    mock_client_class.return_value = mock_client

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

    def side_effect(ticker: str, *_: str) -> pd.DataFrame:
        if ticker == "BZ=F":
            return mock_df
        return pd.DataFrame()

    mock_client.get_ticker_history.side_effect = side_effect

    pipeline = CommoditiesPricePipeline(batch_date="2026-06-18")
    result_df = pipeline.fetch()

    assert len(result_df) == 1
    assert result_df["commodity_name"].iloc[0] == "Brent Crude"


@patch("src.ingest.pipeline.commodities_price.YahooFinanceClient")
def test_commodities_price_pipeline_uses_correct_date_range(
    mock_client_class: MagicMock,
) -> None:
    """Verify fetch passes batch_date as start and batch_date+1 as exclusive end."""
    mock_client = MagicMock()
    mock_client_class.return_value = mock_client
    mock_client.get_ticker_history.return_value = pd.DataFrame()

    pipeline = CommoditiesPricePipeline(batch_date="2026-06-18")
    pipeline.fetch()

    for call in mock_client.get_ticker_history.call_args_list:
        assert call.args[1] == "2026-06-18"
        assert call.args[2] == "2026-06-19"


@patch("src.ingest.pipeline.commodities_price.YahooFinanceClient")
def test_commodities_price_pipeline_uses_correct_tickers(
    mock_client_class: MagicMock,
) -> None:
    """Verify fetch uses the correct Yahoo Finance ticker symbols for each commodity."""
    mock_client = MagicMock()
    mock_client_class.return_value = mock_client
    mock_client.get_ticker_history.return_value = pd.DataFrame()

    pipeline = CommoditiesPricePipeline(batch_date="2026-06-18")
    pipeline.fetch()

    called_tickers = {
        call.args[0] for call in mock_client.get_ticker_history.call_args_list
    }
    assert called_tickers == {"BZ=F", "CL=F", "RB=F", "^BDTI", "GC=F", "HR=F"}
