"""Unit tests for CommoditiesPricePipeline."""

from unittest.mock import MagicMock, patch

import pandas as pd
import pytest

from src.ingest.pipeline.commodities_price import CommoditiesPricePipeline


def _make_date_index_df(date: str = "2026-06-18", close: float = 78.50) -> pd.DataFrame:
    """Build a commodity DataFrame with a DatetimeIndex — mirrors yfinance output."""
    df = pd.DataFrame(
        {
            "Close": [close],
            "Open": [close - 0.50],
            "High": [close + 0.50],
            "Low": [close - 1.00],
            "Volume": [1000],
        },
        index=pd.DatetimeIndex([date]),
    )
    df.index.name = "Date"
    return df


@patch("src.ingest.pipeline.commodities_price.YahooFinanceClient")
def test_commodities_price_pipeline_fetch_success(
    mock_client_class: MagicMock,
) -> None:
    """Verify fetch queries all 6 commodities and merges them correctly."""
    mock_client = MagicMock()
    mock_client_class.return_value = mock_client
    mock_client.get_ticker_history.return_value = _make_date_index_df()

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

    def side_effect(ticker: str, *_: str) -> pd.DataFrame:
        if ticker == "BZ=F":
            return _make_date_index_df()
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


@patch("src.ingest.pipeline.commodities_price.YahooFinanceClient")
def test_commodities_price_pipeline_handles_datetime_index_name(
    mock_client_class: MagicMock,
) -> None:
    """Verify fetch works when yfinance renames the index to 'Datetime' (>= 1.x)."""
    mock_client = MagicMock()
    mock_client_class.return_value = mock_client

    # Simulate yfinance >= 1.x renaming Date → Datetime
    df = pd.DataFrame(
        {
            "Close": [78.50],
            "Open": [78.00],
            "High": [79.00],
            "Low": [77.50],
            "Volume": [1000],
        },
        index=pd.DatetimeIndex(["2026-06-18"]),
    )
    df.index.name = "Datetime"
    mock_client.get_ticker_history.return_value = df

    pipeline = CommoditiesPricePipeline(batch_date="2026-06-18")
    result_df = pipeline.fetch()

    assert len(result_df) == 6
    assert result_df["date"].iloc[0] == "2026-06-18"


@patch("src.ingest.pipeline.commodities_price.YahooFinanceClient")
def test_commodities_price_pipeline_skips_ticker_with_no_date_column(
    mock_client_class: MagicMock,
) -> None:
    """Verify that a ticker with no recognizable date column is skipped, not raised."""
    mock_client = MagicMock()
    mock_client_class.return_value = mock_client

    # DataFrame with no Date/Datetime column (plain RangeIndex, no named date column)
    bad_df = pd.DataFrame({"Close": [78.50], "Open": [78.00]})

    def side_effect(ticker: str, *_: str) -> pd.DataFrame:
        if ticker == "BZ=F":
            return bad_df
        return pd.DataFrame()

    mock_client.get_ticker_history.side_effect = side_effect

    pipeline = CommoditiesPricePipeline(batch_date="2026-06-18")
    result_df = pipeline.fetch()

    # BZ=F skipped (no date column); remaining 5 return empty → total empty
    assert result_df.empty


@patch("src.ingest.pipeline.commodities_price.YahooFinanceClient")
def test_commodities_price_pipeline_emits_single_summary_log(
    mock_client_class: MagicMock,
) -> None:
    """Verify fetch emits exactly one summary info log (not one per commodity)."""
    mock_client = MagicMock()
    mock_client_class.return_value = mock_client
    mock_client.get_ticker_history.return_value = _make_date_index_df()

    mock_logger = MagicMock()
    pipeline = CommoditiesPricePipeline(batch_date="2026-06-18", logger=mock_logger)
    pipeline.fetch()

    info_calls = mock_logger.info.call_args_list
    # Only one info log should be emitted from fetch() — the summary after the loop
    assert len(info_calls) == 1
    assert "6" in str(info_calls[0])  # 6 commodities
