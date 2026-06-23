"""Unit tests for ExchangeRatesPipeline."""

from unittest.mock import MagicMock, patch

import pandas as pd
import pytest

from src.ingest.pipeline.exchange_rates import ExchangeRatesPipeline


def _make_date_index_df(
    date: str = "2026-06-18", close: float = 25450.0
) -> pd.DataFrame:
    """Build a DataFrame with a DatetimeIndex named 'Date' — mirrors yfinance output."""
    df = pd.DataFrame({"Close": [close]}, index=pd.DatetimeIndex([date]))
    df.index.name = "Date"
    return df


@patch("src.ingest.pipeline.exchange_rates.YahooFinanceClient")
def test_exchange_rates_pipeline_fetch_success(
    mock_client_class: MagicMock,
) -> None:
    """Verify fetch queries all currency tickers and combines them."""
    mock_client = MagicMock()
    mock_client_class.return_value = mock_client
    mock_client.get_ticker_history.return_value = _make_date_index_df()

    pipeline = ExchangeRatesPipeline(batch_date="2026-06-18")
    result_df = pipeline.fetch()

    assert mock_client.get_ticker_history.call_count == 5
    assert len(result_df) == 5
    expected_pairs = {"USD/VND", "EUR/VND", "GBP/VND", "JPY/VND", "CNY/VND"}
    assert set(result_df["pair"]) == expected_pairs
    assert result_df["date"].iloc[0] == "2026-06-18"
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

    def side_effect(ticker: str, start: str, end: str) -> pd.DataFrame:
        if ticker == "USDVND=X":
            return _make_date_index_df()
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


@patch("src.ingest.pipeline.exchange_rates.YahooFinanceClient")
def test_exchange_rates_pipeline_handles_datetime_index_name(
    mock_client_class: MagicMock,
) -> None:
    """Verify fetch works when yfinance renames the index to 'Datetime' (>= 1.x)."""
    mock_client = MagicMock()
    mock_client_class.return_value = mock_client

    # Simulate yfinance >= 1.x renaming Date → Datetime
    df = pd.DataFrame({"Close": [25450.0]}, index=pd.DatetimeIndex(["2026-06-18"]))
    df.index.name = "Datetime"
    mock_client.get_ticker_history.return_value = df

    pipeline = ExchangeRatesPipeline(batch_date="2026-06-18")
    result_df = pipeline.fetch()

    assert len(result_df) == 5
    assert result_df["date"].iloc[0] == "2026-06-18"


@patch("src.ingest.pipeline.exchange_rates.YahooFinanceClient")
def test_exchange_rates_pipeline_skips_ticker_with_no_date_column(
    mock_client_class: MagicMock,
) -> None:
    """Verify that a ticker with no recognizable date column is skipped, not raised."""
    mock_client = MagicMock()
    mock_client_class.return_value = mock_client

    # No Date/Datetime column after reset_index — only a plain RangeIndex
    bad_df = pd.DataFrame({"Close": [25450.0], "Open": [25400.0]})

    def side_effect(ticker: str, *_: str) -> pd.DataFrame:
        if ticker == "USDVND=X":
            return bad_df
        return pd.DataFrame()

    mock_client.get_ticker_history.side_effect = side_effect

    pipeline = ExchangeRatesPipeline(batch_date="2026-06-18")
    result_df = pipeline.fetch()

    # USD/VND is skipped because no date column; all others return empty → total empty
    assert result_df.empty


@patch("src.ingest.pipeline.exchange_rates.YahooFinanceClient")
def test_exchange_rates_pipeline_emits_single_summary_log(
    mock_client_class: MagicMock,
) -> None:
    """Verify fetch emits exactly one summary info log (not one per ticker)."""
    mock_client = MagicMock()
    mock_client_class.return_value = mock_client
    mock_client.get_ticker_history.return_value = _make_date_index_df()

    mock_logger = MagicMock()
    pipeline = ExchangeRatesPipeline(batch_date="2026-06-18", logger=mock_logger)
    pipeline.fetch()

    info_calls = list(mock_logger.info.call_args_list)
    # Only one info log should be emitted from fetch() — the summary after the loop
    assert len(info_calls) == 1
    assert "5" in str(info_calls[0])  # 5 currency pairs
