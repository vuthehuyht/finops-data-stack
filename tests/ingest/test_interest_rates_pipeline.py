"""Unit tests for InterestRatesPipeline."""

from unittest.mock import MagicMock, patch

import pandas as pd
import pytest

from src.ingest.pipeline.interest_rates import (
    DEFAULT_RATE_MAPPING,
    InterestRatesPipeline,
)


@patch("src.ingest.pipeline.interest_rates.YahooFinanceClient")
def test_interest_rates_pipeline_fetch_success(
    mock_client_class: MagicMock,
) -> None:
    """Verify fetch queries all rate tickers and combines them."""
    mock_client = MagicMock()
    mock_client_class.return_value = mock_client

    mock_df = pd.DataFrame(
        {
            "Date": [pd.Timestamp("2026-06-18 00:00:00")],
            "Close": [4.35],
        }
    )
    mock_client.get_ticker_history.return_value = mock_df

    pipeline = InterestRatesPipeline(batch_date="2026-06-18")
    result_df = pipeline.fetch()

    assert mock_client.get_ticker_history.call_count == len(DEFAULT_RATE_MAPPING)
    assert len(result_df) == len(DEFAULT_RATE_MAPPING)
    assert set(result_df["rate_type"]) == set(DEFAULT_RATE_MAPPING.keys())
    assert result_df["rate_value"].iloc[0] == "4.35"


@patch("src.ingest.pipeline.interest_rates.YahooFinanceClient")
def test_interest_rates_pipeline_fetch_empty(
    mock_client_class: MagicMock,
) -> None:
    """Verify fetch returns empty DataFrame if no data found for any ticker."""
    mock_client = MagicMock()
    mock_client_class.return_value = mock_client
    mock_client.get_ticker_history.return_value = pd.DataFrame()

    pipeline = InterestRatesPipeline(batch_date="2026-06-18")
    result_df = pipeline.fetch()

    assert result_df.empty


@patch("src.ingest.pipeline.interest_rates.YahooFinanceClient")
def test_interest_rates_pipeline_fetch_error(
    mock_client_class: MagicMock,
) -> None:
    """Verify fetch propagates exception if API client fails."""
    mock_client = MagicMock()
    mock_client_class.return_value = mock_client
    mock_client.get_ticker_history.side_effect = Exception("API connection error")

    pipeline = InterestRatesPipeline(batch_date="2026-06-18")
    with pytest.raises(Exception, match="API connection error"):
        pipeline.fetch()


@patch("src.ingest.pipeline.interest_rates.YahooFinanceClient")
def test_interest_rates_pipeline_fetch_partial_skips_empty(
    mock_client_class: MagicMock,
) -> None:
    """Verify rate types returning empty data are skipped; others are included."""
    mock_client = MagicMock()
    mock_client_class.return_value = mock_client

    first_ticker = next(iter(DEFAULT_RATE_MAPPING.values()))
    first_rate_type = next(iter(DEFAULT_RATE_MAPPING.keys()))
    mock_df = pd.DataFrame({"Date": [pd.Timestamp("2026-06-18")], "Close": [4.35]})

    def side_effect(ticker: str, start: str, end: str) -> pd.DataFrame:
        if ticker == first_ticker:
            return mock_df
        return pd.DataFrame()

    mock_client.get_ticker_history.side_effect = side_effect

    pipeline = InterestRatesPipeline(batch_date="2026-06-18")
    result_df = pipeline.fetch()

    assert len(result_df) == 1
    assert result_df["rate_type"].iloc[0] == first_rate_type


@patch("src.ingest.pipeline.interest_rates.YahooFinanceClient")
def test_interest_rates_pipeline_fetch_uses_correct_date_range(
    mock_client_class: MagicMock,
) -> None:
    """Verify fetch passes batch_date as start and batch_date+1 as exclusive end."""
    mock_client = MagicMock()
    mock_client_class.return_value = mock_client
    mock_client.get_ticker_history.return_value = pd.DataFrame()

    pipeline = InterestRatesPipeline(batch_date="2026-06-18")
    pipeline.fetch()

    for call in mock_client.get_ticker_history.call_args_list:
        assert call.args[1] == "2026-06-18"
        assert call.args[2] == "2026-06-19"


@patch("src.ingest.pipeline.interest_rates.YahooFinanceClient")
def test_interest_rates_pipeline_fetch_uses_correct_tickers(
    mock_client_class: MagicMock,
) -> None:
    """Verify fetch uses the Yahoo Finance ticker symbols for each rate type."""
    mock_client = MagicMock()
    mock_client_class.return_value = mock_client
    mock_client.get_ticker_history.return_value = pd.DataFrame()

    pipeline = InterestRatesPipeline(batch_date="2026-06-18")
    pipeline.fetch()

    called_tickers = {
        call.args[0] for call in mock_client.get_ticker_history.call_args_list
    }
    assert called_tickers == set(DEFAULT_RATE_MAPPING.values())


def test_interest_rates_pipeline_table_name() -> None:
    """Verify table_name matches the design spec."""
    pipeline = InterestRatesPipeline(batch_date="2026-06-18")
    assert pipeline.table_name == "RAW_INTEREST_RATES"


def test_interest_rates_pipeline_schema_columns() -> None:
    """Verify schema_columns matches the design spec."""
    pipeline = InterestRatesPipeline(batch_date="2026-06-18")
    assert pipeline.schema_columns == ["rate_type", "date", "rate_value"]
