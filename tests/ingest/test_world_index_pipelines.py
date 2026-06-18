"""Unit tests for world-index-based ingestion pipelines (no symbol loop)."""

from unittest.mock import MagicMock, patch

import pandas as pd

from src.ingest.pipeline.commodities_price import CommoditiesPricePipeline
from src.ingest.pipeline.exchange_rates import ExchangeRatesPipeline
from src.ingest.pipeline.interest_rates import InterestRatesPipeline
from src.ingest.pipeline.macro_indicators import MacroIndicatorsPipeline

# ---------------------------------------------------------------------------
# MacroIndicatorsPipeline
# ---------------------------------------------------------------------------


@patch("src.ingest.pipeline.macro_indicators.VnStockClient")
def test_macro_indicators_pipeline_fetch_returns_data(
    mock_client_class: MagicMock,
) -> None:
    """Verify fetch calls call_api_with_retry once and returns the DataFrame."""
    mock_client = MagicMock()
    mock_client_class.return_value = mock_client

    expected_df = pd.DataFrame({"indicator_name": ["GDP"], "value": [450.0]})
    mock_client.call_api_with_retry.return_value = expected_df

    pipeline = MacroIndicatorsPipeline(batch_date="2026-06-18")
    result_df = pipeline.fetch()

    mock_client.call_api_with_retry.assert_called_once()
    assert len(result_df) == 1
    assert result_df["indicator_name"].iloc[0] == "GDP"


@patch("src.ingest.pipeline.macro_indicators.VnStockClient")
def test_macro_indicators_pipeline_fetch_returns_empty_on_api_error(
    mock_client_class: MagicMock,
) -> None:
    """Verify fetch returns empty DataFrame when call_api_with_retry returns empty."""
    mock_client = MagicMock()
    mock_client_class.return_value = mock_client
    mock_client.call_api_with_retry.return_value = pd.DataFrame()

    pipeline = MacroIndicatorsPipeline(batch_date="2026-06-18")
    result_df = pipeline.fetch()

    assert result_df.empty


# ---------------------------------------------------------------------------
# ExchangeRatesPipeline
# ---------------------------------------------------------------------------


@patch("src.ingest.pipeline.exchange_rates.VnStockClient")
def test_exchange_rates_pipeline_fetch_returns_data(
    mock_client_class: MagicMock,
) -> None:
    """Verify fetch calls call_api_with_retry once and returns the DataFrame."""
    mock_client = MagicMock()
    mock_client_class.return_value = mock_client

    expected_df = pd.DataFrame({"pair": ["USD/VND"], "exchange_rate": [25000.0]})
    mock_client.call_api_with_retry.return_value = expected_df

    pipeline = ExchangeRatesPipeline(batch_date="2026-06-18")
    result_df = pipeline.fetch()

    mock_client.call_api_with_retry.assert_called_once()
    assert len(result_df) == 1


@patch("src.ingest.pipeline.exchange_rates.VnStockClient")
def test_exchange_rates_pipeline_fetch_returns_empty_when_no_data(
    mock_client_class: MagicMock,
) -> None:
    """Verify fetch returns empty DataFrame when world_index returns empty."""
    mock_client = MagicMock()
    mock_client_class.return_value = mock_client
    mock_client.call_api_with_retry.return_value = pd.DataFrame()

    pipeline = ExchangeRatesPipeline(batch_date="2026-06-18")
    result_df = pipeline.fetch()

    assert result_df.empty


# ---------------------------------------------------------------------------
# InterestRatesPipeline
# ---------------------------------------------------------------------------


@patch("src.ingest.pipeline.interest_rates.VnStockClient")
def test_interest_rates_pipeline_fetch_returns_data(
    mock_client_class: MagicMock,
) -> None:
    """Verify fetch calls call_api_with_retry once and returns the DataFrame."""
    mock_client = MagicMock()
    mock_client_class.return_value = mock_client

    expected_df = pd.DataFrame({"rate_type": ["OVERNIGHT"], "rate_value": [4.5]})
    mock_client.call_api_with_retry.return_value = expected_df

    pipeline = InterestRatesPipeline(batch_date="2026-06-18")
    result_df = pipeline.fetch()

    mock_client.call_api_with_retry.assert_called_once()
    assert len(result_df) == 1


@patch("src.ingest.pipeline.interest_rates.VnStockClient")
def test_interest_rates_pipeline_fetch_returns_empty_when_no_data(
    mock_client_class: MagicMock,
) -> None:
    """Verify fetch returns empty DataFrame when world_index returns empty."""
    mock_client = MagicMock()
    mock_client_class.return_value = mock_client
    mock_client.call_api_with_retry.return_value = pd.DataFrame()

    pipeline = InterestRatesPipeline(batch_date="2026-06-18")
    result_df = pipeline.fetch()

    assert result_df.empty


# ---------------------------------------------------------------------------
# CommoditiesPricePipeline
# ---------------------------------------------------------------------------


@patch("src.ingest.pipeline.commodities_price.VnStockClient")
def test_commodities_price_pipeline_fetch_returns_data(
    mock_client_class: MagicMock,
) -> None:
    """Verify fetch calls call_api_with_retry once and returns the DataFrame."""
    mock_client = MagicMock()
    mock_client_class.return_value = mock_client

    expected_df = pd.DataFrame({"commodity_name": ["OIL"], "price": [80.5]})
    mock_client.call_api_with_retry.return_value = expected_df

    pipeline = CommoditiesPricePipeline(batch_date="2026-06-18")
    result_df = pipeline.fetch()

    mock_client.call_api_with_retry.assert_called_once()
    assert len(result_df) == 1
    assert result_df["commodity_name"].iloc[0] == "OIL"


@patch("src.ingest.pipeline.commodities_price.VnStockClient")
def test_commodities_price_pipeline_fetch_returns_empty_when_no_data(
    mock_client_class: MagicMock,
) -> None:
    """Verify fetch returns empty DataFrame when world_index returns empty."""
    mock_client = MagicMock()
    mock_client_class.return_value = mock_client
    mock_client.call_api_with_retry.return_value = pd.DataFrame()

    pipeline = CommoditiesPricePipeline(batch_date="2026-06-18")
    result_df = pipeline.fetch()

    assert result_df.empty
