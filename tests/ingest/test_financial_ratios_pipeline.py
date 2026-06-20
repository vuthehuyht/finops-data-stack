"""Unit tests for FinancialRatiosPipeline."""

from unittest.mock import MagicMock, patch

import pandas as pd
import pytest

from src.ingest.pipeline.base import DEFAULT_TICKER_SYMBOLS
from src.ingest.pipeline.financial_ratios import FinancialRatiosPipeline


@patch("src.ingest.pipeline.financial_ratios.VnStockClient")
def test_financial_ratios_pipeline_fetch(mock_client_class: MagicMock) -> None:
    """Verify fetch calls finance.ratio with no extra kwargs."""
    mock_client = MagicMock()
    mock_client_class.return_value = mock_client
    mock_stock_obj = MagicMock()
    mock_client.client.stock.return_value = mock_stock_obj

    mock_df = pd.DataFrame({"date": ["2026-06-18"], "market_cap": [50000]})
    mock_client.call_api_with_retry.return_value = mock_df

    pipeline = FinancialRatiosPipeline(batch_date="2026-06-18", symbols=["HPG"])
    result_df = pipeline.fetch()

    mock_client.client.stock.assert_called_once_with(symbol="HPG", source="TCBS")
    mock_client.call_api_with_retry.assert_called_once_with(
        mock_stock_obj.finance.ratio
    )
    assert result_df["ticker"].iloc[0] == "HPG"


@patch("src.ingest.pipeline.financial_ratios.VnStockClient")
def test_financial_ratios_pipeline_skips_symbol_without_finance_attr(
    mock_client_class: MagicMock,
) -> None:
    """Verify that symbols where stock_obj has no finance attribute are skipped."""
    mock_client = MagicMock()
    mock_client_class.return_value = mock_client
    mock_client.client.stock.return_value = MagicMock(spec=[])

    pipeline = FinancialRatiosPipeline(batch_date="2026-06-18", symbols=["HPG"])
    result_df = pipeline.fetch()

    mock_client.call_api_with_retry.assert_not_called()
    assert result_df.empty


@patch("src.ingest.pipeline.financial_ratios.VnStockClient")
def test_financial_ratios_pipeline_propagates_error(
    mock_client_class: MagicMock,
) -> None:
    """Verify that API errors are propagated and not silenced."""
    mock_client = MagicMock()
    mock_client_class.return_value = mock_client
    mock_client.client.stock.return_value = MagicMock()
    mock_client.call_api_with_retry.side_effect = RuntimeError("TCBS down")

    pipeline = FinancialRatiosPipeline(batch_date="2026-06-18", symbols=["HPG"])
    with pytest.raises(RuntimeError, match="TCBS down"):
        pipeline.fetch()


@patch("src.ingest.pipeline.financial_ratios.VnStockClient")
def test_financial_ratios_pipeline_returns_empty_when_all_empty(
    mock_client_class: MagicMock,
) -> None:
    """Verify fetch returns empty DataFrame when all symbols return empty data."""
    mock_client = MagicMock()
    mock_client_class.return_value = mock_client
    mock_client.client.stock.return_value = MagicMock()
    mock_client.call_api_with_retry.return_value = pd.DataFrame()

    pipeline = FinancialRatiosPipeline(batch_date="2026-06-18", symbols=["HPG"])
    result_df = pipeline.fetch()

    assert result_df.empty


@patch("src.ingest.pipeline.financial_ratios.VnStockClient")
def test_financial_ratios_defaults_to_vn30_when_no_symbols(
    mock_client_class: MagicMock,
) -> None:
    """Verify fetch uses DEFAULT_TICKER_SYMBOLS when symbols=[]."""
    mock_client = MagicMock()
    mock_client_class.return_value = mock_client
    mock_client.call_api_with_retry.return_value = pd.DataFrame()

    pipeline = FinancialRatiosPipeline(batch_date="2026-06-18")
    pipeline.fetch()

    called_symbols = [
        call.kwargs["symbol"] for call in mock_client.client.stock.call_args_list
    ]
    assert called_symbols == DEFAULT_TICKER_SYMBOLS
