"""Unit tests for IncomeStatementPipeline."""

from unittest.mock import MagicMock, patch

import pandas as pd
import pytest

from src.ingest.pipeline.base import DEFAULT_TICKER_SYMBOLS
from src.ingest.pipeline.income_statement import IncomeStatementPipeline


@patch("src.ingest.pipeline.income_statement.VnStockClient")
def test_income_statement_pipeline_fetch(mock_client_class: MagicMock) -> None:
    """Verify fetch calls finance.income_statement with correct period and year."""
    mock_client = MagicMock()
    mock_client_class.return_value = mock_client
    mock_stock_obj = MagicMock()
    mock_client.client.stock.return_value = mock_stock_obj

    mock_df = pd.DataFrame({"period": ["Q1"], "year": [2026], "revenue": [100]})
    mock_client.call_api_with_retry.return_value = mock_df

    pipeline = IncomeStatementPipeline(batch_date="2026-06-18", symbols=["FPT"])
    result_df = pipeline.fetch()

    mock_client.client.stock.assert_called_once_with(symbol="FPT", source="TCBS")
    mock_client.call_api_with_retry.assert_called_once_with(
        mock_stock_obj.finance.income_statement,
        period="quarter",
        year=2026,
    )
    assert result_df["ticker"].iloc[0] == "FPT"


@patch("src.ingest.pipeline.income_statement.VnStockClient")
def test_income_statement_pipeline_skips_symbol_without_finance_attr(
    mock_client_class: MagicMock,
) -> None:
    """Verify that symbols where stock_obj has no finance attribute are skipped."""
    mock_client = MagicMock()
    mock_client_class.return_value = mock_client
    mock_client.client.stock.return_value = MagicMock(spec=[])

    pipeline = IncomeStatementPipeline(batch_date="2026-06-18", symbols=["FPT"])
    result_df = pipeline.fetch()

    mock_client.call_api_with_retry.assert_not_called()
    assert result_df.empty


@patch("src.ingest.pipeline.income_statement.VnStockClient")
def test_income_statement_pipeline_propagates_error(
    mock_client_class: MagicMock,
) -> None:
    """Verify that API errors are propagated and not silenced."""
    mock_client = MagicMock()
    mock_client_class.return_value = mock_client
    mock_client.client.stock.return_value = MagicMock()
    mock_client.call_api_with_retry.side_effect = RuntimeError("TCBS down")

    pipeline = IncomeStatementPipeline(batch_date="2026-06-18", symbols=["FPT"])
    with pytest.raises(RuntimeError, match="TCBS down"):
        pipeline.fetch()


@patch("src.ingest.pipeline.income_statement.VnStockClient")
def test_income_statement_pipeline_returns_empty_when_all_empty(
    mock_client_class: MagicMock,
) -> None:
    """Verify fetch returns empty DataFrame when all symbols return empty data."""
    mock_client = MagicMock()
    mock_client_class.return_value = mock_client
    mock_client.client.stock.return_value = MagicMock()
    mock_client.call_api_with_retry.return_value = pd.DataFrame()

    pipeline = IncomeStatementPipeline(batch_date="2026-06-18", symbols=["FPT"])
    result_df = pipeline.fetch()

    assert result_df.empty


@patch("src.ingest.pipeline.income_statement.VnStockClient")
def test_income_statement_defaults_to_vn30_when_no_symbols(
    mock_client_class: MagicMock,
) -> None:
    """Verify fetch uses DEFAULT_TICKER_SYMBOLS when symbols=[]."""
    mock_client = MagicMock()
    mock_client_class.return_value = mock_client
    mock_client.call_api_with_retry.return_value = pd.DataFrame()

    pipeline = IncomeStatementPipeline(batch_date="2026-06-18")
    pipeline.fetch()

    called_symbols = [
        call.kwargs["symbol"] for call in mock_client.client.stock.call_args_list
    ]
    assert called_symbols == DEFAULT_TICKER_SYMBOLS
