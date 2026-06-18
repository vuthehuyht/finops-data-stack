"""Unit tests for finance sub-accessor ingestion pipelines."""

from unittest.mock import MagicMock, patch

import pandas as pd
import pytest

from src.ingest.pipeline.balance_sheet import BalanceSheetPipeline
from src.ingest.pipeline.cashflow_statement import CashflowStatementPipeline
from src.ingest.pipeline.financial_ratios import FinancialRatiosPipeline
from src.ingest.pipeline.income_statement import IncomeStatementPipeline

# ---------------------------------------------------------------------------
# BalanceSheetPipeline
# ---------------------------------------------------------------------------


@patch("src.ingest.pipeline.balance_sheet.VnStockClient")
def test_balance_sheet_pipeline_fetch(mock_client_class: MagicMock) -> None:
    """Verify fetch calls finance.balance_sheet with correct period and year."""
    mock_client = MagicMock()
    mock_client_class.return_value = mock_client
    mock_stock_obj = MagicMock()
    mock_client.client.stock.return_value = mock_stock_obj

    mock_df = pd.DataFrame({"period": ["Q1"], "year": [2026], "total_assets": [5000]})
    mock_client.call_api_with_retry.return_value = mock_df

    pipeline = BalanceSheetPipeline(batch_date="2026-06-18", symbols=["TCB"])
    result_df = pipeline.fetch()

    mock_client.client.stock.assert_called_once_with(symbol="TCB", source="TCBS")
    mock_client.call_api_with_retry.assert_called_once_with(
        mock_stock_obj.finance.balance_sheet,
        period="quarter",
        year=2026,
    )
    assert len(result_df) == 1
    assert result_df["ticker"].iloc[0] == "TCB"


@patch("src.ingest.pipeline.balance_sheet.VnStockClient")
def test_balance_sheet_pipeline_skips_symbol_without_finance_attr(
    mock_client_class: MagicMock,
) -> None:
    """Verify that symbols where stock_obj has no finance attribute are silently skipped."""
    mock_client = MagicMock()
    mock_client_class.return_value = mock_client
    # MagicMock(spec=[]) has no attributes → hasattr(obj, 'finance') is False
    mock_client.client.stock.return_value = MagicMock(spec=[])

    pipeline = BalanceSheetPipeline(batch_date="2026-06-18", symbols=["TCB"])
    result_df = pipeline.fetch()

    mock_client.call_api_with_retry.assert_not_called()
    assert result_df.empty


@patch("src.ingest.pipeline.balance_sheet.VnStockClient")
def test_balance_sheet_pipeline_propagates_error(mock_client_class: MagicMock) -> None:
    """Verify that API errors are propagated and not silenced."""
    mock_client = MagicMock()
    mock_client_class.return_value = mock_client
    mock_client.client.stock.return_value = MagicMock()
    mock_client.call_api_with_retry.side_effect = RuntimeError("TCBS down")

    pipeline = BalanceSheetPipeline(batch_date="2026-06-18", symbols=["TCB"])
    with pytest.raises(RuntimeError, match="TCBS down"):
        pipeline.fetch()


# ---------------------------------------------------------------------------
# IncomeStatementPipeline
# ---------------------------------------------------------------------------


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

    mock_client.call_api_with_retry.assert_called_once_with(
        mock_stock_obj.finance.income_statement,
        period="quarter",
        year=2026,
    )
    assert result_df["ticker"].iloc[0] == "FPT"


# ---------------------------------------------------------------------------
# CashflowStatementPipeline
# ---------------------------------------------------------------------------


@patch("src.ingest.pipeline.cashflow_statement.VnStockClient")
def test_cashflow_statement_pipeline_fetch(mock_client_class: MagicMock) -> None:
    """Verify fetch calls finance.cash_flow with correct period and year."""
    mock_client = MagicMock()
    mock_client_class.return_value = mock_client
    mock_stock_obj = MagicMock()
    mock_client.client.stock.return_value = mock_stock_obj

    mock_df = pd.DataFrame({"period": ["Q1"], "year": [2026], "cfo": [200]})
    mock_client.call_api_with_retry.return_value = mock_df

    pipeline = CashflowStatementPipeline(batch_date="2026-06-18", symbols=["VNM"])
    result_df = pipeline.fetch()

    mock_client.call_api_with_retry.assert_called_once_with(
        mock_stock_obj.finance.cash_flow,
        period="quarter",
        year=2026,
    )
    assert result_df["ticker"].iloc[0] == "VNM"


# ---------------------------------------------------------------------------
# FinancialRatiosPipeline
# ---------------------------------------------------------------------------


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

    mock_client.call_api_with_retry.assert_called_once_with(
        mock_stock_obj.finance.ratio
    )
    assert result_df["ticker"].iloc[0] == "HPG"


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
