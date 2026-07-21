"""Unit tests for IncomeStatementPipeline."""

from unittest.mock import MagicMock, patch

import pandas as pd
import pytest

from src.ingest.pipeline.base import DEFAULT_TICKER_SYMBOLS
from src.ingest.pipeline.income_statement import IncomeStatementPipeline


def _make_long_df(period_col: str = "2026-Q1") -> pd.DataFrame:
    """Return a minimal long-format VCI income statement DataFrame."""
    return pd.DataFrame(
        {
            "item_en": [
                "Sales",
                "Cost of sales",
                "Gross Profit",
                "General and admin expenses",
                "Operating profit/(loss)",
                "Financial income",
                "Financial expenses",
                "Net profit/(loss) after tax",
            ],
            "item": ["x"] * 8,
            "item_id": range(8),
            period_col: [100.0, 40.0, 60.0, 10.0, 50.0, 5.0, 3.0, 35.0],
        }
    )


@patch("src.ingest.pipeline.income_statement.VnStockClient")
def test_income_statement_pipeline_fetch_pivots_to_wide_format(
    mock_client_class: MagicMock,
) -> None:
    """Verify fetch transforms long-format VCI data into wide schema columns."""
    mock_client = MagicMock()
    mock_client_class.return_value = mock_client
    mock_client.call_api_with_retry.return_value = _make_long_df("2026-Q1")

    pipeline = IncomeStatementPipeline(batch_date="2026-06-18", symbols=["FPT"])
    result_df = pipeline.fetch()

    assert len(result_df) == 1
    assert result_df["ticker"].iloc[0] == "FPT"
    assert result_df["period"].iloc[0] == "Q1"
    assert result_df["year"].iloc[0] == "2026"
    assert result_df["revenue"].iloc[0] == 100.0
    assert result_df["cogs"].iloc[0] == 40.0
    assert result_df["net_profit_after_tax"].iloc[0] == 35.0


@patch("src.ingest.pipeline.income_statement.VnStockClient")
def test_income_statement_pipeline_uses_vci_source(
    mock_client_class: MagicMock,
) -> None:
    """Verify fetch calls call_api_with_retry once per symbol."""
    mock_client = MagicMock()
    mock_client_class.return_value = mock_client
    mock_client.call_api_with_retry.return_value = pd.DataFrame()

    pipeline = IncomeStatementPipeline(batch_date="2026-06-18", symbols=["FPT"])
    pipeline.fetch()

    assert mock_client.call_api_with_retry.call_count == 1


@patch("src.ingest.pipeline.income_statement.VnStockClient")
def test_income_statement_pipeline_propagates_error(
    mock_client_class: MagicMock,
) -> None:
    """Verify that API errors are propagated and not silenced."""
    mock_client = MagicMock()
    mock_client_class.return_value = mock_client
    mock_client.call_api_with_retry.side_effect = RuntimeError("VCI unavailable")

    pipeline = IncomeStatementPipeline(batch_date="2026-06-18", symbols=["FPT"])
    with pytest.raises(RuntimeError, match="VCI unavailable"):
        pipeline.fetch()


@patch("src.ingest.pipeline.income_statement.VnStockClient")
def test_income_statement_pipeline_returns_empty_when_api_returns_empty(
    mock_client_class: MagicMock,
) -> None:
    """Verify fetch returns empty DataFrame when API returns no data."""
    mock_client = MagicMock()
    mock_client_class.return_value = mock_client
    mock_client.call_api_with_retry.return_value = pd.DataFrame()

    pipeline = IncomeStatementPipeline(batch_date="2026-06-18", symbols=["FPT"])
    result_df = pipeline.fetch()

    assert result_df.empty


@patch("src.ingest.pipeline.income_statement.VnStockClient")
def test_income_statement_pipeline_returns_empty_when_no_matching_items(
    mock_client_class: MagicMock,
) -> None:
    """Verify fetch returns empty when API response has no matching item_en values.

    Banks return different line items (Net Interest Income vs Sales/COGS),
    so the pivot finds no matching rows and skips the symbol.
    """
    mock_client = MagicMock()
    mock_client_class.return_value = mock_client
    mock_client.call_api_with_retry.return_value = pd.DataFrame(
        {
            "item_en": ["Net Interest Income", "Net Fee and Commission Income"],
            "item": ["x", "x"],
            "item_id": [1, 2],
            "2026-Q1": [1.0e13, 2.0e12],
        }
    )

    pipeline = IncomeStatementPipeline(batch_date="2026-06-18", symbols=["TCB"])
    result_df = pipeline.fetch()

    assert result_df.empty


def _make_bank_long_df(period_col: str = "2026-Q1") -> pd.DataFrame:
    """Return long-format VCI income statement DataFrame with bank item_en labels."""
    return pd.DataFrame(
        {
            "item_en": [
                "Total Operating Income",
                "General and Admin Expenses",
                "Net Operating Profit Before Allowance for Credit Loss",
                "Interest and Similar Income",
                "Interest and Similar Expenses",
                "Net profit/(loss) after tax",
            ],
            "item": ["x"] * 6,
            "item_id": range(6),
            period_col: [500.0, 100.0, 300.0, 800.0, 400.0, 200.0],
        }
    )


@patch("src.ingest.pipeline.income_statement.VnStockClient")
def test_income_statement_pipeline_falls_back_to_bank_col_map(
    mock_client_class: MagicMock,
) -> None:
    """Verify fetch falls back to bank item_en labels if corporate labels miss."""
    mock_client = MagicMock()
    mock_client_class.return_value = mock_client
    mock_client.call_api_with_retry.return_value = _make_bank_long_df("2026-Q1")

    pipeline = IncomeStatementPipeline(batch_date="2026-06-18", symbols=["ACB"])
    result_df = pipeline.fetch()

    assert len(result_df) == 1
    assert result_df["ticker"].iloc[0] == "ACB"
    assert result_df["revenue"].iloc[0] == 500.0
    assert result_df["net_profit_after_tax"].iloc[0] == 200.0


@patch("src.ingest.pipeline.income_statement.VnStockClient")
def test_income_statement_pipeline_multiple_symbols_concatenated(
    mock_client_class: MagicMock,
) -> None:
    """Verify fetch concatenates results from multiple symbols."""
    mock_client = MagicMock()
    mock_client_class.return_value = mock_client
    mock_client.call_api_with_retry.side_effect = [
        _make_long_df("2026-Q1"),
        _make_long_df("2026-Q1"),
    ]

    pipeline = IncomeStatementPipeline(batch_date="2026-06-18", symbols=["FPT", "VNM"])
    result_df = pipeline.fetch()

    assert len(result_df) == 2
    assert mock_client.call_api_with_retry.call_count == 2


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

    assert mock_client.call_api_with_retry.call_count == len(DEFAULT_TICKER_SYMBOLS)
