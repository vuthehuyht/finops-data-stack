"""Unit tests for CashflowStatementPipeline."""

from unittest.mock import MagicMock, patch

import pandas as pd
import pytest

from src.ingest.pipeline.base import DEFAULT_TICKER_SYMBOLS
from src.ingest.pipeline.cashflow_statement import CashflowStatementPipeline


def _make_long_df(period_col: str = "2026-Q1") -> pd.DataFrame:
    """Return a minimal long-format VCI cashflow statement DataFrame."""
    return pd.DataFrame(
        {
            "item_en": [
                "Net cash inflows/(outflows) from operating activities",
                "Net cash inflows/(outflows) from investing activities",
                "Net cash inflows/(outflows) from financing activities",
                "Net increase in cash and cash equivalents",
                "Purchases of fixed assets and other long term assets",
            ],
            "item": ["x"] * 5,
            "item_id": range(5),
            period_col: [500.0, -200.0, -100.0, 200.0, -150.0],
        }
    )


@patch("src.ingest.pipeline.cashflow_statement.VnStockClient")
def test_cashflow_statement_pipeline_fetch_pivots_to_wide_format(
    mock_client_class: MagicMock,
) -> None:
    """Verify fetch transforms long-format VCI data into wide schema columns."""
    mock_client = MagicMock()
    mock_client_class.return_value = mock_client
    mock_client.call_api_with_retry.return_value = _make_long_df("2026-Q1")

    pipeline = CashflowStatementPipeline(batch_date="2026-06-18", symbols=["VNM"])
    result_df = pipeline.fetch()

    assert len(result_df) == 1
    assert result_df["ticker"].iloc[0] == "VNM"
    assert result_df["period"].iloc[0] == "Q1"
    assert result_df["year"].iloc[0] == "2026"
    assert result_df["cfo"].iloc[0] == 500.0
    assert result_df["cfi"].iloc[0] == -200.0
    assert result_df["capex"].iloc[0] == -150.0


@patch("src.ingest.pipeline.cashflow_statement.VnStockClient")
def test_cashflow_statement_pipeline_uses_vci_source(
    mock_client_class: MagicMock,
) -> None:
    """Verify fetch calls call_api_with_retry once per symbol."""
    mock_client = MagicMock()
    mock_client_class.return_value = mock_client
    mock_client.call_api_with_retry.return_value = pd.DataFrame()

    pipeline = CashflowStatementPipeline(batch_date="2026-06-18", symbols=["VNM"])
    pipeline.fetch()

    assert mock_client.call_api_with_retry.call_count == 1


@patch("src.ingest.pipeline.cashflow_statement.VnStockClient")
def test_cashflow_statement_pipeline_propagates_error(
    mock_client_class: MagicMock,
) -> None:
    """Verify that API errors are propagated and not silenced."""
    mock_client = MagicMock()
    mock_client_class.return_value = mock_client
    mock_client.call_api_with_retry.side_effect = RuntimeError("VCI unavailable")

    pipeline = CashflowStatementPipeline(batch_date="2026-06-18", symbols=["VNM"])
    with pytest.raises(RuntimeError, match="VCI unavailable"):
        pipeline.fetch()


@patch("src.ingest.pipeline.cashflow_statement.VnStockClient")
def test_cashflow_statement_pipeline_returns_empty_when_api_returns_empty(
    mock_client_class: MagicMock,
) -> None:
    """Verify fetch returns empty DataFrame when API returns no data."""
    mock_client = MagicMock()
    mock_client_class.return_value = mock_client
    mock_client.call_api_with_retry.return_value = pd.DataFrame()

    pipeline = CashflowStatementPipeline(batch_date="2026-06-18", symbols=["VNM"])
    result_df = pipeline.fetch()

    assert result_df.empty


@patch("src.ingest.pipeline.cashflow_statement.VnStockClient")
def test_cashflow_statement_pipeline_returns_empty_when_no_matching_items(
    mock_client_class: MagicMock,
) -> None:
    """Verify fetch returns empty when no item_en values match the schema mapping."""
    mock_client = MagicMock()
    mock_client_class.return_value = mock_client
    mock_client.call_api_with_retry.return_value = pd.DataFrame(
        {
            "item_en": ["Unknown Item A", "Unknown Item B"],
            "item": ["x", "x"],
            "item_id": [1, 2],
            "2026-Q1": [0.0, 0.0],
        }
    )

    pipeline = CashflowStatementPipeline(batch_date="2026-06-18", symbols=["VNM"])
    result_df = pipeline.fetch()

    assert result_df.empty


def _make_bank_long_df(period_col: str = "2026-Q1") -> pd.DataFrame:
    """Return long-format VCI cashflow statement DataFrame with bank item_en labels."""
    return pd.DataFrame(
        {
            "item_en": [
                "Net cash from operating activities",
                "Net cash from investing activities",
                "Net Increase/(Decrease) in cash and cash equivalents",
                "Purchases of fixed assets and other long term assets",
            ],
            "item": ["x"] * 4,
            "item_id": range(4),
            period_col: [500.0, -200.0, 200.0, -150.0],
        }
    )


@patch("src.ingest.pipeline.cashflow_statement.VnStockClient")
def test_cashflow_statement_pipeline_falls_back_to_bank_col_map(
    mock_client_class: MagicMock,
) -> None:
    """Verify fetch falls back to bank item_en labels if corporate labels miss."""
    mock_client = MagicMock()
    mock_client_class.return_value = mock_client
    mock_client.call_api_with_retry.return_value = _make_bank_long_df("2026-Q1")

    pipeline = CashflowStatementPipeline(batch_date="2026-06-18", symbols=["ACB"])
    result_df = pipeline.fetch()

    assert len(result_df) == 1
    assert result_df["ticker"].iloc[0] == "ACB"
    assert result_df["cfo"].iloc[0] == 500.0
    assert result_df["net_cash_flow"].iloc[0] == 200.0


@patch("src.ingest.pipeline.cashflow_statement.VnStockClient")
def test_cashflow_statement_pipeline_multiple_symbols_concatenated(
    mock_client_class: MagicMock,
) -> None:
    """Verify fetch concatenates results from multiple symbols."""
    mock_client = MagicMock()
    mock_client_class.return_value = mock_client
    mock_client.call_api_with_retry.side_effect = [
        _make_long_df("2026-Q1"),
        _make_long_df("2026-Q1"),
    ]

    pipeline = CashflowStatementPipeline(
        batch_date="2026-06-18", symbols=["VNM", "FPT"]
    )
    result_df = pipeline.fetch()

    assert len(result_df) == 2
    assert mock_client.call_api_with_retry.call_count == 2


@patch("src.ingest.pipeline.cashflow_statement.VnStockClient")
def test_cashflow_statement_defaults_to_vn30_when_no_symbols(
    mock_client_class: MagicMock,
) -> None:
    """Verify fetch uses DEFAULT_TICKER_SYMBOLS when symbols=[]."""
    mock_client = MagicMock()
    mock_client_class.return_value = mock_client
    mock_client.call_api_with_retry.return_value = pd.DataFrame()

    pipeline = CashflowStatementPipeline(batch_date="2026-06-18")
    pipeline.fetch()

    assert mock_client.call_api_with_retry.call_count == len(DEFAULT_TICKER_SYMBOLS)
