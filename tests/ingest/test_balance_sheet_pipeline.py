"""Unit tests for BalanceSheetPipeline."""

from unittest.mock import MagicMock, patch

import pandas as pd
import pytest

from src.ingest.pipeline.balance_sheet import BalanceSheetPipeline
from src.ingest.pipeline.base import DEFAULT_TICKER_SYMBOLS


def _make_long_df(period_col: str = "2026-Q1") -> pd.DataFrame:
    """Return a minimal long-format VCI balance sheet DataFrame."""
    return pd.DataFrame(
        {
            "item_en": [
                "Total Assets",
                "CURRENT ASSETS",
                "Cash and cash equivalents",
                "Inventories, Net",
                "Liabilities",
                "Short-term borrowings",
                "Long-term borrowings",
                "Capital and reserves",
            ],
            "item": ["x"] * 8,
            "item_id": range(8),
            period_col: [1000.0, 400.0, 100.0, 80.0, 600.0, 150.0, 200.0, 400.0],
        }
    )


@patch("src.ingest.pipeline.balance_sheet.VnStockClient")
def test_balance_sheet_pipeline_fetch_pivots_to_wide_format(
    mock_client_class: MagicMock,
) -> None:
    """Verify fetch transforms long-format VCI data into wide schema columns."""
    mock_client = MagicMock()
    mock_client_class.return_value = mock_client
    mock_client.call_api_with_retry.return_value = _make_long_df("2026-Q1")

    pipeline = BalanceSheetPipeline(batch_date="2026-06-18", symbols=["FPT"])
    result_df = pipeline.fetch()

    assert len(result_df) == 1
    assert result_df["ticker"].iloc[0] == "FPT"
    assert result_df["period"].iloc[0] == "Q1"
    assert result_df["year"].iloc[0] == "2026"
    assert result_df["total_assets"].iloc[0] == 1000.0
    assert result_df["cash"].iloc[0] == 100.0
    assert result_df["equity"].iloc[0] == 400.0


@patch("src.ingest.pipeline.balance_sheet.VnStockClient")
def test_balance_sheet_pipeline_uses_vci_source(
    mock_client_class: MagicMock,
) -> None:
    """Verify fetch calls call_api_with_retry once per symbol."""
    mock_client = MagicMock()
    mock_client_class.return_value = mock_client
    mock_client.call_api_with_retry.return_value = pd.DataFrame()

    pipeline = BalanceSheetPipeline(batch_date="2026-06-18", symbols=["FPT"])
    pipeline.fetch()

    assert mock_client.call_api_with_retry.call_count == 1


@patch("src.ingest.pipeline.balance_sheet.VnStockClient")
def test_balance_sheet_pipeline_propagates_error(mock_client_class: MagicMock) -> None:
    """Verify that API errors are propagated and not silenced."""
    mock_client = MagicMock()
    mock_client_class.return_value = mock_client
    mock_client.call_api_with_retry.side_effect = RuntimeError("VCI unavailable")

    pipeline = BalanceSheetPipeline(batch_date="2026-06-18", symbols=["FPT"])
    with pytest.raises(RuntimeError, match="VCI unavailable"):
        pipeline.fetch()


@patch("src.ingest.pipeline.balance_sheet.VnStockClient")
def test_balance_sheet_pipeline_returns_empty_when_api_returns_empty(
    mock_client_class: MagicMock,
) -> None:
    """Verify fetch returns empty DataFrame when API returns no data."""
    mock_client = MagicMock()
    mock_client_class.return_value = mock_client
    mock_client.call_api_with_retry.return_value = pd.DataFrame()

    pipeline = BalanceSheetPipeline(batch_date="2026-06-18", symbols=["FPT"])
    result_df = pipeline.fetch()

    assert result_df.empty


@patch("src.ingest.pipeline.balance_sheet.VnStockClient")
def test_balance_sheet_pipeline_returns_empty_when_no_matching_items(
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

    pipeline = BalanceSheetPipeline(batch_date="2026-06-18", symbols=["FPT"])
    result_df = pipeline.fetch()

    assert result_df.empty


@patch("src.ingest.pipeline.balance_sheet.VnStockClient")
def test_balance_sheet_pipeline_multiple_symbols_concatenated(
    mock_client_class: MagicMock,
) -> None:
    """Verify fetch concatenates results from multiple symbols."""
    mock_client = MagicMock()
    mock_client_class.return_value = mock_client
    mock_client.call_api_with_retry.side_effect = [
        _make_long_df("2026-Q1"),
        _make_long_df("2026-Q1"),
    ]

    pipeline = BalanceSheetPipeline(batch_date="2026-06-18", symbols=["FPT", "VNM"])
    result_df = pipeline.fetch()

    assert len(result_df) == 2
    assert mock_client.call_api_with_retry.call_count == 2


@patch("src.ingest.pipeline.balance_sheet.VnStockClient")
def test_balance_sheet_defaults_to_vn30_when_no_symbols(
    mock_client_class: MagicMock,
) -> None:
    """Verify fetch uses DEFAULT_TICKER_SYMBOLS when symbols=[]."""
    mock_client = MagicMock()
    mock_client_class.return_value = mock_client
    mock_client.call_api_with_retry.return_value = pd.DataFrame()

    pipeline = BalanceSheetPipeline(batch_date="2026-06-18")
    pipeline.fetch()

    assert mock_client.call_api_with_retry.call_count == len(DEFAULT_TICKER_SYMBOLS)
