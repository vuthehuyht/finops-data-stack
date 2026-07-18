"""Unit tests for FinancialRatiosPipeline."""

from unittest.mock import MagicMock, patch

import pandas as pd
import pytest

from src.ingest.pipeline.base import DEFAULT_TICKER_SYMBOLS
from src.ingest.pipeline.financial_ratios import FinancialRatiosPipeline


def _make_long_df(period_col: str = "2026-Q1") -> pd.DataFrame:
    """Return a minimal long-format VCI financial ratio DataFrame."""
    return pd.DataFrame(
        {
            "item_en": ["Outstanding Shares (mil)", "Market Cap"],
            "item": ["x"] * 2,
            "item_id": range(2),
            period_col: [1000.0, 50000.0],
        }
    )


@patch("src.ingest.pipeline.financial_ratios.VnstockV4")
@patch("src.ingest.pipeline.financial_ratios.VnStockClient")
def test_financial_ratios_pipeline_fetch_pivots_to_wide_format(
    mock_client_class: MagicMock, mock_vnstock_v4: MagicMock
) -> None:
    """Verify fetch transforms long-format VCI data into wide schema columns."""
    mock_client = MagicMock()
    mock_client_class.return_value = mock_client
    mock_client.call_api_with_retry.side_effect = lambda fn: fn()
    mock_vnstock_v4.return_value.stock.return_value.finance.ratio.return_value = (
        _make_long_df("2026-Q1")
    )

    pipeline = FinancialRatiosPipeline(batch_date="2026-06-18", symbols=["HPG"])
    result_df = pipeline.fetch()

    assert len(result_df) == 1
    assert result_df["ticker"].iloc[0] == "HPG"
    assert result_df["period"].iloc[0] == "Q1"
    assert result_df["year"].iloc[0] == "2026"
    assert result_df["shares_outstanding"].iloc[0] == 1000.0
    assert result_df["market_cap"].iloc[0] == 50000.0


@patch("src.ingest.pipeline.financial_ratios.VnstockV4")
@patch("src.ingest.pipeline.financial_ratios.VnStockClient")
def test_financial_ratios_pipeline_uses_vci_source(
    mock_client_class: MagicMock, mock_vnstock_v4: MagicMock
) -> None:
    """Verify fetch calls call_api_with_retry once per symbol."""
    mock_client = MagicMock()
    mock_client_class.return_value = mock_client
    mock_client.call_api_with_retry.side_effect = lambda fn: fn()
    mock_vnstock_v4.return_value.stock.return_value.finance.ratio.return_value = (
        pd.DataFrame()
    )

    pipeline = FinancialRatiosPipeline(batch_date="2026-06-18", symbols=["HPG"])
    pipeline.fetch()

    assert mock_client.call_api_with_retry.call_count == 1


@patch("src.ingest.pipeline.financial_ratios.VnstockV4")
@patch("src.ingest.pipeline.financial_ratios.VnStockClient")
def test_financial_ratios_pipeline_propagates_error(
    mock_client_class: MagicMock, mock_vnstock_v4: MagicMock
) -> None:
    """Verify that API errors are propagated and not silenced."""
    mock_client = MagicMock()
    mock_client_class.return_value = mock_client
    mock_client.call_api_with_retry.side_effect = RuntimeError("VCI down")

    pipeline = FinancialRatiosPipeline(batch_date="2026-06-18", symbols=["HPG"])
    with pytest.raises(RuntimeError, match="VCI down"):
        pipeline.fetch()


@patch("src.ingest.pipeline.financial_ratios.VnstockV4")
@patch("src.ingest.pipeline.financial_ratios.VnStockClient")
def test_financial_ratios_pipeline_returns_empty_when_all_empty(
    mock_client_class: MagicMock, mock_vnstock_v4: MagicMock
) -> None:
    """Verify fetch returns empty DataFrame when all symbols return empty data."""
    mock_client = MagicMock()
    mock_client_class.return_value = mock_client
    mock_client.call_api_with_retry.side_effect = lambda fn: fn()
    mock_vnstock_v4.return_value.stock.return_value.finance.ratio.return_value = (
        pd.DataFrame()
    )

    pipeline = FinancialRatiosPipeline(batch_date="2026-06-18", symbols=["HPG"])
    result_df = pipeline.fetch()

    assert result_df.empty


@patch("src.ingest.pipeline.financial_ratios.VnstockV4")
@patch("src.ingest.pipeline.financial_ratios.VnStockClient")
def test_financial_ratios_pipeline_returns_empty_when_no_matching_items(
    mock_client_class: MagicMock, mock_vnstock_v4: MagicMock
) -> None:
    """Verify fetch returns empty when no item_en values match the schema mapping."""
    mock_client = MagicMock()
    mock_client_class.return_value = mock_client
    mock_client.call_api_with_retry.side_effect = lambda fn: fn()
    mock_vnstock_v4.return_value.stock.return_value.finance.ratio.return_value = (
        pd.DataFrame(
            {
                "item_en": ["Unknown Item A", "Unknown Item B"],
                "item": ["x", "x"],
                "item_id": [1, 2],
                "2026-Q1": [0.0, 0.0],
            }
        )
    )

    pipeline = FinancialRatiosPipeline(batch_date="2026-06-18", symbols=["HPG"])
    result_df = pipeline.fetch()

    assert result_df.empty


@patch("src.ingest.pipeline.financial_ratios.VnstockV4")
@patch("src.ingest.pipeline.financial_ratios.VnStockClient")
def test_financial_ratios_pipeline_multiple_symbols_concatenated(
    mock_client_class: MagicMock, mock_vnstock_v4: MagicMock
) -> None:
    """Verify fetch concatenates results from multiple symbols."""
    mock_client = MagicMock()
    mock_client_class.return_value = mock_client
    mock_client.call_api_with_retry.side_effect = lambda fn: fn()
    mock_vnstock_v4.return_value.stock.return_value.finance.ratio.return_value = (
        _make_long_df("2026-Q1")
    )

    pipeline = FinancialRatiosPipeline(batch_date="2026-06-18", symbols=["HPG", "VNM"])
    result_df = pipeline.fetch()

    assert len(result_df) == 2
    assert mock_client.call_api_with_retry.call_count == 2


@patch("src.ingest.pipeline.financial_ratios.VnstockV4")
@patch("src.ingest.pipeline.financial_ratios.VnStockClient")
def test_financial_ratios_defaults_to_vn30_when_no_symbols(
    mock_client_class: MagicMock, mock_vnstock_v4: MagicMock
) -> None:
    """Verify fetch uses DEFAULT_TICKER_SYMBOLS when symbols=[]."""
    mock_client = MagicMock()
    mock_client_class.return_value = mock_client
    mock_client.call_api_with_retry.side_effect = lambda fn: fn()
    mock_vnstock_v4.return_value.stock.return_value.finance.ratio.return_value = (
        pd.DataFrame()
    )

    pipeline = FinancialRatiosPipeline(batch_date="2026-06-18")
    pipeline.fetch()

    assert mock_client.call_api_with_retry.call_count == len(DEFAULT_TICKER_SYMBOLS)
