"""Unit tests for IndexPriceEodPipeline fetch logic."""

from unittest.mock import MagicMock, patch

import pandas as pd

from src.ingest.pipeline.index_price_eod import IndexPriceEodPipeline


@patch("src.ingest.pipeline.index_price_eod.VnStockClient")
def test_index_price_eod_fetch_with_custom_symbols(
    mock_client_class: MagicMock,
) -> None:
    """Verify fetch calls get_stock_price_eod for each provided symbol."""
    mock_client = MagicMock()
    mock_client_class.return_value = mock_client

    mock_df_vni = pd.DataFrame({"time": ["2026-06-18"], "close": [1200.0]})
    mock_df_hnx = pd.DataFrame({"time": ["2026-06-18"], "close": [230.0]})
    mock_client.get_stock_price_eod.side_effect = [mock_df_vni, mock_df_hnx]

    pipeline = IndexPriceEodPipeline(
        batch_date="2026-06-18", symbols=["VNINDEX", "HNXINDEX"]
    )
    result_df = pipeline.fetch()

    assert mock_client.get_stock_price_eod.call_count == 2
    mock_client.get_stock_price_eod.assert_any_call(
        symbol="VNINDEX", start_date="2026-06-18", end_date="2026-06-18"
    )
    mock_client.get_stock_price_eod.assert_any_call(
        symbol="HNXINDEX", start_date="2026-06-18", end_date="2026-06-18"
    )
    assert len(result_df) == 2
    assert "index_name" in result_df.columns
    assert "trading_date" in result_df.columns
    assert "time" not in result_df.columns


@patch("src.ingest.pipeline.index_price_eod.VnStockClient")
def test_index_price_eod_fetch_defaults_to_vnindex_hnx(
    mock_client_class: MagicMock,
) -> None:
    """Verify that no-symbols pipeline defaults to VNINDEX and HNXINDEX."""
    mock_client = MagicMock()
    mock_client_class.return_value = mock_client

    mock_df = pd.DataFrame({"time": ["2026-06-18"], "close": [1200.0]})
    mock_client.get_stock_price_eod.return_value = mock_df

    pipeline = IndexPriceEodPipeline(batch_date="2026-06-18")  # no symbols
    pipeline.fetch()

    called_symbols = [
        call.kwargs["symbol"] for call in mock_client.get_stock_price_eod.call_args_list
    ]
    assert "VNINDEX" in called_symbols
    assert "HNXINDEX" in called_symbols


@patch("src.ingest.pipeline.index_price_eod.VnStockClient")
def test_index_price_eod_fetch_returns_empty_when_all_empty(
    mock_client_class: MagicMock,
) -> None:
    """Verify fetch returns empty DataFrame when all symbols return empty."""
    mock_client = MagicMock()
    mock_client_class.return_value = mock_client
    mock_client.get_stock_price_eod.return_value = pd.DataFrame()

    pipeline = IndexPriceEodPipeline(batch_date="2026-06-18", symbols=["VNINDEX"])
    result_df = pipeline.fetch()

    assert result_df.empty


@patch("src.ingest.pipeline.index_price_eod.VnStockClient")
def test_index_price_eod_fetch_propagates_error(mock_client_class: MagicMock) -> None:
    """Verify that API errors are propagated and not silenced."""
    mock_client = MagicMock()
    mock_client_class.return_value = mock_client
    mock_client.get_stock_price_eod.side_effect = RuntimeError("API timeout")

    pipeline = IndexPriceEodPipeline(batch_date="2026-06-18", symbols=["VNINDEX"])
    import pytest

    with pytest.raises(RuntimeError, match="API timeout"):
        pipeline.fetch()
