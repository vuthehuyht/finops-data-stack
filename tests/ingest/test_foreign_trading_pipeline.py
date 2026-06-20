"""Unit tests for ForeignTradingPipeline."""

from unittest.mock import MagicMock, patch

import pandas as pd
import pytest

from src.ingest.pipeline.base import DEFAULT_TICKER_SYMBOLS
from src.ingest.pipeline.foreign_trading import ForeignTradingPipeline


@patch("src.ingest.pipeline.foreign_trading.VnStockClient")
def test_foreign_trading_pipeline_fetch(mock_client_class: MagicMock) -> None:
    """Verify fetch calls trading.foreign_flow with correct date range."""
    mock_client = MagicMock()
    mock_client_class.return_value = mock_client
    mock_stock_obj = MagicMock()
    mock_client.client.stock.return_value = mock_stock_obj

    mock_df = pd.DataFrame({"buy_vol": [100], "sell_vol": [80], "net_val": [500]})
    mock_client.call_api_with_retry.return_value = mock_df

    pipeline = ForeignTradingPipeline(batch_date="2026-06-18", symbols=["TCB"])
    result_df = pipeline.fetch()

    mock_client.client.stock.assert_called_once_with(symbol="TCB", source="TCBS")
    mock_client.call_api_with_retry.assert_called_once_with(
        mock_stock_obj.trading.foreign_flow,
        start="2026-06-18",
        end="2026-06-18",
    )
    assert result_df["ticker"].iloc[0] == "TCB"


@patch("src.ingest.pipeline.foreign_trading.VnStockClient")
def test_foreign_trading_pipeline_skips_symbol_without_trading_attr(
    mock_client_class: MagicMock,
) -> None:
    """Verify that symbols where stock_obj has no trading attribute are skipped."""
    mock_client = MagicMock()
    mock_client_class.return_value = mock_client
    mock_client.client.stock.return_value = MagicMock(spec=[])

    pipeline = ForeignTradingPipeline(batch_date="2026-06-18", symbols=["TCB"])
    result_df = pipeline.fetch()

    mock_client.call_api_with_retry.assert_not_called()
    assert result_df.empty


@patch("src.ingest.pipeline.foreign_trading.VnStockClient")
def test_foreign_trading_pipeline_propagates_error(
    mock_client_class: MagicMock,
) -> None:
    """Verify that API errors are propagated and not silenced."""
    mock_client = MagicMock()
    mock_client_class.return_value = mock_client
    mock_client.client.stock.return_value = MagicMock()
    mock_client.call_api_with_retry.side_effect = ConnectionError("Socket closed")

    pipeline = ForeignTradingPipeline(batch_date="2026-06-18", symbols=["TCB"])
    with pytest.raises(ConnectionError, match="Socket closed"):
        pipeline.fetch()


@patch("src.ingest.pipeline.foreign_trading.VnStockClient")
def test_foreign_trading_pipeline_returns_empty_when_all_empty(
    mock_client_class: MagicMock,
) -> None:
    """Verify fetch returns empty DataFrame when all symbols return empty data."""
    mock_client = MagicMock()
    mock_client_class.return_value = mock_client
    mock_client.client.stock.return_value = MagicMock()
    mock_client.call_api_with_retry.return_value = pd.DataFrame()

    pipeline = ForeignTradingPipeline(batch_date="2026-06-18", symbols=["TCB"])
    result_df = pipeline.fetch()

    assert result_df.empty


@patch("src.ingest.pipeline.foreign_trading.VnStockClient")
def test_foreign_trading_pipeline_multiple_symbols_concatenated(
    mock_client_class: MagicMock,
) -> None:
    """Verify fetch concatenates results from multiple symbols into one DataFrame."""
    mock_client = MagicMock()
    mock_client_class.return_value = mock_client
    mock_client.client.stock.return_value = MagicMock()

    mock_df_tcb = pd.DataFrame({"buy_vol": [100], "ticker": ["TCB"]})
    mock_df_fpt = pd.DataFrame({"buy_vol": [200], "ticker": ["FPT"]})
    mock_client.call_api_with_retry.side_effect = [mock_df_tcb, mock_df_fpt]

    pipeline = ForeignTradingPipeline(batch_date="2026-06-18", symbols=["TCB", "FPT"])
    result_df = pipeline.fetch()

    assert len(result_df) == 2
    assert mock_client.call_api_with_retry.call_count == 2


@patch("src.ingest.pipeline.foreign_trading.VnStockClient")
def test_foreign_trading_defaults_to_vn30_when_no_symbols(
    mock_client_class: MagicMock,
) -> None:
    """Verify fetch uses DEFAULT_TICKER_SYMBOLS when symbols=[]."""
    mock_client = MagicMock()
    mock_client_class.return_value = mock_client
    mock_client.call_api_with_retry.return_value = pd.DataFrame()

    pipeline = ForeignTradingPipeline(batch_date="2026-06-18")
    pipeline.fetch()

    called_symbols = [
        call.kwargs["symbol"] for call in mock_client.client.stock.call_args_list
    ]
    assert called_symbols == DEFAULT_TICKER_SYMBOLS
