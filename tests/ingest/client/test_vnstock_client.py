"""Unit tests for VnStockClient."""

from unittest.mock import MagicMock, patch

import pandas as pd
import pytest

from src.ingest.client.base_client import BaseClient
from src.ingest.client.vnstock_client import VnStockClient


@patch("src.ingest.client.vnstock_client.Vnstock")
def test_vnstock_client_get_stock_price_eod_calls_correct_symbol_and_source(
    mock_vnstock_class: MagicMock,
) -> None:
    """Verify get_stock_price_eod calls stock() with the given symbol and source."""
    mock_vnstock = MagicMock()
    mock_vnstock_class.return_value = mock_vnstock
    mock_stock_obj = MagicMock()
    mock_vnstock.stock.return_value = mock_stock_obj
    mock_stock_obj.quote.history.return_value = pd.DataFrame()

    client = VnStockClient(request_delay_seconds=0.0)
    client.get_stock_price_eod(
        symbol="TCB", start_date="2026-06-18", end_date="2026-06-18"
    )

    mock_vnstock.stock.assert_called_once_with(symbol="TCB", source="TCBS")


@patch("src.ingest.client.vnstock_client.Vnstock")
def test_vnstock_client_get_stock_price_eod_calls_quote_history_with_dates(
    mock_vnstock_class: MagicMock,
) -> None:
    """Verify get_stock_price_eod passes start and end dates to quote.history."""
    mock_vnstock = MagicMock()
    mock_vnstock_class.return_value = mock_vnstock
    mock_stock_obj = MagicMock()
    mock_vnstock.stock.return_value = mock_stock_obj

    mock_df = pd.DataFrame({"time": ["2026-06-18"], "close": [48.5]})
    mock_stock_obj.quote.history.return_value = mock_df

    client = VnStockClient(request_delay_seconds=0.0)
    result_df = client.get_stock_price_eod(
        symbol="TCB", start_date="2026-06-18", end_date="2026-06-18"
    )

    mock_stock_obj.quote.history.assert_called_once_with(
        start="2026-06-18", end="2026-06-18"
    )
    assert len(result_df) == 1
    assert result_df["close"].iloc[0] == 48.5


@patch("src.ingest.client.vnstock_client.Vnstock")
def test_vnstock_client_get_stock_price_eod_default_source_is_tcbs(
    mock_vnstock_class: MagicMock,
) -> None:
    """Verify get_stock_price_eod defaults to source='TCBS' when not specified."""
    mock_vnstock = MagicMock()
    mock_vnstock_class.return_value = mock_vnstock
    mock_vnstock.stock.return_value = MagicMock()

    client = VnStockClient(request_delay_seconds=0.0)
    client.get_stock_price_eod(
        symbol="FPT", start_date="2026-06-18", end_date="2026-06-18"
    )

    _, call_kwargs = mock_vnstock.stock.call_args
    assert call_kwargs["source"] == "TCBS"


@patch("src.ingest.client.vnstock_client.Vnstock")
def test_vnstock_client_get_stock_price_eod_accepts_custom_source(
    mock_vnstock_class: MagicMock,
) -> None:
    """Verify get_stock_price_eod passes a custom source to stock()."""
    mock_vnstock = MagicMock()
    mock_vnstock_class.return_value = mock_vnstock
    mock_vnstock.stock.return_value = MagicMock()

    client = VnStockClient(request_delay_seconds=0.0)
    client.get_stock_price_eod(
        symbol="VNM", start_date="2026-06-18", end_date="2026-06-18", source="SSI"
    )

    _, call_kwargs = mock_vnstock.stock.call_args
    assert call_kwargs["source"] == "SSI"


@patch("src.ingest.client.vnstock_client.Vnstock")
def test_vnstock_client_get_stock_price_eod_propagates_error(
    mock_vnstock_class: MagicMock,
) -> None:
    """Verify get_stock_price_eod propagates API errors after retries."""
    mock_vnstock = MagicMock()
    mock_vnstock_class.return_value = mock_vnstock
    mock_stock_obj = MagicMock()
    mock_vnstock.stock.return_value = mock_stock_obj
    mock_stock_obj.quote.history.side_effect = RuntimeError("API unavailable")

    client = VnStockClient(request_delay_seconds=0.0)
    with pytest.raises(RuntimeError, match="API unavailable"):
        client.get_stock_price_eod(
            symbol="HPG", start_date="2026-06-18", end_date="2026-06-18"
        )


@patch("src.ingest.client.vnstock_client.Vnstock")
def test_vnstock_client_inherits_base_client(_mock_vnstock_class: MagicMock) -> None:
    """Verify VnStockClient is a subclass of BaseClient."""
    client = VnStockClient()
    assert isinstance(client, BaseClient)
