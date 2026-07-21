"""Unit tests for StockPriceEodPipeline."""

from unittest.mock import MagicMock, patch

import pandas as pd
import pytest

from src.ingest.pipeline.base import DEFAULT_TICKER_SYMBOLS
from src.ingest.pipeline.stock_price_eod import StockPriceEodPipeline


def _make_vnstock_row(**overrides) -> dict:
    """Return a row matching vnstock v4 VCI quote.history() column schema."""
    base = {
        "time": "2026-06-18",
        "open": 31.75,
        "high": 31.8,
        "low": 31.35,
        "close": 31.45,
        "volume": 16629400,
    }
    base.update(overrides)
    return base


@patch("src.ingest.pipeline.stock_price_eod.VnStockClient")
def test_stock_price_eod_pipeline_fetch_success(
    mock_client_class: MagicMock,
) -> None:
    """Verify fetch queries all symbols and returns combined DataFrame."""
    mock_client = MagicMock()
    mock_client_class.return_value = mock_client
    mock_client.get_stock_price_eod.return_value = pd.DataFrame([_make_vnstock_row()])

    pipeline = StockPriceEodPipeline(batch_date="2026-06-18", symbols=["TCB", "VCB"])
    result_df = pipeline.fetch()

    assert mock_client.get_stock_price_eod.call_count == 2
    assert len(result_df) == 2


@patch("src.ingest.pipeline.stock_price_eod.VnStockClient")
def test_stock_price_eod_pipeline_fetch_renames_time_to_trading_date(
    mock_client_class: MagicMock,
) -> None:
    """Verify fetch renames vnstock 'time' column to 'trading_date'."""
    mock_client = MagicMock()
    mock_client_class.return_value = mock_client
    mock_client.get_stock_price_eod.return_value = pd.DataFrame([_make_vnstock_row()])

    pipeline = StockPriceEodPipeline(batch_date="2026-06-18", symbols=["TCB"])
    result_df = pipeline.fetch()

    assert "trading_date" in result_df.columns
    assert "time" not in result_df.columns
    assert result_df["trading_date"].iloc[0] == "2026-06-18"


@patch("src.ingest.pipeline.stock_price_eod.VnStockClient")
def test_stock_price_eod_pipeline_fetch_injects_ticker(
    mock_client_class: MagicMock,
) -> None:
    """Verify fetch injects symbol as ticker when vnstock omits it."""
    mock_client = MagicMock()
    mock_client_class.return_value = mock_client
    mock_client.get_stock_price_eod.return_value = pd.DataFrame([_make_vnstock_row()])

    pipeline = StockPriceEodPipeline(batch_date="2026-06-18", symbols=["TCB"])
    result_df = pipeline.fetch()

    assert result_df["ticker"].iloc[0] == "TCB"


@patch("src.ingest.pipeline.stock_price_eod.VnStockClient")
def test_stock_price_eod_pipeline_fetch_adds_value_column(
    mock_client_class: MagicMock,
) -> None:
    """Verify fetch adds NULL value column when vnstock v4 doesn't return it."""
    mock_client = MagicMock()
    mock_client_class.return_value = mock_client
    mock_client.get_stock_price_eod.return_value = pd.DataFrame([_make_vnstock_row()])

    pipeline = StockPriceEodPipeline(batch_date="2026-06-18", symbols=["TCB"])
    result_df = pipeline.fetch()

    assert "value" in result_df.columns
    assert result_df["value"].iloc[0] is None


@patch("src.ingest.pipeline.stock_price_eod.VnStockClient")
def test_stock_price_eod_pipeline_fetch_adds_adjusted_close_column(
    mock_client_class: MagicMock,
) -> None:
    """Verify fetch adds NULL adjusted_close column when vnstock v4 omits it."""
    mock_client = MagicMock()
    mock_client_class.return_value = mock_client
    mock_client.get_stock_price_eod.return_value = pd.DataFrame([_make_vnstock_row()])

    pipeline = StockPriceEodPipeline(batch_date="2026-06-18", symbols=["TCB"])
    result_df = pipeline.fetch()

    assert "adjusted_close" in result_df.columns
    assert result_df["adjusted_close"].iloc[0] is None


@patch("src.ingest.pipeline.stock_price_eod.VnStockClient")
def test_stock_price_eod_pipeline_fetch_passes_batch_date_as_date_range(
    mock_client_class: MagicMock,
) -> None:
    """Verify fetch calls client with batch_date as both start and end."""
    mock_client = MagicMock()
    mock_client_class.return_value = mock_client
    mock_client.get_stock_price_eod.return_value = pd.DataFrame([_make_vnstock_row()])

    pipeline = StockPriceEodPipeline(batch_date="2026-06-18", symbols=["TCB"])
    pipeline.fetch()

    mock_client.get_stock_price_eod.assert_called_once_with(
        symbol="TCB",
        start_date="2026-06-18",
        end_date="2026-06-18",
    )


@patch("src.ingest.pipeline.stock_price_eod.VnStockClient")
def test_stock_price_eod_pipeline_fetch_empty(
    mock_client_class: MagicMock,
) -> None:
    """Verify fetch returns empty DataFrame when no data is returned."""
    mock_client = MagicMock()
    mock_client_class.return_value = mock_client
    mock_client.get_stock_price_eod.return_value = pd.DataFrame()

    pipeline = StockPriceEodPipeline(batch_date="2026-06-18", symbols=["TCB"])
    result_df = pipeline.fetch()

    assert result_df.empty


@patch("src.ingest.pipeline.stock_price_eod.VnStockClient")
def test_stock_price_eod_pipeline_fetch_error_propagates(
    mock_client_class: MagicMock,
) -> None:
    """Verify fetch propagates exception from the client."""
    mock_client = MagicMock()
    mock_client_class.return_value = mock_client
    mock_client.get_stock_price_eod.side_effect = ConnectionError("Timeout")

    pipeline = StockPriceEodPipeline(batch_date="2026-06-18", symbols=["TCB"])
    with pytest.raises(ConnectionError, match="Timeout"):
        pipeline.fetch()


@patch("src.ingest.pipeline.stock_price_eod.VnStockClient")
def test_stock_price_eod_pipeline_fetch_uses_default_symbols(
    mock_client_class: MagicMock,
) -> None:
    """Verify fetch falls back to DEFAULT_TICKER_SYMBOLS when symbols not given."""
    mock_client = MagicMock()
    mock_client_class.return_value = mock_client
    mock_client.get_stock_price_eod.return_value = pd.DataFrame()

    pipeline = StockPriceEodPipeline(batch_date="2026-06-18")
    pipeline.fetch()

    assert mock_client.get_stock_price_eod.call_count == len(DEFAULT_TICKER_SYMBOLS)


def test_stock_price_eod_pipeline_table_name() -> None:
    """Verify table_name matches the design spec."""
    pipeline = StockPriceEodPipeline(batch_date="2026-06-18")
    assert pipeline.table_name == "RAW_STOCK_PRICE_EOD"


def test_stock_price_eod_pipeline_schema_columns() -> None:
    """Verify schema_columns matches the design spec."""
    pipeline = StockPriceEodPipeline(batch_date="2026-06-18")
    assert pipeline.schema_columns == [
        "ticker",
        "trading_date",
        "open",
        "high",
        "low",
        "close",
        "volume",
        "value",
        "adjusted_close",
    ]
