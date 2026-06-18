"""Unit tests for the StockPriceEodPipeline ingestion flow."""

from unittest.mock import MagicMock, patch

import pandas as pd

from src.ingest.pipeline.stock_price_eod import StockPriceEodPipeline


@patch("src.ingest.pipeline.stock_price_eod.VnStockClient")
def test_stock_price_eod_pipeline_fetch(mock_client_class: MagicMock) -> None:
    """Verify that fetch coordinates correct queries to VnStockClient for all symbols."""
    mock_client = MagicMock()
    mock_client_class.return_value = mock_client

    # Mock return values for symbols
    mock_df_tcb = pd.DataFrame({"time": ["2026-06-18"], "close": [48.5]})
    mock_df_fpt = pd.DataFrame({"time": ["2026-06-18"], "close": [135.2]})

    mock_client.get_stock_price_eod.side_effect = [mock_df_tcb, mock_df_fpt]

    pipeline = StockPriceEodPipeline(
        batch_date="2026-06-18",
        symbols=["TCB", "FPT"],
    )

    result_df = pipeline.fetch()

    # Verify VnStockClient calls
    assert mock_client.get_stock_price_eod.call_count == 2
    mock_client.get_stock_price_eod.assert_any_call(
        symbol="TCB", start_date="2026-06-18", end_date="2026-06-18"
    )
    mock_client.get_stock_price_eod.assert_any_call(
        symbol="FPT", start_date="2026-06-18", end_date="2026-06-18"
    )

    # Verify merged result and symbol injection
    assert len(result_df) == 2
    assert "ticker" in result_df.columns
    assert result_df.loc[result_df["ticker"] == "TCB", "close"].values[0] == 48.5
    assert result_df.loc[result_df["ticker"] == "FPT", "close"].values[0] == 135.2
