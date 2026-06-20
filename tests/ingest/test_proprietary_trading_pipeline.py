"""Unit tests for ProprietaryTradingPipeline."""

import pytest

from src.ingest.pipeline.proprietary_trading import ProprietaryTradingPipeline


def test_proprietary_trading_pipeline_fetch_raises_not_implemented() -> None:
    """Verify fetch raises NotImplementedError until a paid data source is available."""
    pipeline = ProprietaryTradingPipeline(batch_date="2026-06-18", symbols=["VNM"])
    with pytest.raises(NotImplementedError, match="paid data source"):
        pipeline.fetch()


def test_proprietary_trading_pipeline_table_name() -> None:
    """Verify table_name matches the design spec."""
    pipeline = ProprietaryTradingPipeline(batch_date="2026-06-18")
    assert pipeline.table_name == "RAW_PROPRIETARY_TRADING"


def test_proprietary_trading_pipeline_schema_columns() -> None:
    """Verify schema_columns matches the design spec."""
    pipeline = ProprietaryTradingPipeline(batch_date="2026-06-18")
    assert pipeline.schema_columns == [
        "ticker",
        "trading_date",
        "buy_vol",
        "sell_vol",
        "net_val",
    ]
