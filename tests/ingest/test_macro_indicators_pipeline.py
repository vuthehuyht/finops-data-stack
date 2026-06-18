"""Unit tests for MacroIndicatorsPipeline."""

import pytest

from src.ingest.pipeline.macro_indicators import MacroIndicatorsPipeline


def test_macro_indicators_pipeline_fetch_raises_not_implemented() -> None:
    """Verify fetch raises NotImplementedError."""
    pipeline = MacroIndicatorsPipeline(batch_date="2026-06-18")

    with pytest.raises(NotImplementedError):
        pipeline.fetch()


def test_macro_indicators_pipeline_table_name() -> None:
    """Verify table_name matches the design spec."""
    pipeline = MacroIndicatorsPipeline(batch_date="2026-06-18")
    assert pipeline.table_name == "RAW_MACRO_INDICATORS"


def test_macro_indicators_pipeline_source_uri_prefix() -> None:
    """Verify source_uri_prefix identifies the data origin."""
    pipeline = MacroIndicatorsPipeline(batch_date="2026-06-18")
    assert pipeline.source_uri_prefix == "api://vnstock/macro_indicators"


def test_macro_indicators_pipeline_schema_columns() -> None:
    """Verify schema_columns matches the design spec."""
    pipeline = MacroIndicatorsPipeline(batch_date="2026-06-18")
    assert pipeline.schema_columns == ["indicator_name", "report_date", "value", "unit"]
