"""Unit tests for InterestRatesPipeline."""

import pytest

from src.ingest.pipeline.interest_rates import InterestRatesPipeline


def test_interest_rates_pipeline_fetch_raises_not_implemented() -> None:
    """Verify fetch raises NotImplementedError."""
    pipeline = InterestRatesPipeline(batch_date="2026-06-18")

    with pytest.raises(NotImplementedError):
        pipeline.fetch()


def test_interest_rates_pipeline_table_name() -> None:
    """Verify table_name matches the design spec."""
    pipeline = InterestRatesPipeline(batch_date="2026-06-18")
    assert pipeline.table_name == "RAW_INTEREST_RATES"


def test_interest_rates_pipeline_source_uri_prefix() -> None:
    """Verify source_uri_prefix identifies the data origin."""
    pipeline = InterestRatesPipeline(batch_date="2026-06-18")
    assert pipeline.source_uri_prefix == "api://vnstock/interest_rates"


def test_interest_rates_pipeline_schema_columns() -> None:
    """Verify schema_columns matches the design spec."""
    pipeline = InterestRatesPipeline(batch_date="2026-06-18")
    assert pipeline.schema_columns == ["rate_type", "date", "rate_value"]
