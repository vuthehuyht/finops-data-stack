"""Unit tests for InsiderTransactionsPipeline."""

import pytest

from src.ingest.pipeline.insider_transactions import InsiderTransactionsPipeline


def test_insider_transactions_pipeline_fetch_raises_not_implemented() -> None:
    """Verify fetch raises NotImplementedError until a working source is found."""
    pipeline = InsiderTransactionsPipeline(batch_date="2026-06-18")

    with pytest.raises(NotImplementedError):
        pipeline.fetch()


def test_insider_transactions_pipeline_table_name() -> None:
    """Verify table_name matches the design spec."""
    pipeline = InsiderTransactionsPipeline(batch_date="2026-06-18")
    assert pipeline.table_name == "RAW_INSIDER_TRANSACTIONS"


def test_insider_transactions_pipeline_source_uri_prefix() -> None:
    """Verify source_uri_prefix identifies the data origin."""
    pipeline = InsiderTransactionsPipeline(batch_date="2026-06-18")
    assert pipeline.source_uri_prefix == "api://vnstock/insider_transactions"


def test_insider_transactions_pipeline_schema_columns() -> None:
    """Verify schema_columns reflects TCBS deal format."""
    pipeline = InsiderTransactionsPipeline(batch_date="2026-06-18")
    assert pipeline.schema_columns == [
        "ticker",
        "deal_announce_date",
        "deal_method",
        "deal_action",
        "deal_quantity",
        "deal_price",
        "deal_ratio",
    ]
