"""Unit tests for MacroIndicatorsPipeline."""

from unittest.mock import MagicMock, patch

import pytest

from src.ingest.pipeline.macro_indicators import (
    DEFAULT_INDICATOR_MAPPING,
    MacroIndicatorsPipeline,
)


@patch("src.ingest.pipeline.macro_indicators.WorldBankClient")
def test_macro_indicators_pipeline_fetch_success(
    mock_client_class: MagicMock,
) -> None:
    """Verify fetch queries all indicators and combines them into a DataFrame."""
    mock_client = MagicMock()
    mock_client_class.return_value = mock_client
    mock_client.get_indicator.return_value = [
        {"date": "2024", "value": 430000000000.0, "unit": ""},
    ]

    pipeline = MacroIndicatorsPipeline(batch_date="2026-06-18")
    result_df = pipeline.fetch()

    assert mock_client.get_indicator.call_count == len(DEFAULT_INDICATOR_MAPPING)
    assert len(result_df) == len(DEFAULT_INDICATOR_MAPPING)
    assert set(result_df["indicator_name"]) == set(DEFAULT_INDICATOR_MAPPING.keys())


@patch("src.ingest.pipeline.macro_indicators.WorldBankClient")
def test_macro_indicators_pipeline_fetch_skips_null_values(
    mock_client_class: MagicMock,
) -> None:
    """Verify fetch skips records where the API returns a null value."""
    mock_client = MagicMock()
    mock_client_class.return_value = mock_client
    mock_client.get_indicator.return_value = [
        {"date": "2024", "value": None, "unit": ""},
        {"date": "2023", "value": 410000000000.0, "unit": ""},
    ]

    pipeline = MacroIndicatorsPipeline(batch_date="2026-06-18")
    result_df = pipeline.fetch()

    # Only the non-null record per indicator should be kept (most recent non-null)
    assert all(result_df["value"].notna())


@patch("src.ingest.pipeline.macro_indicators.WorldBankClient")
def test_macro_indicators_pipeline_fetch_empty_when_all_null(
    mock_client_class: MagicMock,
) -> None:
    """Verify fetch returns empty DataFrame when all values are null."""
    mock_client = MagicMock()
    mock_client_class.return_value = mock_client
    mock_client.get_indicator.return_value = [
        {"date": "2024", "value": None, "unit": ""},
    ]

    pipeline = MacroIndicatorsPipeline(batch_date="2026-06-18")
    result_df = pipeline.fetch()

    assert result_df.empty


@patch("src.ingest.pipeline.macro_indicators.WorldBankClient")
def test_macro_indicators_pipeline_fetch_error_propagates(
    mock_client_class: MagicMock,
) -> None:
    """Verify fetch propagates exception if API client fails."""
    mock_client = MagicMock()
    mock_client_class.return_value = mock_client
    mock_client.get_indicator.side_effect = Exception("World Bank API unavailable")

    pipeline = MacroIndicatorsPipeline(batch_date="2026-06-18")
    with pytest.raises(Exception, match="World Bank API unavailable"):
        pipeline.fetch()


@patch("src.ingest.pipeline.macro_indicators.WorldBankClient")
def test_macro_indicators_pipeline_fetch_uses_correct_series_ids(
    mock_client_class: MagicMock,
) -> None:
    """Verify fetch calls get_indicator with all World Bank series IDs."""
    mock_client = MagicMock()
    mock_client_class.return_value = mock_client
    mock_client.get_indicator.return_value = []

    pipeline = MacroIndicatorsPipeline(batch_date="2026-06-18")
    pipeline.fetch()

    called_series = {call.args[0] for call in mock_client.get_indicator.call_args_list}
    assert called_series == set(DEFAULT_INDICATOR_MAPPING.values())


@patch("src.ingest.pipeline.macro_indicators.WorldBankClient")
def test_macro_indicators_pipeline_fetch_annual_date_formatted(
    mock_client_class: MagicMock,
) -> None:
    """Verify annual WB date '2024' is converted to '2024-01-01'."""
    mock_client = MagicMock()
    mock_client_class.return_value = mock_client
    mock_client.get_indicator.return_value = [
        {"date": "2024", "value": 430000000000.0, "unit": ""},
    ]

    pipeline = MacroIndicatorsPipeline(batch_date="2026-06-18")
    result_df = pipeline.fetch()

    assert (result_df["report_date"] == "2024-01-01").all()


@patch("src.ingest.pipeline.macro_indicators.WorldBankClient")
def test_macro_indicators_pipeline_fetch_value_as_string(
    mock_client_class: MagicMock,
) -> None:
    """Verify value is stored as string (Bronze convention)."""
    mock_client = MagicMock()
    mock_client_class.return_value = mock_client
    mock_client.get_indicator.return_value = [
        {"date": "2024", "value": 430000000000.0, "unit": ""},
    ]

    pipeline = MacroIndicatorsPipeline(batch_date="2026-06-18")
    result_df = pipeline.fetch()

    assert result_df["value"].dtype == object


def test_macro_indicators_pipeline_table_name() -> None:
    """Verify table_name matches the design spec."""
    pipeline = MacroIndicatorsPipeline(batch_date="2026-06-18")
    assert pipeline.table_name == "RAW_MACRO_INDICATORS"


def test_macro_indicators_pipeline_schema_columns() -> None:
    """Verify schema_columns matches the design spec."""
    pipeline = MacroIndicatorsPipeline(batch_date="2026-06-18")
    assert pipeline.schema_columns == ["indicator_name", "report_date", "value", "unit"]
