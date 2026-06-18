"""Unit tests for AnalystReportsPipeline."""

import os
from unittest.mock import MagicMock, patch

import pytest

from src.ingest.pipeline.analyst_reports import AnalystReportsPipeline
from src.ingest.pipeline.base import DEFAULT_TICKER_SYMBOLS

_BATCH_DATE = "2026-06-18"

_REPORT_TCB = {
    "reportID": 101,
    "symbol": "TCB",
    "sourceName": "VNDirect",
    "date": _BATCH_DATE,
    "title": "TCB Q2 Outlook",
    "description": "Strong buy signal",
    "fileName": "tcb_q2.pdf",
}

_REPORT_FPT = {
    "reportID": 202,
    "symbol": "FPT",
    "sourceName": "SSI",
    "date": _BATCH_DATE,
    "title": "FPT Tech Growth",
    "description": "Neutral",
    "fileName": "fpt_tech.pdf",
}


def _make_pipeline(symbols: list[str]) -> AnalystReportsPipeline:
    return AnalystReportsPipeline(batch_date=_BATCH_DATE, symbols=symbols)


# ---------------------------------------------------------------------------
# Field mapping
# ---------------------------------------------------------------------------


@patch("src.ingest.pipeline.analyst_reports.FireAntClient")
def test_fetch_maps_fireant_fields_correctly(
    mock_client_class: MagicMock, monkeypatch
) -> None:
    """fetch() maps FireAnt ReportInfo fields to the pipeline schema."""
    monkeypatch.setenv("FIREANT_EMAIL", "u@test.com")
    monkeypatch.setenv("FIREANT_PASSWORD", "pass")

    mock_client = MagicMock()
    mock_client_class.return_value = mock_client
    mock_client.get_reports.return_value = [_REPORT_TCB]

    result = _make_pipeline(["TCB"]).fetch()

    assert result["report_id"].iloc[0] == 101
    assert result["ticker"].iloc[0] == "TCB"
    assert result["brokerage_firm"].iloc[0] == "VNDirect"
    assert result["publish_date"].iloc[0] == _BATCH_DATE
    assert result["title"].iloc[0] == "TCB Q2 Outlook"
    assert result["description"].iloc[0] == "Strong buy signal"
    assert result["file_name"].iloc[0] == "tcb_q2.pdf"


# ---------------------------------------------------------------------------
# Multi-symbol
# ---------------------------------------------------------------------------


@patch("src.ingest.pipeline.analyst_reports.FireAntClient")
def test_fetch_concatenates_reports_for_multiple_symbols(
    mock_client_class: MagicMock, monkeypatch
) -> None:
    """fetch() calls get_reports for each symbol and concatenates results."""
    monkeypatch.setenv("FIREANT_EMAIL", "u@test.com")
    monkeypatch.setenv("FIREANT_PASSWORD", "pass")

    mock_client = MagicMock()
    mock_client_class.return_value = mock_client
    mock_client.get_reports.side_effect = [[_REPORT_TCB], [_REPORT_FPT]]

    result = _make_pipeline(["TCB", "FPT"]).fetch()

    assert len(result) == 2
    assert mock_client.get_reports.call_count == 2
    mock_client.get_reports.assert_any_call(
        symbol="TCB", start_date=_BATCH_DATE, end_date=_BATCH_DATE
    )
    mock_client.get_reports.assert_any_call(
        symbol="FPT", start_date=_BATCH_DATE, end_date=_BATCH_DATE
    )


# ---------------------------------------------------------------------------
# Empty cases
# ---------------------------------------------------------------------------


@patch.dict(os.environ, {"FIREANT_EMAIL": "u@test.com", "FIREANT_PASSWORD": "pass"})
@patch("src.ingest.pipeline.analyst_reports.FireAntClient")
def test_analyst_reports_defaults_to_vn30_when_no_symbols(
    mock_client_class: MagicMock,
) -> None:
    # Ensure fetch uses DEFAULT_TICKER_SYMBOLS when no symbols are explicitly specified
    """Verify fetch uses DEFAULT_TICKER_SYMBOLS when symbols=[]."""
    mock_client = MagicMock()
    mock_client_class.return_value = mock_client
    mock_client.get_reports.return_value = []

    pipeline = AnalystReportsPipeline(batch_date="2026-06-18")
    pipeline.fetch()

    called_symbols = [
        call.kwargs["symbol"]
        for call in mock_client.get_reports.call_args_list
    ]
    assert called_symbols == DEFAULT_TICKER_SYMBOLS


@patch("src.ingest.pipeline.analyst_reports.FireAntClient")
def test_fetch_returns_empty_dataframe_when_no_reports_returned(
    mock_client_class: MagicMock, monkeypatch
) -> None:
    """fetch() returns empty DataFrame when FireAnt returns no reports."""
    monkeypatch.setenv("FIREANT_EMAIL", "u@test.com")
    monkeypatch.setenv("FIREANT_PASSWORD", "pass")

    mock_client = MagicMock()
    mock_client_class.return_value = mock_client
    mock_client.get_reports.return_value = []

    result = _make_pipeline(["TCB"]).fetch()

    assert result.empty


# ---------------------------------------------------------------------------
# Ticker fallback
# ---------------------------------------------------------------------------


@patch("src.ingest.pipeline.analyst_reports.FireAntClient")
def test_fetch_uses_symbol_as_ticker_when_api_symbol_is_missing(
    mock_client_class: MagicMock, monkeypatch
) -> None:
    """fetch() falls back to loop symbol when 'symbol' is absent in report."""
    monkeypatch.setenv("FIREANT_EMAIL", "u@test.com")
    monkeypatch.setenv("FIREANT_PASSWORD", "pass")

    mock_client = MagicMock()
    mock_client_class.return_value = mock_client
    report_without_symbol = {**_REPORT_TCB, "symbol": None}
    mock_client.get_reports.return_value = [report_without_symbol]

    result = _make_pipeline(["TCB"]).fetch()

    assert result["ticker"].iloc[0] == "TCB"


# ---------------------------------------------------------------------------
# Error propagation
# ---------------------------------------------------------------------------


@patch("src.ingest.pipeline.analyst_reports.FireAntClient")
def test_fetch_raises_when_client_raises(
    mock_client_class: MagicMock, monkeypatch
) -> None:
    """fetch() re-raises exceptions from FireAntClient.get_reports."""
    monkeypatch.setenv("FIREANT_EMAIL", "u@test.com")
    monkeypatch.setenv("FIREANT_PASSWORD", "pass")

    mock_client = MagicMock()
    mock_client_class.return_value = mock_client
    mock_client.get_reports.side_effect = ConnectionError("API unreachable")

    with pytest.raises(ConnectionError, match="API unreachable"):
        _make_pipeline(["TCB"]).fetch()


# ---------------------------------------------------------------------------
# Credentials
# ---------------------------------------------------------------------------


@patch("src.ingest.pipeline.analyst_reports.FireAntClient")
def test_fetch_passes_env_credentials_to_client(
    mock_client_class: MagicMock, monkeypatch
) -> None:
    """fetch() passes FIREANT_EMAIL and FIREANT_PASSWORD to client."""
    monkeypatch.setenv("FIREANT_EMAIL", "real@user.com")
    monkeypatch.setenv("FIREANT_PASSWORD", "secret123")

    mock_client = MagicMock()
    mock_client_class.return_value = mock_client
    mock_client.get_reports.return_value = []

    _make_pipeline(["TCB"]).fetch()

    mock_client_class.assert_called_once_with(
        email="real@user.com", password="secret123"
    )


@patch("src.ingest.pipeline.analyst_reports.FireAntClient")
def test_fetch_raises_key_error_when_env_vars_missing(
    mock_client_class: MagicMock,
) -> None:
    """fetch() raises KeyError when FIREANT_EMAIL or FIREANT_PASSWORD are not set."""
    os.environ.pop("FIREANT_EMAIL", None)
    os.environ.pop("FIREANT_PASSWORD", None)

    with pytest.raises(KeyError):
        _make_pipeline(["TCB"]).fetch()
