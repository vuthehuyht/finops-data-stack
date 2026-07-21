"""Unit tests for ForeignTradingPipeline."""

import os
from unittest.mock import MagicMock, patch

import pytest

from src.ingest.pipeline.base import DEFAULT_TICKER_SYMBOLS
from src.ingest.pipeline.foreign_trading import ForeignTradingPipeline

_BATCH_DATE = "2026-06-18"

_QUOTE_TCB = {
    "date": f"{_BATCH_DATE}T00:00:00Z",
    "buyForeignQuantity": 100,
    "sellForeignQuantity": 80,
    "buyForeignValue": 5000,
    "sellForeignValue": 3000,
}


def _make_pipeline(symbols: list[str]) -> ForeignTradingPipeline:
    return ForeignTradingPipeline(batch_date=_BATCH_DATE, symbols=symbols)


@patch("src.ingest.pipeline.foreign_trading.FireAntClient")
def test_foreign_trading_pipeline_fetch(
    mock_client_class: MagicMock, monkeypatch
) -> None:
    """Verify fetch calls get_historical_quotes with correct date range and fields."""
    monkeypatch.setenv("FIREANT_EMAIL", "u@test.com")
    monkeypatch.setenv("FIREANT_PASSWORD", "pass")

    mock_client = MagicMock()
    mock_client_class.return_value = mock_client
    mock_client.get_historical_quotes.return_value = [_QUOTE_TCB]

    result_df = _make_pipeline(["TCB"]).fetch()

    mock_client.get_historical_quotes.assert_called_once_with(
        "TCB", start_date=_BATCH_DATE, end_date=_BATCH_DATE
    )
    assert result_df["ticker"].iloc[0] == "TCB"
    assert result_df["trading_date"].iloc[0] == _BATCH_DATE
    assert result_df["buy_vol"].iloc[0] == 100
    assert result_df["sell_vol"].iloc[0] == 80
    assert result_df["buy_val"].iloc[0] == 5000
    assert result_df["sell_val"].iloc[0] == 3000
    assert result_df["net_val"].iloc[0] == 2000


@patch("src.ingest.pipeline.foreign_trading.FireAntClient")
def test_foreign_trading_pipeline_returns_empty_when_no_quotes(
    mock_client_class: MagicMock, monkeypatch
) -> None:
    """Verify fetch returns empty DataFrame when FireAnt returns no quotes."""
    monkeypatch.setenv("FIREANT_EMAIL", "u@test.com")
    monkeypatch.setenv("FIREANT_PASSWORD", "pass")

    mock_client = MagicMock()
    mock_client_class.return_value = mock_client
    mock_client.get_historical_quotes.return_value = []

    result_df = _make_pipeline(["TCB"]).fetch()

    assert result_df.empty


@patch("src.ingest.pipeline.foreign_trading.FireAntClient")
def test_foreign_trading_pipeline_propagates_error(
    mock_client_class: MagicMock, monkeypatch
) -> None:
    """Verify that API errors are propagated and not silenced."""
    monkeypatch.setenv("FIREANT_EMAIL", "u@test.com")
    monkeypatch.setenv("FIREANT_PASSWORD", "pass")

    mock_client = MagicMock()
    mock_client_class.return_value = mock_client
    mock_client.get_historical_quotes.side_effect = ConnectionError("Socket closed")

    with pytest.raises(ConnectionError, match="Socket closed"):
        _make_pipeline(["TCB"]).fetch()


@patch("src.ingest.pipeline.foreign_trading.FireAntClient")
def test_foreign_trading_pipeline_multiple_symbols_concatenated(
    mock_client_class: MagicMock, monkeypatch
) -> None:
    """Verify fetch concatenates results from multiple symbols into one DataFrame."""
    monkeypatch.setenv("FIREANT_EMAIL", "u@test.com")
    monkeypatch.setenv("FIREANT_PASSWORD", "pass")

    mock_client = MagicMock()
    mock_client_class.return_value = mock_client
    quote_fpt = {**_QUOTE_TCB}
    mock_client.get_historical_quotes.side_effect = [[_QUOTE_TCB], [quote_fpt]]

    result_df = _make_pipeline(["TCB", "FPT"]).fetch()

    assert len(result_df) == 2
    assert mock_client.get_historical_quotes.call_count == 2


@patch("src.ingest.pipeline.foreign_trading.FireAntClient")
def test_foreign_trading_defaults_to_vn30_when_no_symbols(
    mock_client_class: MagicMock, monkeypatch
) -> None:
    """Verify fetch uses DEFAULT_TICKER_SYMBOLS when symbols=[]."""
    monkeypatch.setenv("FIREANT_EMAIL", "u@test.com")
    monkeypatch.setenv("FIREANT_PASSWORD", "pass")

    mock_client = MagicMock()
    mock_client_class.return_value = mock_client
    mock_client.get_historical_quotes.return_value = []

    pipeline = ForeignTradingPipeline(batch_date=_BATCH_DATE)
    pipeline.fetch()

    called_symbols = [
        call.args[0] for call in mock_client.get_historical_quotes.call_args_list
    ]
    assert called_symbols == DEFAULT_TICKER_SYMBOLS


@patch("src.ingest.pipeline.foreign_trading.FireAntClient")
def test_foreign_trading_pipeline_passes_env_credentials_to_client(
    mock_client_class: MagicMock, monkeypatch
) -> None:
    """fetch() passes FIREANT_EMAIL and FIREANT_PASSWORD to client."""
    monkeypatch.setenv("FIREANT_EMAIL", "real@user.com")
    monkeypatch.setenv("FIREANT_PASSWORD", "mock_pass")

    mock_client = MagicMock()
    mock_client_class.return_value = mock_client
    mock_client.get_historical_quotes.return_value = []

    _make_pipeline(["TCB"]).fetch()

    mock_client_class.assert_called_once_with(
        email="real@user.com",
        password="mock_pass",  # gitleaks:allow
    )


@patch("src.ingest.pipeline.foreign_trading.FireAntClient")
def test_foreign_trading_pipeline_raises_key_error_when_env_vars_missing(
    mock_client_class: MagicMock,
) -> None:
    """fetch() raises KeyError when FIREANT_EMAIL or FIREANT_PASSWORD are not set."""
    os.environ.pop("FIREANT_EMAIL", None)
    os.environ.pop("FIREANT_PASSWORD", None)

    with pytest.raises(KeyError):
        _make_pipeline(["TCB"]).fetch()
