"""Unit tests for ProprietaryTradingPipeline."""

import os
from unittest.mock import MagicMock, patch

import pytest

from src.ingest.pipeline.base import DEFAULT_TICKER_SYMBOLS
from src.ingest.pipeline.proprietary_trading import ProprietaryTradingPipeline

_BATCH_DATE = "2026-06-18"

_QUOTE_HPG = {
    "date": f"{_BATCH_DATE}T00:00:00Z",
    "propTradingNetValue": 50000000,
}


def _make_pipeline(symbols: list[str]) -> ProprietaryTradingPipeline:
    return ProprietaryTradingPipeline(batch_date=_BATCH_DATE, symbols=symbols)


def test_proprietary_trading_pipeline_table_name() -> None:
    """Verify table_name matches the design spec."""
    pipeline = ProprietaryTradingPipeline(batch_date=_BATCH_DATE)
    assert pipeline.table_name == "RAW_PROPRIETARY_TRADING"


def test_proprietary_trading_pipeline_schema_columns() -> None:
    """Verify schema_columns matches the design spec."""
    pipeline = ProprietaryTradingPipeline(batch_date=_BATCH_DATE)
    assert pipeline.schema_columns == [
        "ticker",
        "trading_date",
        "buy_vol",
        "sell_vol",
        "net_val",
    ]


@patch("src.ingest.pipeline.proprietary_trading.FireAntClient")
def test_proprietary_trading_pipeline_fetch(
    mock_client_class: MagicMock, monkeypatch
) -> None:
    """Verify fetch calls get_historical_quotes and maps propTradingNetValue."""
    monkeypatch.setenv("FIREANT_EMAIL", "u@test.com")
    monkeypatch.setenv("FIREANT_PASSWORD", "pass")

    mock_client = MagicMock()
    mock_client_class.return_value = mock_client
    mock_client.get_historical_quotes.return_value = [_QUOTE_HPG]

    result_df = _make_pipeline(["HPG"]).fetch()

    mock_client.get_historical_quotes.assert_called_once_with(
        "HPG", start_date=_BATCH_DATE, end_date=_BATCH_DATE
    )
    assert result_df["ticker"].iloc[0] == "HPG"
    assert result_df["trading_date"].iloc[0] == _BATCH_DATE
    assert result_df["net_val"].iloc[0] == "50000000"


@patch("src.ingest.pipeline.proprietary_trading.FireAntClient")
def test_proprietary_trading_pipeline_skips_zero_net_value(
    mock_client_class: MagicMock, monkeypatch
) -> None:
    """Verify quotes with propTradingNetValue == 0 are skipped."""
    monkeypatch.setenv("FIREANT_EMAIL", "u@test.com")
    monkeypatch.setenv("FIREANT_PASSWORD", "pass")

    mock_client = MagicMock()
    mock_client_class.return_value = mock_client
    mock_client.get_historical_quotes.return_value = [
        {**_QUOTE_HPG, "propTradingNetValue": 0}
    ]

    result_df = _make_pipeline(["HPG"]).fetch()

    assert result_df.empty


@patch("src.ingest.pipeline.proprietary_trading.FireAntClient")
def test_proprietary_trading_pipeline_returns_empty_when_no_quotes(
    mock_client_class: MagicMock, monkeypatch
) -> None:
    """Verify fetch returns empty DataFrame when FireAnt returns no quotes."""
    monkeypatch.setenv("FIREANT_EMAIL", "u@test.com")
    monkeypatch.setenv("FIREANT_PASSWORD", "pass")

    mock_client = MagicMock()
    mock_client_class.return_value = mock_client
    mock_client.get_historical_quotes.return_value = []

    result_df = _make_pipeline(["HPG"]).fetch()

    assert result_df.empty


@patch("src.ingest.pipeline.proprietary_trading.FireAntClient")
def test_proprietary_trading_pipeline_propagates_error(
    mock_client_class: MagicMock, monkeypatch
) -> None:
    """Verify that API errors are propagated and not silenced."""
    monkeypatch.setenv("FIREANT_EMAIL", "u@test.com")
    monkeypatch.setenv("FIREANT_PASSWORD", "pass")

    mock_client = MagicMock()
    mock_client_class.return_value = mock_client
    mock_client.get_historical_quotes.side_effect = ConnectionError("Socket closed")

    with pytest.raises(ConnectionError, match="Socket closed"):
        _make_pipeline(["HPG"]).fetch()


@patch("src.ingest.pipeline.proprietary_trading.FireAntClient")
def test_proprietary_trading_defaults_to_vn30_when_no_symbols(
    mock_client_class: MagicMock, monkeypatch
) -> None:
    """Verify fetch uses DEFAULT_TICKER_SYMBOLS when symbols=[]."""
    monkeypatch.setenv("FIREANT_EMAIL", "u@test.com")
    monkeypatch.setenv("FIREANT_PASSWORD", "pass")

    mock_client = MagicMock()
    mock_client_class.return_value = mock_client
    mock_client.get_historical_quotes.return_value = []

    pipeline = ProprietaryTradingPipeline(batch_date=_BATCH_DATE)
    pipeline.fetch()

    called_symbols = [
        call.args[0] for call in mock_client.get_historical_quotes.call_args_list
    ]
    assert called_symbols == DEFAULT_TICKER_SYMBOLS


@patch("src.ingest.pipeline.proprietary_trading.FireAntClient")
def test_proprietary_trading_pipeline_raises_key_error_when_env_vars_missing(
    mock_client_class: MagicMock,
) -> None:
    """fetch() raises KeyError when FIREANT_EMAIL or FIREANT_PASSWORD are not set."""
    os.environ.pop("FIREANT_EMAIL", None)
    os.environ.pop("FIREANT_PASSWORD", None)

    with pytest.raises(KeyError):
        _make_pipeline(["HPG"]).fetch()


def test_proprietary_trading_pipeline_standardize_drops_actual_source() -> None:
    """standardize() must drop the _actual_source column if present."""
    import pandas as pd

    pipeline = ProprietaryTradingPipeline(batch_date=_BATCH_DATE)
    df = pd.DataFrame(
        {
            "ticker": ["HPG"],
            "trading_date": [_BATCH_DATE],
            "buy_vol": ["0"],
            "sell_vol": ["0"],
            "net_val": ["50000000"],
            "_actual_source": ["fireant"],
        }
    )
    result = pipeline.standardize(df)
    assert "_actual_source" not in result.columns


def test_proprietary_trading_pipeline_standardize_without_actual_source() -> None:
    """standardize() must not error when _actual_source column is absent."""
    import pandas as pd

    pipeline = ProprietaryTradingPipeline(batch_date=_BATCH_DATE)
    df = pd.DataFrame(
        {
            "ticker": ["HPG"],
            "trading_date": [_BATCH_DATE],
            "buy_vol": ["0"],
            "sell_vol": ["0"],
            "net_val": ["50000000"],
        }
    )
    result = pipeline.standardize(df)
    assert not result.empty
