"""Unit tests for ProprietaryTradingPipeline."""

from unittest.mock import MagicMock

import pandas as pd

from src.ingest.pipeline.proprietary_trading import ProprietaryTradingPipeline


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


def test_proprietary_trading_pipeline_fetch_vndirect_success(mocker) -> None:
    """Verify fetch successfully gets data from VNDIRECT API."""
    pipeline = ProprietaryTradingPipeline(batch_date="2026-06-18", symbols=["HPG"])

    # Mock requests.get response for VNDIRECT
    mock_resp = MagicMock()
    mock_resp.status_code = 200
    mock_resp.json.return_value = {
        "data": [
            {
                "code": "HPG",
                "date": "2026-06-18T00:00:00Z",
                "totalVolBuy": 150000,
                "totalVolSell": 100000,
                "netVal": 50000000,
            }
        ]
    }
    mocker.patch("requests.get", return_value=mock_resp)

    df = pipeline.fetch()
    assert not df.empty
    assert len(df) == 1
    assert df.iloc[0]["ticker"] == "HPG"
    assert df.iloc[0]["buy_vol"] == 150000
    assert df.iloc[0]["sell_vol"] == 100000
    assert df.iloc[0]["net_val"] == 50000000


def test_proprietary_trading_pipeline_fetch_ssi_success(mocker) -> None:
    """Verify fetch successfully falls back to SSI when VNDIRECT fails."""
    pipeline = ProprietaryTradingPipeline(batch_date="2026-06-18", symbols=["HPG"])

    # Mock requests.get to raise error for VNDIRECT, then succeed for SSI
    mock_resp_vnd = MagicMock()
    mock_resp_vnd.status_code = 500

    mock_resp_ssi = MagicMock()
    mock_resp_ssi.status_code = 200
    mock_resp_ssi.json.return_value = [
        {
            "ticker": "HPG",
            "buyVolume": 200000,
            "sellVolume": 120000,
            "netValue": 80000000,
        }
    ]

    mocker.patch("requests.get", side_effect=[Exception("VND Error"), mock_resp_ssi])

    df = pipeline.fetch()
    assert not df.empty
    assert len(df) == 1
    assert df.iloc[0]["ticker"] == "HPG"
    assert df.iloc[0]["buy_vol"] == 200000
    assert df.iloc[0]["sell_vol"] == 120000
    assert df.iloc[0]["net_val"] == 80000000


def test_proprietary_trading_pipeline_mock_fallback(mocker) -> None:
    """Verify fetch falls back to mock generator when all APIs fail."""
    pipeline = ProprietaryTradingPipeline(batch_date="2026-06-18", symbols=["HPG"])

    # Mock requests.get to fail for all APIs
    mocker.patch("requests.get", side_effect=Exception("API Down"))

    # Mock vnstock Quote history to return a valid DataFrame to test EOD-based mock
    mock_df_hist = pd.DataFrame([{"volume": 1000000, "close": 30.5}])
    mock_quote_instance = MagicMock()
    mock_quote_instance.history.return_value = mock_df_hist
    mocker.patch("vnstock.api.quote.Quote", return_value=mock_quote_instance)

    df = pipeline.fetch()
    assert not df.empty
    assert len(df) == 1
    assert df.iloc[0]["ticker"] == "HPG"
    assert int(df.iloc[0]["buy_vol"]) > 0
    assert int(df.iloc[0]["sell_vol"]) > 0
    # verify source
    df_std = pipeline.standardize(df)
    assert df_std.iloc[0]["_CONATA_SOURCE"] == "mock://fallback/proprietary_trading"
