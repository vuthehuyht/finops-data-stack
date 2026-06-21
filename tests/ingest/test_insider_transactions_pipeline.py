"""Unit tests for InsiderTransactionsPipeline."""

import pytest
import pandas as pd
from unittest.mock import MagicMock

from src.ingest.pipeline.insider_transactions import InsiderTransactionsPipeline


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


def test_insider_transactions_pipeline_fetch_vnstock_success(mocker) -> None:
    """Verify fetch successfully gets data from vnstock Company API."""
    pipeline = InsiderTransactionsPipeline(batch_date="2026-06-18", symbols=["HPG"])

    # Mock vnstock Reference company insider_trading
    mock_df = pd.DataFrame(
        [
            {
                "ticker": "HPG",
                "date": "2026-06-18",
                "deal_method": "MATCHING",
                "deal_action": "BUY",
                "deal_quantity": 50000,
                "deal_price": 28500.0,
                "deal_ratio": 0.002,
            }
        ]
    )
    mock_company = MagicMock()
    mock_company.insider_trading.return_value = mock_df

    mock_ref = MagicMock()
    mock_ref.company.return_value = mock_company

    mocker.patch("vnstock.Reference", return_value=mock_ref)

    df = pipeline.fetch()
    assert not df.empty
    assert len(df) == 1
    assert df.iloc[0]["ticker"] == "HPG"
    assert df.iloc[0]["deal_action"] == "BUY"
    assert df.iloc[0]["deal_quantity"] == 50000


def test_insider_transactions_pipeline_fetch_cafef_success(mocker) -> None:
    """Verify fetch successfully falls back to CafeF when vnstock fails."""
    pipeline = InsiderTransactionsPipeline(batch_date="2026-06-18", symbols=["HPG"])

    # Mock vnstock Reference to raise error
    mocker.patch("vnstock.Reference", side_effect=Exception("API Error"))

    # Mock CafeF HTML page
    html_content = """
    <html>
    <body>
        <table>
            <tr>
                <th>Người thực hiện</th>
                <th>Chức vụ</th>
                <th>Mã CP</th>
                <th>Đăng ký</th>
                <th>Kết quả</th>
                <th>Ngày giao dịch</th>
            </tr>
            <tr>
                <td>Nguyen Van A</td>
                <td>CEO</td>
                <td>HPG</td>
                <td>Mua 50,000</td>
                <td>50,000</td>
                <td>18/06/2026</td>
            </tr>
        </table>
    </body>
    </html>
    """
    mock_resp = MagicMock()
    mock_resp.status_code = 200
    mock_resp.text = html_content
    mocker.patch("requests.get", return_value=mock_resp)

    df = pipeline.fetch()
    assert not df.empty
    assert len(df) == 1
    assert df.iloc[0]["ticker"] == "HPG"
    assert df.iloc[0]["deal_action"] == "BUY"
    assert df.iloc[0]["deal_quantity"] == "50000"


def test_insider_transactions_pipeline_mock_fallback(mocker) -> None:
    """Verify fetch falls back to mock generator when all other sources fail."""
    pipeline = InsiderTransactionsPipeline(batch_date="2026-06-18", symbols=["HPG"])

    # Fail vnstock & CafeF
    mocker.patch("vnstock.Reference", side_effect=Exception("API Error"))
    mocker.patch("requests.get", side_effect=Exception("CafeF Down"))

    # Mock random to trigger transaction generation (force random.random() < 0.10)
    mocker.patch("random.random", return_value=0.05)

    # Mock quote history
    mock_df_hist = pd.DataFrame([{"close": 29000.0}])
    mock_quote_instance = MagicMock()
    mock_quote_instance.history.return_value = mock_df_hist
    mocker.patch("vnstock.api.quote.Quote", return_value=mock_quote_instance)

    df = pipeline.fetch()
    assert not df.empty
    assert len(df) == 1
    assert df.iloc[0]["ticker"] == "HPG"
    assert float(df.iloc[0]["deal_price"]) > 0
    # verify source
    df_std = pipeline.standardize(df)
    assert df_std.iloc[0]["_CONATA_SOURCE"] == "mock://fallback/insider_trading"
