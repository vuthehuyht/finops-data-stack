"""Unit tests for company sub-accessor ingestion pipelines."""

from unittest.mock import MagicMock, patch

import pandas as pd

from src.ingest.pipeline.base import DEFAULT_TICKER_SYMBOLS
from src.ingest.pipeline.company_profile import CompanyProfilePipeline
from src.ingest.pipeline.corporate_events import CorporateEventsPipeline
from src.ingest.pipeline.insider_transactions import InsiderTransactionsPipeline
from src.ingest.pipeline.news_articles import NewsArticlesPipeline

# ---------------------------------------------------------------------------
# CompanyProfilePipeline
# ---------------------------------------------------------------------------


@patch("src.ingest.pipeline.company_profile.Company")
@patch("src.ingest.pipeline.company_profile.VnStockClient")
def test_company_profile_pipeline_fetch(
    mock_client_class: MagicMock, mock_company_class: MagicMock
) -> None:
    """Verify fetch calls Company VCI info for each symbol and maps columns."""
    mock_client = MagicMock()
    mock_client_class.return_value = mock_client
    mock_client.call_api_with_retry.side_effect = lambda f: f()

    mock_company = MagicMock()
    mock_company_class.return_value = mock_company

    mock_df = pd.DataFrame(
        {
            "symbol": ["TCB"],
            "organ_name": ["Techcombank"],
            "sector": ["Banking"],
            "com_group_code": ["HOSE"],
            "company_profile": ["Techcombank profile info"],
        }
    )
    mock_company.info.return_value = mock_df

    pipeline = CompanyProfilePipeline(batch_date="2026-06-18", symbols=["TCB"])
    result_df = pipeline.fetch()

    mock_company_class.assert_called_once_with(source="VCI", symbol="TCB")
    assert result_df["ticker"].iloc[0] == "TCB"
    assert result_df["company_name"].iloc[0] == "Techcombank"
    assert result_df["industry"].iloc[0] == "Banking"
    assert result_df["exchange"].iloc[0] == "HOSE"
    assert result_df["description"].iloc[0] == "Techcombank profile info"


# ---------------------------------------------------------------------------
# CorporateEventsPipeline
# ---------------------------------------------------------------------------


@patch("src.ingest.pipeline.corporate_events.Company")
@patch("src.ingest.pipeline.corporate_events.VnStockClient")
def test_corporate_events_pipeline_fetch(
    mock_client_class: MagicMock, mock_company_class: MagicMock
) -> None:
    """Verify fetch calls Company VCI events for each symbol and maps columns."""
    mock_client = MagicMock()
    mock_client_class.return_value = mock_client
    mock_client.call_api_with_retry.side_effect = lambda f: f()

    mock_company = MagicMock()
    mock_company_class.return_value = mock_company

    mock_df = pd.DataFrame(
        {
            "id": ["123"],
            "ticker": ["VNM"],
            "category": ["DIVIDEND"],
            "exright_date": ["2026-07-01"],
            "record_date": ["2026-07-02"],
            "event_title_vi": ["Tra co tuc 10%"],
        }
    )
    mock_company.events.return_value = mock_df

    pipeline = CorporateEventsPipeline(batch_date="2026-06-18", symbols=["VNM"])
    result_df = pipeline.fetch()

    mock_company_class.assert_called_once_with(source="VCI", symbol="VNM")
    assert result_df["event_id"].iloc[0] == "123"
    assert result_df["ticker"].iloc[0] == "VNM"
    assert result_df["event_type"].iloc[0] == "DIVIDEND"
    assert result_df["ex_right_date"].iloc[0] == "2026-07-01"
    assert result_df["record_date"].iloc[0] == "2026-07-02"
    assert result_df["event_details"].iloc[0] == "Tra co tuc 10%"


# ---------------------------------------------------------------------------
# InsiderTransactionsPipeline
# ---------------------------------------------------------------------------


@patch("src.ingest.pipeline.insider_transactions.VnStockClient")
def test_insider_transactions_pipeline_fetch(mock_client_class: MagicMock) -> None:
    """Verify fetch calls company.insider_deals for each symbol."""
    mock_client = MagicMock()
    mock_client_class.return_value = mock_client
    mock_stock_obj = MagicMock()
    mock_client.client.stock.return_value = mock_stock_obj

    mock_df = pd.DataFrame({"deal_action": ["BUY"], "deal_quantity": [1000]})
    mock_client.call_api_with_retry.return_value = mock_df

    pipeline = InsiderTransactionsPipeline(batch_date="2026-06-18", symbols=["HPG"])
    result_df = pipeline.fetch()

    mock_client.call_api_with_retry.assert_called_once_with(
        mock_stock_obj.company.insider_deals
    )
    assert result_df["ticker"].iloc[0] == "HPG"


# ---------------------------------------------------------------------------
# NewsArticlesPipeline
# ---------------------------------------------------------------------------


@patch("src.ingest.pipeline.news_articles.VnStockClient")
def test_news_articles_pipeline_fetch(mock_client_class: MagicMock) -> None:
    """Verify fetch calls company.news for each symbol."""
    mock_client = MagicMock()
    mock_client_class.return_value = mock_client
    mock_stock_obj = MagicMock()
    mock_client.client.stock.return_value = mock_stock_obj

    mock_df = pd.DataFrame({"title": ["Q2 Results"], "publish_time": ["2026-06-18"]})
    mock_client.call_api_with_retry.return_value = mock_df

    pipeline = NewsArticlesPipeline(batch_date="2026-06-18", symbols=["FPT"])
    result_df = pipeline.fetch()

    mock_client.call_api_with_retry.assert_called_once_with(mock_stock_obj.company.news)
    assert result_df["ticker"].iloc[0] == "FPT"


@patch("src.ingest.pipeline.company_profile.Company")
@patch("src.ingest.pipeline.company_profile.VnStockClient")
def test_company_profile_defaults_to_vn30_when_no_symbols(
    mock_client_class: MagicMock,
    mock_company_class: MagicMock,
) -> None:
    # Ensure fetch uses DEFAULT_TICKER_SYMBOLS when no symbols are explicitly specified
    """Verify fetch uses DEFAULT_TICKER_SYMBOLS when symbols=[]."""
    mock_client = MagicMock()
    mock_client_class.return_value = mock_client
    mock_client.call_api_with_retry.side_effect = lambda f: f()

    mock_company = MagicMock()
    mock_company_class.return_value = mock_company
    mock_company.info.return_value = pd.DataFrame()

    pipeline = CompanyProfilePipeline(batch_date="2026-06-18")
    pipeline.fetch()

    called_symbols = [
        call.kwargs["symbol"] for call in mock_company_class.call_args_list
    ]
    assert called_symbols == DEFAULT_TICKER_SYMBOLS


@patch("src.ingest.pipeline.news_articles.VnStockClient")
def test_news_articles_defaults_to_vn30_when_no_symbols(
    mock_client_class: MagicMock,
) -> None:
    # Ensure fetch uses DEFAULT_TICKER_SYMBOLS when no symbols are explicitly specified
    """Verify fetch uses DEFAULT_TICKER_SYMBOLS when symbols=[]."""
    mock_client = MagicMock()
    mock_client_class.return_value = mock_client
    mock_client.call_api_with_retry.return_value = pd.DataFrame()

    pipeline = NewsArticlesPipeline(batch_date="2026-06-18")
    pipeline.fetch()

    called_symbols = [
        call.kwargs["symbol"] for call in mock_client.client.stock.call_args_list
    ]
    assert called_symbols == DEFAULT_TICKER_SYMBOLS


@patch("src.ingest.pipeline.corporate_events.Company")
@patch("src.ingest.pipeline.corporate_events.VnStockClient")
def test_corporate_events_defaults_to_vn30_when_no_symbols(
    mock_client_class: MagicMock, mock_company_class: MagicMock
) -> None:
    """Verify fetch uses DEFAULT_TICKER_SYMBOLS when symbols=[]."""
    mock_client = MagicMock()
    mock_client_class.return_value = mock_client
    mock_client.call_api_with_retry.side_effect = lambda f: f()

    mock_company = MagicMock()
    mock_company_class.return_value = mock_company
    mock_company.events.return_value = pd.DataFrame()

    pipeline = CorporateEventsPipeline(batch_date="2026-06-18")
    pipeline.fetch()

    called_symbols = [
        call.kwargs["symbol"] for call in mock_company_class.call_args_list
    ]
    assert called_symbols == DEFAULT_TICKER_SYMBOLS


@patch("src.ingest.pipeline.insider_transactions.VnStockClient")
def test_insider_transactions_defaults_to_vn30_when_no_symbols(
    mock_client_class: MagicMock,
) -> None:
    # Ensure fetch uses DEFAULT_TICKER_SYMBOLS when no symbols are explicitly specified
    """Verify fetch uses DEFAULT_TICKER_SYMBOLS when symbols=[]."""
    mock_client = MagicMock()
    mock_client_class.return_value = mock_client
    mock_client.call_api_with_retry.return_value = pd.DataFrame()

    pipeline = InsiderTransactionsPipeline(batch_date="2026-06-18")
    pipeline.fetch()

    called_symbols = [
        call.kwargs["symbol"] for call in mock_client.client.stock.call_args_list
    ]
    assert called_symbols == DEFAULT_TICKER_SYMBOLS
