"""Unit tests for company sub-accessor ingestion pipelines."""

from unittest.mock import MagicMock, patch

import pandas as pd

from src.ingest.pipeline.analyst_reports import AnalystReportsPipeline
from src.ingest.pipeline.company_profile import CompanyProfilePipeline
from src.ingest.pipeline.corporate_events import CorporateEventsPipeline
from src.ingest.pipeline.insider_transactions import InsiderTransactionsPipeline
from src.ingest.pipeline.news_articles import NewsArticlesPipeline

# ---------------------------------------------------------------------------
# CompanyProfilePipeline
# ---------------------------------------------------------------------------


@patch("src.ingest.pipeline.company_profile.VnStockClient")
def test_company_profile_pipeline_fetch(mock_client_class: MagicMock) -> None:
    """Verify fetch calls company.profile for each symbol and injects ticker."""
    mock_client = MagicMock()
    mock_client_class.return_value = mock_client
    mock_stock_obj = MagicMock()
    mock_client.client.stock.return_value = mock_stock_obj

    mock_df = pd.DataFrame({"company_name": ["Techcombank"], "industry": ["Banking"]})
    mock_client.call_api_with_retry.return_value = mock_df

    pipeline = CompanyProfilePipeline(batch_date="2026-06-18", symbols=["TCB"])
    result_df = pipeline.fetch()

    mock_client.call_api_with_retry.assert_called_once_with(
        mock_stock_obj.company.profile
    )
    assert result_df["ticker"].iloc[0] == "TCB"


@patch("src.ingest.pipeline.company_profile.VnStockClient")
def test_company_profile_pipeline_skips_symbol_without_company_attr(
    mock_client_class: MagicMock,
) -> None:
    """Verify that symbols where stock_obj has no company attribute are silently skipped."""
    mock_client = MagicMock()
    mock_client_class.return_value = mock_client
    mock_client.client.stock.return_value = MagicMock(spec=[])

    pipeline = CompanyProfilePipeline(batch_date="2026-06-18", symbols=["TCB"])
    result_df = pipeline.fetch()

    mock_client.call_api_with_retry.assert_not_called()
    assert result_df.empty


# ---------------------------------------------------------------------------
# CorporateEventsPipeline
# ---------------------------------------------------------------------------


@patch("src.ingest.pipeline.corporate_events.VnStockClient")
def test_corporate_events_pipeline_fetch(mock_client_class: MagicMock) -> None:
    """Verify fetch calls company.events for each symbol."""
    mock_client = MagicMock()
    mock_client_class.return_value = mock_client
    mock_stock_obj = MagicMock()
    mock_client.client.stock.return_value = mock_stock_obj

    mock_df = pd.DataFrame(
        {"event_type": ["DIVIDEND"], "ex_right_date": ["2026-07-01"]}
    )
    mock_client.call_api_with_retry.return_value = mock_df

    pipeline = CorporateEventsPipeline(batch_date="2026-06-18", symbols=["VNM"])
    result_df = pipeline.fetch()

    mock_client.call_api_with_retry.assert_called_once_with(
        mock_stock_obj.company.events
    )
    assert result_df["ticker"].iloc[0] == "VNM"


# ---------------------------------------------------------------------------
# InsiderTransactionsPipeline
# ---------------------------------------------------------------------------


@patch("src.ingest.pipeline.insider_transactions.VnStockClient")
def test_insider_transactions_pipeline_fetch(mock_client_class: MagicMock) -> None:
    """Verify fetch calls company.insider_transactions for each symbol."""
    mock_client = MagicMock()
    mock_client_class.return_value = mock_client
    mock_stock_obj = MagicMock()
    mock_client.client.stock.return_value = mock_stock_obj

    mock_df = pd.DataFrame({"insider_name": ["CEO"], "action": ["BUY"]})
    mock_client.call_api_with_retry.return_value = mock_df

    pipeline = InsiderTransactionsPipeline(batch_date="2026-06-18", symbols=["HPG"])
    result_df = pipeline.fetch()

    mock_client.call_api_with_retry.assert_called_once_with(
        mock_stock_obj.company.insider_transactions
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


# ---------------------------------------------------------------------------
# AnalystReportsPipeline
# ---------------------------------------------------------------------------


@patch("src.ingest.pipeline.analyst_reports.VnStockClient")
def test_analyst_reports_pipeline_fetch(mock_client_class: MagicMock) -> None:
    """Verify fetch calls company.news (fallback source) for each symbol."""
    mock_client = MagicMock()
    mock_client_class.return_value = mock_client
    mock_stock_obj = MagicMock()
    mock_client.client.stock.return_value = mock_stock_obj

    mock_df = pd.DataFrame({"title": ["Buy TCB"], "recommendation": ["BUY"]})
    mock_client.call_api_with_retry.return_value = mock_df

    pipeline = AnalystReportsPipeline(batch_date="2026-06-18", symbols=["TCB"])
    result_df = pipeline.fetch()

    mock_client.call_api_with_retry.assert_called_once_with(mock_stock_obj.company.news)
    assert result_df["ticker"].iloc[0] == "TCB"


@patch("src.ingest.pipeline.analyst_reports.VnStockClient")
def test_analyst_reports_pipeline_returns_empty_when_no_symbols(
    mock_client_class: MagicMock,
) -> None:
    """Verify fetch returns empty DataFrame when symbols list is empty."""
    mock_client = MagicMock()
    mock_client_class.return_value = mock_client

    pipeline = AnalystReportsPipeline(batch_date="2026-06-18", symbols=[])
    result_df = pipeline.fetch()

    mock_client.call_api_with_retry.assert_not_called()
    assert result_df.empty
