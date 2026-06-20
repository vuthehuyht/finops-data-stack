"""Unit tests for NewsArticlesPipeline."""

from unittest.mock import MagicMock, patch

import pandas as pd
import pytest

from src.ingest.pipeline.news_articles import NewsArticlesPipeline


def _make_news_row(**overrides) -> dict:
    base = {
        "id": "6a2368e162a0328b2d1e6f53",
        "ticker": None,
        "news_title": "TCB: Quyet dinh cua HDQT",
        "news_sub_title": "Subtitle here",
        "news_full_content": "Full content here",
        "news_source": "Cafef",
        "news_source_link": "https://cafef.vn/article",
        "public_date": "2026-06-18T10:00:00",
    }
    base.update(overrides)
    return base


@patch("src.ingest.pipeline.news_articles.VnStockClient")
def test_news_articles_pipeline_fetch_success(
    mock_client_class: MagicMock,
) -> None:
    """Verify fetch queries all symbols and returns combined DataFrame."""
    mock_client = MagicMock()
    mock_client_class.return_value = mock_client
    mock_client.get_company_news.return_value = pd.DataFrame([_make_news_row()])

    pipeline = NewsArticlesPipeline(batch_date="2026-06-18", symbols=["TCB", "VCB"])
    result_df = pipeline.fetch()

    assert mock_client.get_company_news.call_count == 2
    assert len(result_df) == 2


@patch("src.ingest.pipeline.news_articles.VnStockClient")
def test_news_articles_pipeline_fetch_maps_columns(
    mock_client_class: MagicMock,
) -> None:
    """Verify fetch maps vnstock column names to schema column names."""
    mock_client = MagicMock()
    mock_client_class.return_value = mock_client
    mock_client.get_company_news.return_value = pd.DataFrame([_make_news_row()])

    pipeline = NewsArticlesPipeline(batch_date="2026-06-18", symbols=["TCB"])
    result_df = pipeline.fetch()

    assert "article_id" in result_df.columns
    assert "title" in result_df.columns
    assert "summary" in result_df.columns
    assert "content" in result_df.columns
    assert "source" in result_df.columns
    assert "url" in result_df.columns
    assert "publish_time" in result_df.columns
    assert result_df["article_id"].iloc[0] == "6a2368e162a0328b2d1e6f53"
    assert result_df["title"].iloc[0] == "TCB: Quyet dinh cua HDQT"


@patch("src.ingest.pipeline.news_articles.VnStockClient")
def test_news_articles_pipeline_fetch_injects_ticker_when_null(
    mock_client_class: MagicMock,
) -> None:
    """Verify fetch injects the symbol as ticker when API returns None."""
    mock_client = MagicMock()
    mock_client_class.return_value = mock_client
    mock_client.get_company_news.return_value = pd.DataFrame(
        [_make_news_row(ticker=None)]
    )

    pipeline = NewsArticlesPipeline(batch_date="2026-06-18", symbols=["TCB"])
    result_df = pipeline.fetch()

    assert result_df["ticker"].iloc[0] == "TCB"


@patch("src.ingest.pipeline.news_articles.VnStockClient")
def test_news_articles_pipeline_fetch_empty(
    mock_client_class: MagicMock,
) -> None:
    """Verify fetch returns empty DataFrame when no articles found."""
    mock_client = MagicMock()
    mock_client_class.return_value = mock_client
    mock_client.get_company_news.return_value = pd.DataFrame()

    pipeline = NewsArticlesPipeline(batch_date="2026-06-18", symbols=["TCB"])
    result_df = pipeline.fetch()

    assert result_df.empty


@patch("src.ingest.pipeline.news_articles.VnStockClient")
def test_news_articles_pipeline_fetch_error_propagates(
    mock_client_class: MagicMock,
) -> None:
    """Verify fetch propagates exception from the client."""
    mock_client = MagicMock()
    mock_client_class.return_value = mock_client
    mock_client.get_company_news.side_effect = ConnectionError("404 Not Found")

    pipeline = NewsArticlesPipeline(batch_date="2026-06-18", symbols=["TCB"])
    with pytest.raises(ConnectionError, match="404 Not Found"):
        pipeline.fetch()


@patch("src.ingest.pipeline.news_articles.VnStockClient")
def test_news_articles_pipeline_fetch_uses_default_symbols(
    mock_client_class: MagicMock,
) -> None:
    """Verify fetch falls back to DEFAULT_TICKER_SYMBOLS when symbols not given."""
    mock_client = MagicMock()
    mock_client_class.return_value = mock_client
    mock_client.get_company_news.return_value = pd.DataFrame()

    pipeline = NewsArticlesPipeline(batch_date="2026-06-18")
    pipeline.fetch()

    assert mock_client.get_company_news.call_count == 30


def test_news_articles_pipeline_table_name() -> None:
    """Verify table_name matches the design spec."""
    pipeline = NewsArticlesPipeline(batch_date="2026-06-18")
    assert pipeline.table_name == "RAW_NEWS_ARTICLES"


def test_news_articles_pipeline_schema_columns() -> None:
    """Verify schema_columns matches the design spec."""
    pipeline = NewsArticlesPipeline(batch_date="2026-06-18")
    assert pipeline.schema_columns == [
        "article_id",
        "ticker",
        "publish_time",
        "title",
        "summary",
        "content",
        "source",
        "url",
    ]
