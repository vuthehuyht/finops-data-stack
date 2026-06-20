"""Ingestion pipeline for RAW_NEWS_ARTICLES."""

import pandas as pd

from src.ingest.client.vnstock_client import VnStockClient
from src.ingest.pipeline.base import DEFAULT_TICKER_SYMBOLS, BaseIngestPipeline

# vnstock v4 VCI column → schema column
_COLUMN_MAP = {
    "id": "article_id",
    "news_title": "title",
    "news_sub_title": "summary",
    "news_full_content": "content",
    "news_source": "source",
    "news_source_link": "url",
    "public_date": "publish_time",
}


class NewsArticlesPipeline(BaseIngestPipeline):
    """Pipeline to ingest corporate news articles into S3 Bronze."""

    @property
    def table_name(self) -> str:
        return "RAW_NEWS_ARTICLES"

    @property
    def source_uri_prefix(self) -> str:
        return "api://vnstock/news_articles"

    @property
    def schema_columns(self) -> list[str]:
        return [
            "article_id",
            "ticker",
            "publish_time",
            "title",
            "summary",
            "content",
            "source",
            "url",
        ]

    def fetch(self) -> pd.DataFrame:
        """Fetch news articles for symbols on the batch date."""
        client = VnStockClient()
        all_dfs = []
        targets = self.symbols or DEFAULT_TICKER_SYMBOLS

        for symbol in targets:
            try:
                df = client.get_company_news(symbol)
                if df.empty:
                    continue

                df = df.rename(columns=_COLUMN_MAP)

                # Inject ticker when VCI returns None
                if "ticker" not in df.columns or df["ticker"].isna().all():
                    df["ticker"] = symbol

                all_dfs.append(df)
            except Exception as e:
                self.logger.error("Failed to fetch news for %s: %s", symbol, e)
                raise e

        if not all_dfs:
            return pd.DataFrame()

        return pd.concat(all_dfs, ignore_index=True)
