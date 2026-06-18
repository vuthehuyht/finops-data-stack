"""Ingestion pipeline for RAW_NEWS_ARTICLES."""

import pandas as pd

from src.ingest.client.vnstock_client import VnStockClient
from src.ingest.pipeline.base import DEFAULT_TICKER_SYMBOLS, BaseIngestPipeline


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
                stock_obj = client.client.stock(symbol=symbol, source="TCBS")
                if not hasattr(stock_obj, "company"):
                    continue

                df = client.call_api_with_retry(stock_obj.company.news)
                if not df.empty:
                    if "TICKER" not in df.columns and "symbol" not in df.columns:
                        df["ticker"] = symbol
                    all_dfs.append(df)
            except Exception as e:
                self.logger.error(
                    "Failed to fetch news for %s: %s", symbol, e
                )
                raise e

        if not all_dfs:
            return pd.DataFrame()

        return pd.concat(all_dfs, ignore_index=True)
