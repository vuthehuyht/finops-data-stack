"""Backfill RAW_NEWS_ARTICLES — best-effort historical news per symbol."""

from pathlib import Path

from scripts.historical_backfill import writer
from scripts.historical_backfill.config import VNSTOCK_REQUEST_DELAY_SECONDS
from scripts.historical_backfill.gap_log import GapLogger
from src.ingest.client.vnstock_client import VnStockClient

TABLE_NAME = "news_articles"

_COLUMN_MAP = {
    "id": "article_id",
    "news_title": "title",
    "news_sub_title": "summary",
    "news_full_content": "content",
    "news_source": "source",
    "news_source_link": "url",
    "public_date": "publish_time",
}


def run(
    symbols: list[str],
    start_date: str,
    end_date: str,
    output_dir: Path,
    gap_logger: GapLogger,
) -> None:
    """Fetch whatever news history the source returns per symbol, filtered to range."""
    client = VnStockClient(request_delay_seconds=VNSTOCK_REQUEST_DELAY_SECONDS)

    for symbol in symbols:
        if writer.is_done(output_dir, TABLE_NAME, symbol):
            continue

        df = client.get_company_news(symbol)
        if df.empty:
            gap_logger.log(
                TABLE_NAME, symbol, f"{start_date}..{end_date}", "empty API response"
            )
            writer.mark_done(output_dir, TABLE_NAME, symbol)
            continue

        df = df.rename(columns=_COLUMN_MAP)
        if "ticker" not in df.columns or df["ticker"].isna().all():
            df["ticker"] = symbol
        if "publish_time" not in df.columns:
            gap_logger.log(
                TABLE_NAME,
                symbol,
                f"{start_date}..{end_date}",
                "no publish_time column",
            )
            writer.mark_done(output_dir, TABLE_NAME, symbol)
            continue

        df["publish_date"] = df["publish_time"].astype(str).str.slice(0, 10)
        df = df[(df["publish_date"] >= start_date) & (df["publish_date"] <= end_date)]
        if df.empty:
            gap_logger.log(
                TABLE_NAME,
                symbol,
                f"{start_date}..{end_date}",
                "source only returned articles outside requested range",
            )
            writer.mark_done(output_dir, TABLE_NAME, symbol)
            continue

        for publish_date, group in df.groupby("publish_date"):
            group = group.drop(columns=["publish_date"])
            writer.append_csv(output_dir, TABLE_NAME, publish_date, group)

        writer.mark_done(output_dir, TABLE_NAME, symbol)
