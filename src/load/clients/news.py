"""
Ingestion client for corporate news and events (mock).
"""

import logging
from datetime import datetime

import pandas as pd

logger = logging.getLogger(__name__)


def fetch_news_articles(topics: list[str]) -> pd.DataFrame:
    """Thu thập bài báo tin tức doanh nghiệp giả lập."""
    logger.info(f"Fetching news articles for topics: {topics}")
    data = []
    for topic in topics:
        data.append(
            {
                "article_id": f"art_{topic}_001",
                "ticker": topic,
                "publish_time": datetime.now().isoformat(),
                "title": f"Tin tuc moi nhat ve {topic}",
                "summary": f"Tom tat tin tuc ve {topic}",
                "content": f"Noi dung chi tiet tin tuc ve {topic}",
                "source": "CafeF",
                "url": f"https://cafef.vn/news/{topic}",
            }
        )
    return pd.DataFrame(data)


def fetch_corporate_events(topics: list[str]) -> pd.DataFrame:
    """Thu thập sự kiện doanh nghiệp giả lập."""
    logger.info(f"Fetching corporate events for topics: {topics}")
    data = []
    for topic in topics:
        data.append(
            {
                "event_id": f"evt_{topic}_001",
                "ticker": topic,
                "event_type": "Tra co tuc bang tien",
                "ex_right_date": datetime.today().strftime("%Y-%m-%d"),
                "record_date": datetime.today().strftime("%Y-%m-%d"),
                "event_details": f"Chi tiet su kien cho {topic}",
            }
        )
    return pd.DataFrame(data)


def fetch_insider_transactions(topics: list[str]) -> pd.DataFrame:
    """Thu thập giao dịch nội bộ giả lập."""
    logger.info(f"Fetching insider transactions for topics: {topics}")
    data = []
    for topic in topics:
        data.append(
            {
                "transaction_id": f"tx_{topic}_001",
                "ticker": topic,
                "insider_name": "Nguyen Van A",
                "position": "Giam doc",
                "action": "Mua",
                "registered_vol": "100000",
                "executed_vol": "100000",
                "date_start": datetime.today().strftime("%Y-%m-%d"),
                "date_end": datetime.today().strftime("%Y-%m-%d"),
            }
        )
    return pd.DataFrame(data)
