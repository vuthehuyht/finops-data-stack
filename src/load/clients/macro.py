"""
Ingestion client for macroeconomic metrics (mock).
"""

import logging
from datetime import datetime

import pandas as pd

logger = logging.getLogger(__name__)


def fetch_macro_indicators(indicators: list[str]) -> pd.DataFrame:
    """Thu thập chỉ số vĩ mô giả lập (ví dụ GDP, CPI)."""
    logger.info(f"Fetching macro indicators: {indicators}")
    data = []
    for indicator in indicators:
        data.append(
            {
                "indicator_name": indicator,
                "report_date": datetime.today().strftime("%Y-%m-%d"),
                "value": "4.25" if indicator == "CPI" else "6.5",
                "unit": "%",
            }
        )
    return pd.DataFrame(data)


def fetch_interest_rates(rate_types: list[str]) -> pd.DataFrame:
    """Thu thập lãi suất giả lập."""
    logger.info(f"Fetching interest rates: {rate_types}")
    data = []
    for rate_type in rate_types:
        data.append(
            {
                "rate_type": rate_type,
                "date": datetime.today().strftime("%Y-%m-%d"),
                "rate_value": "4.5",
            }
        )
    return pd.DataFrame(data)


def fetch_exchange_rates(pairs: list[str]) -> pd.DataFrame:
    """Thu thập tỷ giá hối đoái giả lập."""
    logger.info(f"Fetching exchange rates: {pairs}")
    data = []
    for pair in pairs:
        data.append(
            {
                "pair": pair,
                "date": datetime.today().strftime("%Y-%m-%d"),
                "exchange_rate": "25400.0" if "USD" in pair else "27000.0",
            }
        )
    return pd.DataFrame(data)


def fetch_commodities_price(commodities: list[str]) -> pd.DataFrame:
    """Thu thập giá hàng hóa giả lập."""
    logger.info(f"Fetching commodities price: {commodities}")
    data = []
    for commodity in commodities:
        data.append(
            {
                "commodity_name": commodity,
                "date": datetime.today().strftime("%Y-%m-%d"),
                "price": "85.2" if commodity == "BRENT" else "2400.0",
            }
        )
    return pd.DataFrame(data)
