"""
Ingestion client for Vietnamese Stock Market data (mock).
"""

import logging
from datetime import datetime

import pandas as pd

logger = logging.getLogger(__name__)


def fetch_stock_price_eod(symbols: list[str], days_back: int) -> pd.DataFrame:
    """Thu thập giá cổ phiếu EOD giả lập."""
    logger.info(f"Fetching stock price EOD for {symbols} (days_back={days_back})")
    data = []
    for symbol in symbols:
        data.append(
            {
                "ticker": symbol,
                "trading_date": datetime.today().strftime("%Y-%m-%d"),
                "open": "100.0",
                "high": "105.0",
                "low": "98.0",
                "close": "102.5",
                "volume": "1500000",
                "value": "153750000",
                "adjusted_close": "102.5",
            }
        )
    return pd.DataFrame(data)


def fetch_index_price_eod(indices: list[str], days_back: int) -> pd.DataFrame:
    """Thu thập giá chỉ số EOD giả lập."""
    logger.info(f"Fetching index price EOD for {indices} (days_back={days_back})")
    data = []
    for index in indices:
        data.append(
            {
                "index_name": index,
                "trading_date": datetime.today().strftime("%Y-%m-%d"),
                "open": "1200.0",
                "high": "1210.0",
                "low": "1195.0",
                "close": "1205.5",
                "volume": "800000000",
            }
        )
    return pd.DataFrame(data)


def fetch_foreign_trading(symbols: list[str], days_back: int) -> pd.DataFrame:
    """Thu thập dữ liệu giao dịch khối ngoại giả lập."""
    logger.info(f"Fetching foreign trading for {symbols} (days_back={days_back})")
    data = []
    for symbol in symbols:
        data.append(
            {
                "ticker": symbol,
                "trading_date": datetime.today().strftime("%Y-%m-%d"),
                "buy_vol": "50000",
                "sell_vol": "30000",
                "buy_val": "5125000",
                "sell_val": "3075000",
                "net_val": "2050000",
            }
        )
    return pd.DataFrame(data)


def fetch_proprietary_trading(symbols: list[str], days_back: int) -> pd.DataFrame:
    """Thu thập dữ liệu giao dịch tự doanh giả lập."""
    logger.info(f"Fetching proprietary trading for {symbols} (days_back={days_back})")
    data = []
    for symbol in symbols:
        data.append(
            {
                "ticker": symbol,
                "trading_date": datetime.today().strftime("%Y-%m-%d"),
                "buy_vol": "10000",
                "sell_vol": "12000",
                "net_val": "-205000",
            }
        )
    return pd.DataFrame(data)


def fetch_balance_sheet(symbols: list[str], period: str) -> pd.DataFrame:
    """Thu thập bảng cân đối kế toán giả lập."""
    logger.info(f"Fetching balance sheet for {symbols} (period={period})")
    data = []
    for symbol in symbols:
        data.append(
            {
                "ticker": symbol,
                "period": period,
                "year": str(datetime.today().year),
                "total_assets": "1000000000",
                "current_assets": "600000000",
                "cash": "150000000",
                "inventory": "200000000",
                "total_liabilities": "400000000",
                "short_term_debt": "100000000",
                "long_term_debt": "150000000",
                "equity": "600000000",
            }
        )
    return pd.DataFrame(data)


def fetch_income_statement(symbols: list[str], period: str) -> pd.DataFrame:
    """Thu thập báo cáo kết quả hoạt động kinh doanh giả lập."""
    logger.info(f"Fetching income statement for {symbols} (period={period})")
    data = []
    for symbol in symbols:
        data.append(
            {
                "ticker": symbol,
                "period": period,
                "year": str(datetime.today().year),
                "revenue": "500000000",
                "cogs": "300000000",
                "gross_profit": "200000000",
                "operating_expenses": "80000000",
                "operating_profit": "120000000",
                "financial_income": "10000000",
                "financial_expenses": "15000000",
                "net_profit_after_tax": "95000000",
            }
        )
    return pd.DataFrame(data)


def fetch_cashflow_statement(symbols: list[str], period: str) -> pd.DataFrame:
    """Thu thập báo cáo lưu chuyển tiền tệ giả lập."""
    logger.info(f"Fetching cashflow statement for {symbols} (period={period})")
    data = []
    for symbol in symbols:
        data.append(
            {
                "ticker": symbol,
                "period": period,
                "year": str(datetime.today().year),
                "cfo": "110000000",
                "cfi": "-50000000",
                "cff": "-40000000",
                "net_cash_flow": "20000000",
                "capex": "-45000000",
            }
        )
    return pd.DataFrame(data)


def fetch_financial_ratios(symbols: list[str]) -> pd.DataFrame:
    """Thu thập chỉ số tài chính giả lập."""
    logger.info(f"Fetching financial ratios for {symbols}")
    data = []
    for symbol in symbols:
        data.append(
            {
                "ticker": symbol,
                "date": datetime.today().strftime("%Y-%m-%d"),
                "shares_outstanding": "10000000",
                "market_cap": "1025000000",
            }
        )
    return pd.DataFrame(data)


def fetch_company_profile(symbols: list[str]) -> pd.DataFrame:
    """Thu thập thông tin hồ sơ doanh nghiệp giả lập."""
    logger.info(f"Fetching company profile for {symbols}")
    data = []
    for symbol in symbols:
        data.append(
            {
                "ticker": symbol,
                "company_name": f"Mock Company {symbol}",
                "industry": "Technology",
                "exchange": "HOSE",
                "description": f"This is a mock company description for {symbol}.",
            }
        )
    return pd.DataFrame(data)
