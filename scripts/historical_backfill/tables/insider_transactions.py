"""Backfill RAW_INSIDER_TRANSACTIONS — best-effort historical insider deals per symbol.

Does not use the random mock generator that the daily pipeline falls back to —
fabricating years of fake trades would pollute historical training data. A gap
is logged instead when no real source returns data for a symbol.
"""

from pathlib import Path

import pandas as pd
import requests
from bs4 import BeautifulSoup

from scripts.historical_backfill import writer
from scripts.historical_backfill.gap_log import GapLogger

TABLE_NAME = "insider_transactions"

_HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/120.0.0.0 Safari/537.36"
    )
}
_SCHEMA_COLUMNS = [
    "ticker",
    "deal_announce_date",
    "deal_method",
    "deal_action",
    "deal_quantity",
    "deal_price",
    "deal_ratio",
]


def _try_vnstock(symbol: str, start_date: str, end_date: str) -> pd.DataFrame:
    """Try fetching insider deal history via vnstock Company Reference."""
    try:
        from vnstock import Reference

        df = Reference().company(symbol).insider_trading()
        if df is None or df.empty:
            return pd.DataFrame()

        df.columns = df.columns.str.lower()
        date_col = next((c for c in df.columns if "date" in c or "time" in c), None)
        if date_col is None:
            return pd.DataFrame()

        df["deal_announce_date"] = pd.to_datetime(df[date_col]).dt.strftime("%Y-%m-%d")
        df = df[
            (df["deal_announce_date"] >= start_date)
            & (df["deal_announce_date"] <= end_date)
        ]
        if df.empty:
            return pd.DataFrame()

        df["ticker"] = symbol
        df["deal_method"] = df.get("deal_method", "MATCHING")
        df["deal_action"] = df.get("deal_action", "BUY")
        df["deal_quantity"] = df.get("deal_quantity", 0)
        df["deal_price"] = df.get("deal_price", 0)
        df["deal_ratio"] = df.get("deal_ratio", 0)
        return df[_SCHEMA_COLUMNS]
    except Exception:
        return pd.DataFrame()


def _try_cafef(symbol: str, start_date: str, end_date: str) -> pd.DataFrame:
    """Try fetching insider deal history by scraping the CafeF history table."""
    try:
        url = f"https://s.cafef.vn/Lich-su-giao-dich-{symbol}-3.chn"
        response = requests.get(url, headers=_HEADERS, timeout=5)
        if response.status_code != 200:
            return pd.DataFrame()

        soup = BeautifulSoup(response.text, "html.parser")
        target_table = None
        for table in soup.find_all("table"):
            text = table.get_text()
            if "Người thực hiện" in text or "Ngày giao dịch" in text:
                target_table = table
                break
        if target_table is None:
            return pd.DataFrame()

        deals = []
        for row in target_table.find_all("tr")[1:]:
            cols = [td.get_text(strip=True) for td in row.find_all(["td", "th"])]
            if len(cols) < 6:
                continue
            action = "BUY" if "mua" in cols[3].lower() else "SELL"
            raw_qty = cols[4].replace(",", "").replace(".", "")
            qty = int(raw_qty) if raw_qty.isdigit() else 0
            try:
                deal_date = pd.to_datetime(cols[5], format="%d/%m/%Y").strftime(
                    "%Y-%m-%d"
                )
            except Exception:
                continue
            if not (start_date <= deal_date <= end_date):
                continue
            deals.append(
                {
                    "ticker": symbol,
                    "deal_announce_date": deal_date,
                    "deal_method": "MATCHING",
                    "deal_action": action,
                    "deal_quantity": str(qty),
                    "deal_price": "0",
                    "deal_ratio": "0",
                }
            )
        return pd.DataFrame(deals)
    except Exception:
        return pd.DataFrame()


def run(
    symbols: list[str],
    start_date: str,
    end_date: str,
    output_dir: Path,
    gap_logger: GapLogger,
) -> None:
    """Fetch real insider-deal history for each symbol; log a gap if none is found."""
    for symbol in symbols:
        if writer.is_done(output_dir, TABLE_NAME, symbol):
            continue

        df = _try_vnstock(symbol, start_date, end_date)
        if df.empty:
            df = _try_cafef(symbol, start_date, end_date)

        if df.empty:
            gap_logger.log(
                TABLE_NAME,
                symbol,
                f"{start_date}..{end_date}",
                "no real source returned insider deal history",
            )
            writer.mark_done(output_dir, TABLE_NAME, symbol)
            continue

        for deal_date, group in df.groupby("deal_announce_date"):
            writer.append_csv(output_dir, TABLE_NAME, deal_date, group)

        writer.mark_done(output_dir, TABLE_NAME, symbol)
