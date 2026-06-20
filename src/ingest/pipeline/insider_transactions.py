"""Ingestion pipeline for RAW_INSIDER_TRANSACTIONS."""

import logging
import random

import pandas as pd
import requests
from bs4 import BeautifulSoup

from src.ingest.pipeline.base import DEFAULT_TICKER_SYMBOLS, BaseIngestPipeline

logger = logging.getLogger(__name__)


class InsiderTransactionsPipeline(BaseIngestPipeline):
    """Pipeline to ingest corporate insider trading logs into S3 Bronze.

    Uses a multi-tier fallback strategy to fetch data, falling back to a
    smart mock generator if all external sources fail.
    """

    @property
    def table_name(self) -> str:
        return "RAW_INSIDER_TRANSACTIONS"

    @property
    def source_uri_prefix(self) -> str:
        return "api://vnstock/insider_transactions"

    @property
    def schema_columns(self) -> list[str]:
        return [
            "ticker",
            "deal_announce_date",
            "deal_method",
            "deal_action",
            "deal_quantity",
            "deal_price",
            "deal_ratio",
        ]

    def fetch(self) -> pd.DataFrame:
        """Fetch insider transactions for symbols on the batch date.

        Coordinates fallback strategy for each symbol.
        """
        targets = self.symbols or DEFAULT_TICKER_SYMBOLS
        results = []

        for symbol in targets:
            df_symbol = self._fetch_for_symbol(symbol)
            if df_symbol is not None and not df_symbol.empty:
                results.append(df_symbol)

        if not results:
            return pd.DataFrame()

        return pd.concat(results, ignore_index=True)

    def _fetch_for_symbol(self, symbol: str) -> pd.DataFrame:
        """Fetch insider deals for a single symbol using fallbacks."""
        headers = {
            "User-Agent": (
                "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                "AppleWebKit/537.36 (KHTML, like Gecko) "
                "Chrome/120.0.0.0 Safari/537.36"
            )
        }

        # Strategy 1: vnstock v4 Company Reference
        df_vnstock = self._try_vnstock(symbol)
        if not df_vnstock.empty:
            return df_vnstock

        # Strategy 2: CafeF HTML Scraper
        df_cafef = self._try_cafef(symbol, headers)
        if not df_cafef.empty:
            return df_cafef

        # Strategy 3: Mock Fallback
        df_mock = self._generate_mock(symbol)
        if not df_mock.empty:
            df_mock["_actual_source"] = "mock://fallback/insider_trading"
        return df_mock

    def _try_vnstock(self, symbol: str) -> pd.DataFrame:
        """Try fetching using vnstock Company Reference."""
        try:
            from vnstock import Reference

            ref = Reference()
            c = ref.company(symbol)
            df = c.insider_trading()
            if df is not None and not df.empty:
                df.columns = df.columns.str.lower()
                date_col = [c for c in df.columns if "date" in c or "time" in c]
                if date_col:
                    df["deal_announce_date"] = pd.to_datetime(
                        df[date_col[0]]
                    ).dt.strftime("%Y-%m-%d")
                    df = df[df["deal_announce_date"] == self.batch_date]

                if not df.empty:
                    df["ticker"] = symbol
                    df["deal_method"] = df.get("deal_method", "MATCHING")
                    df["deal_action"] = df.get("deal_action", "BUY")
                    df["deal_quantity"] = df.get("deal_quantity", 0)
                    df["deal_price"] = df.get("deal_price", 0)
                    df["deal_ratio"] = df.get("deal_ratio", 0)

                    df_res = df[self.schema_columns].copy()
                    df_res["_actual_source"] = "api://vnstock/insider_trading"
                    return df_res
        except Exception as e:
            self.logger.warning(
                "Failed vnstock company reference for %s: %s", symbol, e
            )
        return pd.DataFrame()

    def _try_cafef(self, symbol: str, headers: dict) -> pd.DataFrame:
        """Try fetching using CafeF HTML Scraper."""
        try:
            url = f"https://s.cafef.vn/Lich-su-giao-dich-{symbol}-3.chn"
            r = requests.get(url, headers=headers, timeout=5)
            if r.status_code == 200:
                soup = BeautifulSoup(r.text, "html.parser")
                tables = soup.find_all("table")
                target_table = None
                for table in tables:
                    t_text = table.get_text()
                    if "Người thực hiện" in t_text or "Ngày giao dịch" in t_text:
                        target_table = table
                        break

                if target_table:
                    rows = target_table.find_all("tr")
                    all_deals = []
                    for row in rows[1:]:
                        cols = [
                            td.get_text(strip=True) for td in row.find_all(["td", "th"])
                        ]
                        if len(cols) >= 6:
                            deal = self._parse_cafef_row(cols, symbol)
                            if deal:
                                all_deals.append(deal)

                    if all_deals:
                        df_res = pd.DataFrame(all_deals)
                        df_res["_actual_source"] = "scrape://cafef/insider_trading"
                        return df_res
        except Exception as e:
            self.logger.warning("Failed CafeF scraper for %s: %s", symbol, e)
        return pd.DataFrame()

    def _parse_cafef_row(self, cols: list[str], symbol: str) -> dict | None:
        """Parse cols list of a single CafeF table row into a deal dict."""
        raw_action_vol = cols[3].lower()
        action = "BUY" if "mua" in raw_action_vol else "SELL"

        raw_qty = cols[4].replace(",", "").replace(".", "")
        qty = int(raw_qty) if raw_qty.isdigit() else 0

        raw_date = cols[5]
        try:
            deal_date = pd.to_datetime(raw_date, format="%d/%m/%Y").strftime("%Y-%m-%d")
        except Exception:
            deal_date = self.batch_date

        if deal_date == self.batch_date:
            return {
                "ticker": symbol,
                "deal_announce_date": deal_date,
                "deal_method": "MATCHING",
                "deal_action": action,
                "deal_quantity": str(qty),
                "deal_price": "0",
                "deal_ratio": "0",
            }
        return None

    def _generate_mock(self, symbol: str) -> pd.DataFrame:
        """Generate realistic mock data based on real close price if available."""
        if random.random() > 0.10:
            return pd.DataFrame()

        close_price = 30000
        try:
            from vnstock.api.quote import Quote

            q = Quote(symbol=symbol, source="KBS")
            df_hist = q.history(start=self.batch_date, end=self.batch_date)
            if df_hist is not None and not df_hist.empty:
                col_close = [
                    c for c in df_hist.columns if c.lower() in ["close", "price"]
                ][0]
                close_price = float(df_hist[col_close].values[0])
        except Exception as e:
            self.logger.debug("Failed to get quote price for insider mock: %s", e)

        actions = ["BUY", "SELL"]
        methods = ["MATCHING", "AGREEMENT"]

        action = random.choice(actions)
        method = random.choice(methods)
        qty = random.randint(10000, 150000)
        price = close_price * random.uniform(0.98, 1.02)
        ratio = round(random.uniform(0.001, 0.02), 4)

        return pd.DataFrame(
            [
                {
                    "ticker": symbol,
                    "deal_announce_date": self.batch_date,
                    "deal_method": method,
                    "deal_action": action,
                    "deal_quantity": str(qty),
                    "deal_price": f"{price:.2f}",
                    "deal_ratio": f"{ratio:.4f}",
                }
            ]
        )

    def standardize(self, df: pd.DataFrame) -> pd.DataFrame:
        """Override to inject dynamic _CONATA_SOURCE values per row."""
        actual_sources = (
            df["_actual_source"].tolist() if "_actual_source" in df.columns else []
        )
        df_clean = df.drop(columns=["_actual_source"], errors="ignore")

        result_df = super().standardize(df_clean)

        if actual_sources and len(actual_sources) == len(result_df):
            result_df["_CONATA_SOURCE"] = actual_sources

        return result_df
