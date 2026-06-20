"""Ingestion pipeline for RAW_PROPRIETARY_TRADING."""

import logging
import random

import pandas as pd
import requests

from src.ingest.pipeline.base import DEFAULT_TICKER_SYMBOLS, BaseIngestPipeline

logger = logging.getLogger(__name__)


class ProprietaryTradingPipeline(BaseIngestPipeline):
    """Pipeline to ingest proprietary trading data into S3 Bronze.

    Uses a multi-tier fallback strategy to fetch data, falling back to a
    smart mock generator if all external sources fail.
    """

    @property
    def table_name(self) -> str:
        return "RAW_PROPRIETARY_TRADING"

    @property
    def source_uri_prefix(self) -> str:
        return "api://vnstock/proprietary_trading"

    @property
    def schema_columns(self) -> list[str]:
        return [
            "ticker",
            "trading_date",
            "buy_vol",
            "sell_vol",
            "net_val",
        ]

    def fetch(self) -> pd.DataFrame:
        """Fetch proprietary trading logs for symbols on the batch date.

        Coordinates fallback strategy for each symbol.
        """
        targets = self.symbols or DEFAULT_TICKER_SYMBOLS
        results = []

        for symbol in targets:
            df_symbol = self._fetch_for_symbol(symbol)
            if not df_symbol.empty:
                results.append(df_symbol)

        if not results:
            return pd.DataFrame()

        return pd.concat(results, ignore_index=True)

    def _fetch_for_symbol(self, symbol: str) -> pd.DataFrame:
        """Fetch trading data for a single symbol using fallbacks."""
        headers = {
            "User-Agent": (
                "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                "AppleWebKit/537.36 (KHTML, like Gecko) "
                "Chrome/120.0.0.0 Safari/537.36"
            )
        }

        # Strategy 1: VNDIRECT API
        df_vnd = self._try_vndirect(symbol, headers)
        if not df_vnd.empty:
            return df_vnd

        # Strategy 2: SSI iBoard API
        df_ssi = self._try_ssi(symbol, headers)
        if not df_ssi.empty:
            return df_ssi

        # Strategy 3: Mock Fallback
        df_mock = self._generate_mock(symbol)
        df_mock["_actual_source"] = "mock://fallback/proprietary_trading"
        return df_mock

    def _try_vndirect(self, symbol: str, headers: dict) -> pd.DataFrame:
        """Try fetching from VNDIRECT API."""
        try:
            url = (
                "https://finfo-api.vndirect.com.vn/v4/"
                f"proprietary_trading?code={symbol}"
            )
            r = requests.get(url, headers=headers, timeout=5)
            if r.status_code == 200:
                data = r.json().get("data", [])
                if data:
                    df = pd.DataFrame(data)
                    df.columns = df.columns.str.lower()
                    date_col = [c for c in df.columns if "date" in c][0]
                    df["trading_date"] = pd.to_datetime(df[date_col]).dt.strftime(
                        "%Y-%m-%d"
                    )
                    df = df[df["trading_date"] == self.batch_date]

                    if not df.empty:
                        df["ticker"] = symbol
                        df["buy_vol"] = df.get("totalvolbuy", 0)
                        df["sell_vol"] = df.get("totalvolsell", 0)
                        df["net_val"] = df.get("netval", 0)

                        df_res = df[self.schema_columns].copy()
                        df_res["_actual_source"] = "api://vndirect/proprietary_trading"
                        return df_res
        except Exception as e:
            self.logger.warning("Failed VNDIRECT API for %s: %s", symbol, e)
        return pd.DataFrame()

    def _try_ssi(self, symbol: str, headers: dict) -> pd.DataFrame:
        """Try fetching from SSI iBoard API."""
        try:
            url = "https://iboard.ssi.com.vn/api/v1/iboard/market/proprietaryToday"
            r = requests.get(url, headers=headers, timeout=5)
            if r.status_code == 200:
                data = r.json()
                if isinstance(data, list) and data:
                    df = pd.DataFrame(data)
                    df.columns = df.columns.str.lower()
                    df = df[df["ticker"].str.upper() == symbol.upper()]
                    if not df.empty:
                        df["trading_date"] = self.batch_date
                        df["buy_vol"] = df.get("buyvolume", 0)
                        df["sell_vol"] = df.get("sellvolume", 0)
                        df["net_val"] = df.get("netvalue", 0)

                        df_res = df[self.schema_columns].copy()
                        df_res["_actual_source"] = "api://ssi/proprietary_trading"
                        return df_res
        except Exception as e:
            self.logger.warning("Failed SSI API for %s: %s", symbol, e)
        return pd.DataFrame()

    def _generate_mock(self, symbol: str) -> pd.DataFrame:
        """Generate realistic mock data based on real EOD stock stats if possible."""
        buy_vol = 0
        sell_vol = 0
        net_val = 0

        try:
            from vnstock.api.quote import Quote

            q = Quote(symbol=symbol, source="KBS")
            df_hist = q.history(start=self.batch_date, end=self.batch_date)

            if df_hist is not None and not df_hist.empty:
                col_vol = [
                    c for c in df_hist.columns if c.lower() in ["volume", "vol"]
                ][0]
                col_close = [
                    c for c in df_hist.columns if c.lower() in ["close", "price"]
                ][0]

                vol = float(df_hist[col_vol].values[0])
                close = float(df_hist[col_close].values[0])

                ratio_buy = random.uniform(0.01, 0.05)
                ratio_sell = random.uniform(0.01, 0.05)

                buy_vol = int(vol * ratio_buy)
                sell_vol = int(vol * ratio_sell)
                net_val = int((buy_vol - sell_vol) * close * 1000)
            else:
                raise ValueError("Empty quote history returned from KBS")
        except Exception as e:
            self.logger.warning(
                "Mock fallback for %s on %s: %s. Using random generation.",
                symbol,
                self.batch_date,
                e,
            )
            buy_vol = random.randint(30000, 250000)
            sell_vol = random.randint(30000, 250000)
            close = random.randint(15000, 90000)
            net_val = int((buy_vol - sell_vol) * close)

        return pd.DataFrame(
            [
                {
                    "ticker": symbol,
                    "trading_date": self.batch_date,
                    "buy_vol": str(buy_vol),
                    "sell_vol": str(sell_vol),
                    "net_val": str(net_val),
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
