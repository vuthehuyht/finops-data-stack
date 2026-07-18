"""Backfill RAW_PROPRIETARY_TRADING — mock-generated prop trading per trading day.

VNDIRECT and SSI only expose *current-day* proprietary trading snapshots, so
there is no real historical source to backfill from. Every trading day uses
the same correlated-with-EOD-price mock generator the daily pipeline falls
back to, seeded from real close price/volume for that day.
"""

import random
from pathlib import Path

import pandas as pd

from scripts.historical_backfill import writer
from scripts.historical_backfill.gap_log import GapLogger
from src.ingest.client.vnstock_client import VnStockClient

TABLE_NAME = "proprietary_trading"


def _generate_mock_row(
    symbol: str, trading_date: str, close: float, volume: float
) -> dict:
    """Generate one mock proprietary-trading row correlated with real EOD stats."""
    ratio_buy = random.uniform(0.01, 0.05)
    ratio_sell = random.uniform(0.01, 0.05)
    buy_vol = int(volume * ratio_buy)
    sell_vol = int(volume * ratio_sell)
    net_val = int((buy_vol - sell_vol) * close * 1000)
    return {
        "ticker": symbol,
        "trading_date": trading_date,
        "buy_vol": str(buy_vol),
        "sell_vol": str(sell_vol),
        "net_val": str(net_val),
    }


def run(
    symbols: list[str],
    start_date: str,
    end_date: str,
    output_dir: Path,
    gap_logger: GapLogger,
) -> None:
    """Generate mock proprietary trading rows for every real trading day per symbol."""
    client = VnStockClient()

    for symbol in symbols:
        if writer.is_done(output_dir, TABLE_NAME, symbol):
            continue

        price_df = client.get_stock_price_eod(
            symbol=symbol, start_date=start_date, end_date=end_date
        )
        if price_df.empty:
            gap_logger.log(
                TABLE_NAME,
                symbol,
                f"{start_date}..{end_date}",
                "no EOD price history available to seed mock generation",
            )
            writer.mark_done(output_dir, TABLE_NAME, symbol)
            continue

        price_df = price_df.rename(columns={"time": "trading_date"})
        price_df["trading_date"] = price_df["trading_date"].astype(str).str.slice(0, 10)

        for _, row in price_df.iterrows():
            trading_date = row["trading_date"]
            mock_row = _generate_mock_row(
                symbol, trading_date, float(row["close"]), float(row["volume"])
            )
            writer.append_csv(
                output_dir, TABLE_NAME, trading_date, pd.DataFrame([mock_row])
            )

        writer.mark_done(output_dir, TABLE_NAME, symbol)
