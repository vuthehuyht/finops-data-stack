"""CLI entrypoint for the historical backfill scripts.

Usage:
    uv run python -m scripts.historical_backfill.runner \
        --start-date 2021-07-18 --end-date 2026-07-18 \
        --tables stock_price_eod,balance_sheet --output-dir data/raw
"""

import argparse
import sys
from pathlib import Path

from scripts.historical_backfill.config import (
    DEFAULT_TICKER_SYMBOLS,
    default_date_range,
)
from scripts.historical_backfill.gap_log import GapLogger
from scripts.historical_backfill.tables import (
    analyst_reports,
    balance_sheet,
    cashflow_statement,
    commodities_price,
    company_profile,
    corporate_events,
    exchange_rates,
    financial_ratios,
    foreign_trading,
    income_statement,
    index_price_eod,
    insider_transactions,
    interest_rates,
    macro_indicators,
    news_articles,
    proprietary_trading,
    stock_price_eod,
)

TABLE_REGISTRY = {
    "stock_price_eod": stock_price_eod.run,
    "index_price_eod": index_price_eod.run,
    "foreign_trading": foreign_trading.run,
    "proprietary_trading": proprietary_trading.run,
    "balance_sheet": balance_sheet.run,
    "income_statement": income_statement.run,
    "cashflow_statement": cashflow_statement.run,
    "financial_ratios": financial_ratios.run,
    "company_profile": company_profile.run,
    "macro_indicators": macro_indicators.run,
    "interest_rates": interest_rates.run,
    "exchange_rates": exchange_rates.run,
    "commodities_price": commodities_price.run,
    "news_articles": news_articles.run,
    "corporate_events": corporate_events.run,
    "insider_transactions": insider_transactions.run,
    "analyst_reports": analyst_reports.run,
}


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    """Parse CLI arguments for the backfill runner."""
    default_start, default_end = default_date_range()
    parser = argparse.ArgumentParser(description="Historical data backfill for VN30.")
    parser.add_argument("--start-date", default=default_start)
    parser.add_argument("--end-date", default=default_end)
    parser.add_argument(
        "--tables",
        default=",".join(TABLE_REGISTRY.keys()),
        help="Comma-separated table names to backfill.",
    )
    parser.add_argument("--output-dir", default="data/raw")
    return parser.parse_args(argv)


def main(argv: list[str] | None = None) -> None:
    """Run the backfill for the requested tables and flush the gap report."""
    args = parse_args(argv)
    output_dir = Path(args.output_dir)
    requested_tables = [t.strip() for t in args.tables.split(",") if t.strip()]

    unknown = [t for t in requested_tables if t not in TABLE_REGISTRY]
    if unknown:
        raise ValueError(f"Unknown table(s): {unknown}. Valid: {list(TABLE_REGISTRY)}")

    gap_logger = GapLogger()
    for table_name in requested_tables:
        print(f"Backfilling {table_name}...")
        TABLE_REGISTRY[table_name](
            DEFAULT_TICKER_SYMBOLS,
            args.start_date,
            args.end_date,
            output_dir,
            gap_logger,
        )

    gap_report_path = gap_logger.flush(output_dir)
    print(f"Gap report written to {gap_report_path}")


if __name__ == "__main__":
    if sys.platform == "win32":
        # Windows consoles default to a codepage (e.g. cp1252) that cannot
        # render the Unicode banners vnstock prints on Vnstock()/finance.* calls.
        sys.stdout.reconfigure(encoding="utf-8")
        sys.stderr.reconfigure(encoding="utf-8")
    main()
