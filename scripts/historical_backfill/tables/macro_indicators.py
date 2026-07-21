"""Backfill RAW_MACRO_INDICATORS — historical World Bank indicator values."""

from pathlib import Path

import pandas as pd

from scripts.historical_backfill import writer
from scripts.historical_backfill.gap_log import GapLogger
from src.ingest.client.world_bank_client import WorldBankClient

TABLE_NAME = "macro_indicators"

INDICATOR_MAPPING: dict[str, str] = {
    "vn_gdp_usd": "NY.GDP.MKTP.CD",
    "vn_cpi_inflation": "FP.CPI.TOTL.ZG",
    "vn_unemployment": "SL.UEM.TOTL.ZS",
}


def _parse_wb_date(date_str: str) -> str:
    """Convert a World Bank date string (annual/monthly/quarterly) to an ISO date."""
    if "M" in date_str:
        year, month = date_str.split("M")
        return f"{year}-{month.zfill(2)}-01"
    if "Q" in date_str:
        year, quarter = date_str.split("Q")
        month = (int(quarter) - 1) * 3 + 1
        return f"{year}-{str(month).zfill(2)}-01"
    return f"{date_str}-01-01"


def run(
    symbols: list[str],
    start_date: str,
    end_date: str,
    output_dir: Path,
    gap_logger: GapLogger,
) -> None:
    """Fetch historical values for each macro indicator within the date range."""
    client = WorldBankClient()
    start_year = int(start_date.split("-")[0])
    end_year = int(end_date.split("-")[0])
    mrv = (end_year - start_year + 1) + 5

    for indicator_name, series_id in INDICATOR_MAPPING.items():
        if writer.is_done(output_dir, TABLE_NAME, indicator_name):
            continue

        records = client.get_indicator(series_id, mrv=mrv)
        rows_written = 0
        for record in records:
            if record.get("value") is None:
                continue
            report_date = _parse_wb_date(record["date"])
            if not (start_date <= report_date <= end_date):
                continue

            row = pd.DataFrame(
                [
                    {
                        "indicator_name": indicator_name,
                        "report_date": report_date,
                        "value": str(record["value"]),
                        "unit": record.get("unit") or "",
                    }
                ]
            )
            writer.append_csv(output_dir, TABLE_NAME, report_date, row)
            rows_written += 1

        if rows_written == 0:
            gap_logger.log(
                TABLE_NAME,
                indicator_name,
                f"{start_date}..{end_date}",
                "no values in range",
            )

        writer.mark_done(output_dir, TABLE_NAME, indicator_name)
