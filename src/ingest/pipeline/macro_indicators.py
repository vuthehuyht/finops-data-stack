"""Ingestion pipeline for RAW_MACRO_INDICATORS."""

import pandas as pd

from src.ingest.client.world_bank_client import WorldBankClient
from src.ingest.pipeline.base import BaseIngestPipeline

# World Bank series IDs for Vietnam macro indicators.
# Data is updated monthly/quarterly by World Bank — fetch uses mrv=5 to
# retrieve recent values and take the latest non-null one per indicator.
DEFAULT_INDICATOR_MAPPING: dict[str, str] = {
    "vn_gdp_usd": "NY.GDP.MKTP.CD",
    "vn_cpi_inflation": "FP.CPI.TOTL.ZG",
    "vn_unemployment": "SL.UEM.TOTL.ZS",
}


def _parse_wb_date(date_str: str) -> str:
    """Convert World Bank date string to ISO date string.

    Handles annual ('2024' -> '2024-01-01'), monthly ('2024M06' -> '2024-06-01'),
    and quarterly ('2024Q2' -> '2024-04-01') formats.
    """
    if "M" in date_str:
        year, month = date_str.split("M")
        return f"{year}-{month.zfill(2)}-01"
    if "Q" in date_str:
        year, quarter = date_str.split("Q")
        month = (int(quarter) - 1) * 3 + 1
        return f"{year}-{str(month).zfill(2)}-01"
    return f"{date_str}-01-01"


class MacroIndicatorsPipeline(BaseIngestPipeline):
    """Pipeline to ingest macro indicators into S3 Bronze."""

    @property
    def table_name(self) -> str:
        return "RAW_MACRO_INDICATORS"

    @property
    def source_uri_prefix(self) -> str:
        return "api://worldbank/macro_indicators"

    @property
    def schema_columns(self) -> list[str]:
        return [
            "indicator_name",
            "report_date",
            "value",
            "unit",
        ]

    def fetch(self) -> pd.DataFrame:
        """Fetch the most recent macro indicator values from World Bank API."""
        client = WorldBankClient()
        all_dfs = []

        self.logger.info(
            "Fetching %d macro indicators from World Bank",
            len(DEFAULT_INDICATOR_MAPPING),
        )

        for indicator_name, series_id in DEFAULT_INDICATOR_MAPPING.items():
            try:
                records = client.get_indicator(series_id)
                # Take first non-null value (records are newest-first)
                record = next((r for r in records if r.get("value") is not None), None)
                if record is None:
                    self.logger.warning(
                        "No non-null value found for indicator %s (%s)",
                        indicator_name,
                        series_id,
                    )
                    continue

                all_dfs.append(
                    pd.DataFrame(
                        [
                            {
                                "indicator_name": indicator_name,
                                "report_date": _parse_wb_date(record["date"]),
                                "value": str(record["value"]),
                                "unit": record.get("unit") or "",
                            }
                        ]
                    )
                )
            except Exception as e:
                self.logger.error(
                    "Failed to fetch indicator %s (%s): %s",
                    indicator_name,
                    series_id,
                    e,
                )
                raise e

        if not all_dfs:
            return pd.DataFrame()

        return pd.concat(all_dfs, ignore_index=True)
