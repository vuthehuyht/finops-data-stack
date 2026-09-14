"""Ingestion pipeline for RAW_ANALYST_REPORTS."""

import os

import pandas as pd

from src.ingest.client.fireant_client import FireAntClient
from src.ingest.pipeline.base import BaseIngestPipeline


class AnalystReportsPipeline(BaseIngestPipeline):
    """Pipeline to ingest analyst research reports from FireAnt into S3 Bronze."""

    @property
    def table_name(self) -> str:
        return "RAW_ANALYST_REPORTS"

    @property
    def source_uri_prefix(self) -> str:
        return "api://fireant/reports"

    @property
    def schema_columns(self) -> list[str]:
        return [
            "report_id",
            "ticker",
            "brokerage_firm",
            "publish_date",
            "title",
            "description",
            "file_name",
        ]

    def fetch(self) -> pd.DataFrame:
        """Fetch analyst reports on the batch date.

        If symbols are explicitly provided, fetch per symbol.
        Otherwise, fetch all market analyst reports published on the batch date.
        """
        client = FireAntClient(
            email=os.environ["FIREANT_EMAIL"],
            password=os.environ["FIREANT_PASSWORD"],
        )
        all_rows: list[dict] = []

        if self.symbols:
            for symbol in self.symbols:
                try:
                    reports = client.get_reports(
                        symbol=symbol,
                        start_date=self.batch_date,
                        end_date=self.batch_date,
                    )
                    for r in reports:
                        all_rows.append(
                            {
                                "report_id": r.get("reportID"),
                                "ticker": r.get("symbol") or symbol,
                                "brokerage_firm": r.get("sourceName"),
                                "publish_date": r.get("date"),
                                "title": r.get("title"),
                                "description": r.get("description"),
                                "file_name": r.get("fileName"),
                            }
                        )
                except Exception as e:
                    self.logger.error(
                        "Failed to fetch analyst reports for %s: %s", symbol, e
                    )
                    raise e
        else:
            try:
                reports = client.get_reports(
                    symbol="",
                    start_date=self.batch_date,
                    end_date=self.batch_date,
                )
                for r in reports:
                    all_rows.append(
                        {
                            "report_id": r.get("reportID"),
                            "ticker": r.get("symbol"),
                            "brokerage_firm": r.get("sourceName"),
                            "publish_date": r.get("date"),
                            "title": r.get("title"),
                            "description": r.get("description"),
                            "file_name": r.get("fileName"),
                        }
                    )
            except Exception as e:
                self.logger.error(
                    "Failed to fetch all analyst reports for %s: %s",
                    self.batch_date,
                    e,
                )
                raise e

        if not all_rows:
            return pd.DataFrame()

        return pd.DataFrame(all_rows)
