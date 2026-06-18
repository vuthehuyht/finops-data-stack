"""Dagster assets and jobs for Ingestion Layer (Crawl/API -> S3)."""

from collections.abc import Sequence

import dagster
import pydantic
from dagster_aws.s3 import S3Resource

import src.pipeline.dagster as dagster_lib
from src.dagster.resources import S3BucketResource
from src.ingest.pipeline.analyst_reports import AnalystReportsPipeline
from src.ingest.pipeline.stock_price_eod import StockPriceEodPipeline


class IngestAssetConfig(dagster.Config):
    """Runtime config injected per asset run."""

    batch_date: str = pydantic.Field(description="Partition date in YYYY-MM-DD format.")
    symbols: list[str] = pydantic.Field(
        default_factory=list, description="Target stock symbols."
    )


@dagster_lib.asset(
    key=dagster_lib.asset_key(["INPUT", "RAW_STOCK_PRICE_EOD"]),
    group_name="INPUT",
    kinds={"python", "s3"},
    description="Fetch daily EOD stock prices and upload to S3 Bronze.",
)
def raw_stock_price_eod(
    context,
    config: IngestAssetConfig,
    s3: S3Resource,
    s3bucket: S3BucketResource,
) -> dagster.Output[None]:
    """Asset representing raw EOD stock price collection."""
    context.log.info(
        "Starting EOD price ingestion for %d symbols on date %s",
        len(config.symbols),
        config.batch_date,
    )

    pipeline = StockPriceEodPipeline(
        batch_date=config.batch_date,
        symbols=config.symbols,
        s3_client=s3.get_client(),
        bucket_name=s3bucket.raw_bucket,
    )

    s3_url = pipeline.run()

    return dagster.Output(
        value=None,
        metadata={
            "s3_url": s3_url,
            "batch_date": config.batch_date,
            "symbols_count": len(config.symbols),
        },
    )


@dagster_lib.asset(
    key=dagster_lib.asset_key(["INPUT", "RAW_ANALYST_REPORTS"]),
    group_name="INPUT",
    kinds={"python", "s3"},
    description="Fetch analyst research reports from FireAnt and upload to S3 Bronze.",
)
def raw_analyst_reports(
    context,
    config: IngestAssetConfig,
    s3: S3Resource,
    s3bucket: S3BucketResource,
) -> dagster.Output[None]:
    """Asset representing analyst report collection from FireAnt API."""
    context.log.info(
        "Starting analyst reports ingestion for %d symbols on date %s",
        len(config.symbols),
        config.batch_date,
    )

    pipeline = AnalystReportsPipeline(
        batch_date=config.batch_date,
        symbols=config.symbols,
        s3_client=s3.get_client(),
        bucket_name=s3bucket.raw_bucket,
    )

    s3_url = pipeline.run()

    return dagster.Output(
        value=None,
        metadata={
            "s3_url": s3_url,
            "batch_date": config.batch_date,
            "symbols_count": len(config.symbols),
        },
    )


def define_ingest_assets() -> Sequence[dagster.AssetsDefinition]:
    """Return all defined ingestion assets for the workspace."""
    return [raw_stock_price_eod, raw_analyst_reports]
