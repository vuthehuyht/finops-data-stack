"""Dagster assets and jobs for Ingestion Layer (Crawl/API -> S3)."""

import datetime
import functools
from dataclasses import dataclass, field

import dagster
import pydantic
from dagster._core.definitions.unresolved_asset_job_definition import (
    UnresolvedAssetJobDefinition,
)
from dagster_aws.s3 import S3Resource

import src.pipeline.dagster as dagster_lib
from src.dagster.resources import S3BucketResource
from src.ingest.pipeline.analyst_reports import AnalystReportsPipeline
from src.ingest.pipeline.balance_sheet import BalanceSheetPipeline
from src.ingest.pipeline.cashflow_statement import CashflowStatementPipeline
from src.ingest.pipeline.commodities_price import CommoditiesPricePipeline
from src.ingest.pipeline.company_profile import CompanyProfilePipeline
from src.ingest.pipeline.corporate_events import CorporateEventsPipeline
from src.ingest.pipeline.exchange_rates import ExchangeRatesPipeline
from src.ingest.pipeline.foreign_trading import ForeignTradingPipeline
from src.ingest.pipeline.income_statement import IncomeStatementPipeline
from src.ingest.pipeline.index_price_eod import IndexPriceEodPipeline
from src.ingest.pipeline.interest_rates import InterestRatesPipeline
from src.ingest.pipeline.macro_indicators import MacroIndicatorsPipeline
from src.ingest.pipeline.news_articles import NewsArticlesPipeline
from src.ingest.pipeline.proprietary_trading import ProprietaryTradingPipeline
from src.ingest.pipeline.stock_price_eod import StockPriceEodPipeline

_TIMEZONE = "Asia/Ho_Chi_Minh"
# 15:30 ICT on weekdays — after VN market closes at 15:00
_INGEST_CRON = "30 15 * * 1-5"


@dataclass
class IngestJobBundle:
    """Return value of define_ingest_jobs() — consumed by workspace.py."""

    assets: list[dagster.AssetsDefinition] = field(default_factory=list)
    jobs: list[dagster.JobDefinition | UnresolvedAssetJobDefinition] = field(
        default_factory=list
    )
    schedules: list[dagster.ScheduleDefinition] = field(default_factory=list)


class IngestAssetConfig(dagster.Config):
    """Runtime config injected per asset run."""

    batch_date: str = pydantic.Field(
        default_factory=lambda: datetime.date.today().isoformat(),
        description="Partition date in YYYY-MM-DD format.",
    )
    symbols: list[str] = pydantic.Field(
        default_factory=list, description="Target stock symbols."
    )


def _build_output(
    s3_url: str, batch_date: str, symbols: list[str]
) -> dagster.Output[None]:
    return dagster.Output(
        value=None,
        metadata={
            "s3_url": s3_url,
            "batch_date": batch_date,
            "symbols_count": len(symbols),
        },
    )


def _make_ingest_schedule(
    job: dagster.JobDefinition | UnresolvedAssetJobDefinition,
    job_name: str,
    asset_py_id: str,
) -> dagster.ScheduleDefinition:
    """Create a daily schedule that runs an ingest job after VN market close."""

    @dagster.schedule(
        job=job,
        cron_schedule=_INGEST_CRON,
        execution_timezone=_TIMEZONE,
        name=f"{job_name}_schedule",
        description=f"Daily VN market ingest schedule for {job_name}.",
    )
    def _schedule(
        context: dagster.ScheduleEvaluationContext,
    ) -> dagster.RunRequest:
        batch_date = context.scheduled_execution_time.date().isoformat()
        return dagster.RunRequest(
            run_config=dagster.RunConfig(
                ops={
                    asset_py_id: IngestAssetConfig(
                        batch_date=batch_date,
                        symbols=[],  # falls back to DEFAULT_TICKER_SYMBOLS in pipeline
                    )
                }
            )
        )

    return _schedule


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
        logger=context.log,
    )
    s3_url = pipeline.run()
    return _build_output(s3_url, config.batch_date, config.symbols)


@dagster_lib.asset(
    key=dagster_lib.asset_key(["INPUT", "RAW_INDEX_PRICE_EOD"]),
    group_name="INPUT",
    kinds={"python", "s3"},
    description="Fetch daily market index prices (VNINDEX, HNX) and upload to S3.",
)
def raw_index_price_eod(
    context,
    config: IngestAssetConfig,
    s3: S3Resource,
    s3bucket: S3BucketResource,
) -> dagster.Output[None]:
    context.log.info("Starting index price ingestion on date %s", config.batch_date)
    pipeline = IndexPriceEodPipeline(
        batch_date=config.batch_date,
        symbols=config.symbols,
        s3_client=s3.get_client(),
        bucket_name=s3bucket.raw_bucket,
        logger=context.log,
    )
    s3_url = pipeline.run()
    return _build_output(s3_url, config.batch_date, config.symbols)


@dagster_lib.asset(
    key=dagster_lib.asset_key(["INPUT", "RAW_FOREIGN_TRADING"]),
    group_name="INPUT",
    kinds={"python", "s3"},
    description="Fetch foreign investor trading flow and upload to S3 Bronze.",
)
def raw_foreign_trading(
    context,
    config: IngestAssetConfig,
    s3: S3Resource,
    s3bucket: S3BucketResource,
) -> dagster.Output[None]:
    context.log.info(
        "Starting foreign trading ingestion for %d symbols on date %s",
        len(config.symbols),
        config.batch_date,
    )
    pipeline = ForeignTradingPipeline(
        batch_date=config.batch_date,
        symbols=config.symbols,
        s3_client=s3.get_client(),
        bucket_name=s3bucket.raw_bucket,
        logger=context.log,
    )
    s3_url = pipeline.run()
    return _build_output(s3_url, config.batch_date, config.symbols)


@dagster_lib.asset(
    key=dagster_lib.asset_key(["INPUT", "RAW_PROPRIETARY_TRADING"]),
    group_name="INPUT",
    kinds={"python", "s3"},
    description="Fetch proprietary (self-trading) flow and upload to S3 Bronze.",
)
def raw_proprietary_trading(
    context,
    config: IngestAssetConfig,
    s3: S3Resource,
    s3bucket: S3BucketResource,
) -> dagster.Output[None]:
    context.log.info(
        "Starting proprietary trading ingestion for %d symbols on date %s",
        len(config.symbols),
        config.batch_date,
    )
    pipeline = ProprietaryTradingPipeline(
        batch_date=config.batch_date,
        symbols=config.symbols,
        s3_client=s3.get_client(),
        bucket_name=s3bucket.raw_bucket,
        logger=context.log,
    )
    s3_url = pipeline.run()
    return _build_output(s3_url, config.batch_date, config.symbols)


@dagster_lib.asset(
    key=dagster_lib.asset_key(["INPUT", "RAW_BALANCE_SHEET"]),
    group_name="INPUT",
    kinds={"python", "s3"},
    description="Fetch quarterly balance sheets from TCBS and upload to S3 Bronze.",
)
def raw_balance_sheet(
    context,
    config: IngestAssetConfig,
    s3: S3Resource,
    s3bucket: S3BucketResource,
) -> dagster.Output[None]:
    context.log.info(
        "Starting balance sheet ingestion for %d symbols on date %s",
        len(config.symbols),
        config.batch_date,
    )
    pipeline = BalanceSheetPipeline(
        batch_date=config.batch_date,
        symbols=config.symbols,
        s3_client=s3.get_client(),
        bucket_name=s3bucket.raw_bucket,
        logger=context.log,
    )
    s3_url = pipeline.run()
    return _build_output(s3_url, config.batch_date, config.symbols)


@dagster_lib.asset(
    key=dagster_lib.asset_key(["INPUT", "RAW_INCOME_STATEMENT"]),
    group_name="INPUT",
    kinds={"python", "s3"},
    description="Fetch quarterly income statements from TCBS and upload to S3 Bronze.",
)
def raw_income_statement(
    context,
    config: IngestAssetConfig,
    s3: S3Resource,
    s3bucket: S3BucketResource,
) -> dagster.Output[None]:
    context.log.info(
        "Starting income statement ingestion for %d symbols on date %s",
        len(config.symbols),
        config.batch_date,
    )
    pipeline = IncomeStatementPipeline(
        batch_date=config.batch_date,
        symbols=config.symbols,
        s3_client=s3.get_client(),
        bucket_name=s3bucket.raw_bucket,
        logger=context.log,
    )
    s3_url = pipeline.run()
    return _build_output(s3_url, config.batch_date, config.symbols)


@dagster_lib.asset(
    key=dagster_lib.asset_key(["INPUT", "RAW_CASHFLOW_STATEMENT"]),
    group_name="INPUT",
    kinds={"python", "s3"},
    description="Fetch quarterly cashflow statements from TCBS and upload to S3.",
)
def raw_cashflow_statement(
    context,
    config: IngestAssetConfig,
    s3: S3Resource,
    s3bucket: S3BucketResource,
) -> dagster.Output[None]:
    context.log.info(
        "Starting cashflow statement ingestion for %d symbols on date %s",
        len(config.symbols),
        config.batch_date,
    )
    pipeline = CashflowStatementPipeline(
        batch_date=config.batch_date,
        symbols=config.symbols,
        s3_client=s3.get_client(),
        bucket_name=s3bucket.raw_bucket,
        logger=context.log,
    )
    s3_url = pipeline.run()
    return _build_output(s3_url, config.batch_date, config.symbols)


@dagster_lib.asset(
    key=dagster_lib.asset_key(["INPUT", "RAW_COMPANY_PROFILE"]),
    group_name="INPUT",
    kinds={"python", "s3"},
    description="Fetch company profiles (name, industry, exchange) and upload to S3.",
)
def raw_company_profile(
    context,
    config: IngestAssetConfig,
    s3: S3Resource,
    s3bucket: S3BucketResource,
) -> dagster.Output[None]:
    context.log.info(
        "Starting company profile ingestion for %d symbols on date %s",
        len(config.symbols),
        config.batch_date,
    )
    pipeline = CompanyProfilePipeline(
        batch_date=config.batch_date,
        symbols=config.symbols,
        s3_client=s3.get_client(),
        bucket_name=s3bucket.raw_bucket,
        logger=context.log,
    )
    s3_url = pipeline.run()
    return _build_output(s3_url, config.batch_date, config.symbols)


@dagster_lib.asset(
    key=dagster_lib.asset_key(["INPUT", "RAW_MACRO_INDICATORS"]),
    group_name="INPUT",
    kinds={"python", "s3"},
    description="Fetch macro indicators (GDP, CPI, world index) and upload to S3.",
)
def raw_macro_indicators(
    context,
    config: IngestAssetConfig,
    s3: S3Resource,
    s3bucket: S3BucketResource,
) -> dagster.Output[None]:
    context.log.info(
        "Starting macro indicators ingestion on date %s", config.batch_date
    )
    pipeline = MacroIndicatorsPipeline(
        batch_date=config.batch_date,
        symbols=config.symbols,
        s3_client=s3.get_client(),
        bucket_name=s3bucket.raw_bucket,
        logger=context.log,
    )
    s3_url = pipeline.run()
    return _build_output(s3_url, config.batch_date, config.symbols)


@dagster_lib.asset(
    key=dagster_lib.asset_key(["INPUT", "RAW_INTEREST_RATES"]),
    group_name="INPUT",
    kinds={"python", "s3"},
    description="Fetch banking interest rates and upload to S3 Bronze.",
)
def raw_interest_rates(
    context,
    config: IngestAssetConfig,
    s3: S3Resource,
    s3bucket: S3BucketResource,
) -> dagster.Output[None]:
    context.log.info("Starting interest rates ingestion on date %s", config.batch_date)
    pipeline = InterestRatesPipeline(
        batch_date=config.batch_date,
        symbols=config.symbols,
        s3_client=s3.get_client(),
        bucket_name=s3bucket.raw_bucket,
        logger=context.log,
    )
    s3_url = pipeline.run()
    return _build_output(s3_url, config.batch_date, config.symbols)


@dagster_lib.asset(
    key=dagster_lib.asset_key(["INPUT", "RAW_EXCHANGE_RATES"]),
    group_name="INPUT",
    kinds={"python", "s3"},
    description="Fetch daily currency exchange rates and upload to S3 Bronze.",
)
def raw_exchange_rates(
    context,
    config: IngestAssetConfig,
    s3: S3Resource,
    s3bucket: S3BucketResource,
) -> dagster.Output[None]:
    context.log.info("Starting exchange rates ingestion on date %s", config.batch_date)
    pipeline = ExchangeRatesPipeline(
        batch_date=config.batch_date,
        symbols=config.symbols,
        s3_client=s3.get_client(),
        bucket_name=s3bucket.raw_bucket,
        logger=context.log,
    )
    s3_url = pipeline.run()
    return _build_output(s3_url, config.batch_date, config.symbols)


@dagster_lib.asset(
    key=dagster_lib.asset_key(["INPUT", "RAW_COMMODITIES_PRICE"]),
    group_name="INPUT",
    kinds={"python", "s3"},
    description="Fetch commodity prices (oil, gold) and upload to S3 Bronze.",
)
def raw_commodities_price(
    context,
    config: IngestAssetConfig,
    s3: S3Resource,
    s3bucket: S3BucketResource,
) -> dagster.Output[None]:
    context.log.info(
        "Starting commodities price ingestion on date %s", config.batch_date
    )
    pipeline = CommoditiesPricePipeline(
        batch_date=config.batch_date,
        symbols=config.symbols,
        s3_client=s3.get_client(),
        bucket_name=s3bucket.raw_bucket,
        logger=context.log,
    )
    s3_url = pipeline.run()
    return _build_output(s3_url, config.batch_date, config.symbols)


@dagster_lib.asset(
    key=dagster_lib.asset_key(["INPUT", "RAW_NEWS_ARTICLES"]),
    group_name="INPUT",
    kinds={"python", "s3"},
    description="Fetch corporate news articles from TCBS and upload to S3 Bronze.",
)
def raw_news_articles(
    context,
    config: IngestAssetConfig,
    s3: S3Resource,
    s3bucket: S3BucketResource,
) -> dagster.Output[None]:
    context.log.info(
        "Starting news articles ingestion for %d symbols on date %s",
        len(config.symbols),
        config.batch_date,
    )
    pipeline = NewsArticlesPipeline(
        batch_date=config.batch_date,
        symbols=config.symbols,
        s3_client=s3.get_client(),
        bucket_name=s3bucket.raw_bucket,
        logger=context.log,
    )
    s3_url = pipeline.run()
    return _build_output(s3_url, config.batch_date, config.symbols)


@dagster_lib.asset(
    key=dagster_lib.asset_key(["INPUT", "RAW_CORPORATE_EVENTS"]),
    group_name="INPUT",
    kinds={"python", "s3"},
    description="Fetch corporate calendar events (dividends, AGM) and upload to S3.",
)
def raw_corporate_events(
    context,
    config: IngestAssetConfig,
    s3: S3Resource,
    s3bucket: S3BucketResource,
) -> dagster.Output[None]:
    context.log.info(
        "Starting corporate events ingestion for %d symbols on date %s",
        len(config.symbols),
        config.batch_date,
    )
    pipeline = CorporateEventsPipeline(
        batch_date=config.batch_date,
        symbols=config.symbols,
        s3_client=s3.get_client(),
        bucket_name=s3bucket.raw_bucket,
        logger=context.log,
    )
    s3_url = pipeline.run()
    return _build_output(s3_url, config.batch_date, config.symbols)


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
        logger=context.log,
    )
    s3_url = pipeline.run()
    return _build_output(s3_url, config.batch_date, config.symbols)


_ALL_INGEST_ASSETS: list[dagster.AssetsDefinition] = [
    raw_stock_price_eod,
    raw_index_price_eod,
    raw_foreign_trading,
    raw_proprietary_trading,
    raw_balance_sheet,
    raw_income_statement,
    raw_cashflow_statement,
    raw_company_profile,
    raw_macro_indicators,
    raw_interest_rates,
    raw_exchange_rates,
    raw_commodities_price,
    raw_news_articles,
    raw_corporate_events,
    raw_analyst_reports,
]


@functools.cache
def define_ingest_jobs() -> IngestJobBundle:
    """Build Dagster assets, jobs, and schedules for all INPUT ingestion assets."""
    bundle = IngestJobBundle()

    for asset in _ALL_INGEST_ASSETS:
        bundle.assets.append(asset)

        job_name = f"ingest_{asset.key.to_python_identifier()}_job"
        job = dagster_lib.define_asset_job(
            job_name,
            selection=[asset],
            tags={
                "limit_concurrent_job_runs_to_1": job_name,
                "type": "ingest",
            },
        )
        bundle.jobs.append(job)
        bundle.schedules.append(
            _make_ingest_schedule(job, job_name, asset.key.to_python_identifier())
        )

    return bundle
