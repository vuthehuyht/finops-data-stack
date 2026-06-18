"""Unit tests for world-index-based ingestion pipelines (no symbol loop)."""

import pytest

from src.ingest.pipeline.commodities_price import CommoditiesPricePipeline
from src.ingest.pipeline.exchange_rates import ExchangeRatesPipeline
from src.ingest.pipeline.interest_rates import InterestRatesPipeline
from src.ingest.pipeline.macro_indicators import MacroIndicatorsPipeline

# ---------------------------------------------------------------------------
# MacroIndicatorsPipeline
# ---------------------------------------------------------------------------


def test_macro_indicators_pipeline_fetch_raises_not_implemented() -> None:
    """Verify fetch raises NotImplementedError."""
    pipeline = MacroIndicatorsPipeline(batch_date="2026-06-18")

    with pytest.raises(NotImplementedError):
        pipeline.fetch()


# ---------------------------------------------------------------------------
# ExchangeRatesPipeline
# ---------------------------------------------------------------------------


def test_exchange_rates_pipeline_fetch_raises_not_implemented() -> None:
    """Verify fetch raises NotImplementedError."""
    pipeline = ExchangeRatesPipeline(batch_date="2026-06-18")

    with pytest.raises(NotImplementedError):
        pipeline.fetch()


# ---------------------------------------------------------------------------
# InterestRatesPipeline
# ---------------------------------------------------------------------------


def test_interest_rates_pipeline_fetch_raises_not_implemented() -> None:
    """Verify fetch raises NotImplementedError."""
    pipeline = InterestRatesPipeline(batch_date="2026-06-18")

    with pytest.raises(NotImplementedError):
        pipeline.fetch()


# ---------------------------------------------------------------------------
# CommoditiesPricePipeline
# ---------------------------------------------------------------------------


def test_commodities_price_pipeline_fetch_raises_not_implemented() -> None:
    """Verify fetch raises NotImplementedError."""
    pipeline = CommoditiesPricePipeline(batch_date="2026-06-18")

    with pytest.raises(NotImplementedError):
        pipeline.fetch()
