"""Inference-time utilities: data quality gate, latest-window builder, prediction.

No AWS dependency — reused by both the Dagster daily inference pipeline
(src/dagster/inference_job.py) and the SageMaker serving entrypoint
(src/ml/serve.py).

`torch` is imported lazily (inside the two functions that need it) rather
than at module level: the Dagster orchestrator only calls the
torch-independent functions here (check_feature_null_rate,
build_latest_window, next_trading_day) and does not have torch installed
(it runs in a separate, lighter image than the SageMaker training/inference
containers, which do have it) -- see src/docker/Dockerfile.
"""

import datetime
from typing import TYPE_CHECKING

import numpy as np
import pandas as pd

if TYPE_CHECKING:
    pass

try:
    # Package-relative import: used when pytest imports this module as
    # `src.ml.inference` from the repo root, where the `src` package resolves.
    from src.ml import features
    from src.ml.config import (
        SEQUENCE_FEATURE_COLUMNS,
        TABULAR_FEATURE_COLUMNS,
    )
except ImportError:
    # Sibling import: SageMaker script mode copies `source_dir`'s contents
    # flat into /opt/ml/input/data/code/, so there is no `src` package there
    # — config.py is a plain sibling of inference.py in that directory.
    import features  # noqa: I001

    from config import (  # noqa: I001
        SEQUENCE_FEATURE_COLUMNS,
        TABULAR_FEATURE_COLUMNS,
    )

_DATE_COLUMN = "trading_date"
_TICKER_COLUMN = "ticker"


def next_trading_day(anchor_date: datetime.date) -> datetime.date:
    """Return the next Mon-Fri weekday after `anchor_date`.

    No VN public-holiday calendar exists in this codebase (same limitation
    as the ingest cron `30 15 * * 1-5`) — only Saturday/Sunday are skipped.
    """
    candidate = anchor_date + datetime.timedelta(days=1)
    while candidate.weekday() >= 5:  # 5=Saturday, 6=Sunday
        candidate += datetime.timedelta(days=1)
    return candidate


def check_feature_null_rate(
    df: pd.DataFrame, columns: list[str], threshold: float
) -> dict[str, float]:
    """Compute the null rate of each column and raise if any exceeds threshold.

    Args:
        df: Rows to check (caller filters to the trading date being gated).
        columns: Column names to check.
        threshold: Maximum acceptable null rate, e.g. 0.2 for 20%.

    Returns:
        Mapping of column name to its null rate (0.0-1.0).

    Raises:
        ValueError: If `df` is empty, or if any column's null rate exceeds
            `threshold`.
    """
    if len(df) == 0:
        raise ValueError("Cannot compute null rates on an empty DataFrame")

    null_rates = {column: float(df[column].isna().mean()) for column in columns}
    breaches = {column: rate for column, rate in null_rates.items() if rate > threshold}
    if breaches:
        details = ", ".join(f"{column}={rate:.2%}" for column, rate in breaches.items())
        raise ValueError(
            f"Data quality gate failed: null rate exceeds {threshold:.2%} "
            f"threshold for: {details}"
        )
    return null_rates


def check_sector_aware_completeness(
    df: pd.DataFrame,
    *,
    null_rate_threshold: float,
    min_ticker_completeness: float,
    max_incomplete_ticker_ratio: float,
) -> dict:
    """Per-ticker, sector-aware completeness check for the latest date.

    - Sequence features must be clean: any column whose null rate exceeds
      ``null_rate_threshold`` fails the gate (the sequence branch only ever
      imputes 0.0).
    - For each ticker, completeness = fraction of its APPLICABLE tabular
      features that are non-null. A ticker below ``min_ticker_completeness``
      is "incomplete". Fails if the incomplete ratio exceeds
      ``max_incomplete_ticker_ratio``.

    Returns a dict with ``sector_breakdown`` (ticker count + mean null rate
    per sector) and ``incomplete_ticker_ratio``.

    Raises:
        ValueError: If ``df`` is empty or either threshold is breached.
    """
    if len(df) == 0:
        raise ValueError("Data quality gate: no rows for the latest trading date")

    for col in SEQUENCE_FEATURE_COLUMNS:
        rate = float(df[col].isna().mean())
        if rate > null_rate_threshold:
            raise ValueError(
                f"Data quality gate: sequence feature {col} null rate "
                f"{rate:.2%} exceeds {null_rate_threshold:.2%}"
            )

    has_sector = "sector" in df.columns
    incomplete = 0
    per_sector: dict[str, list[float]] = {}
    for _, row in df.iterrows():
        sector = str(row["sector"]) if has_sector else "non_financial"
        applicable = [c for c in TABULAR_FEATURE_COLUMNS if features.applies(c, sector)]
        if not applicable:
            completeness = 1.0
        else:
            present = sum(1 for c in applicable if not pd.isna(row[c]))
            completeness = present / len(applicable)
        per_sector.setdefault(sector, []).append(1.0 - completeness)
        if completeness < min_ticker_completeness:
            incomplete += 1

    incomplete_ratio = incomplete / len(df)
    if incomplete_ratio > max_incomplete_ticker_ratio:
        raise ValueError(
            f"Data quality gate: {incomplete_ratio:.2%} of tickers are "
            f"incomplete (> {max_incomplete_ticker_ratio:.2%})"
        )

    return {
        "sector_breakdown": {
            sector: {
                "tickers": len(rates),
                "mean_null_rate": float(np.mean(rates)),
            }
            for sector, rates in per_sector.items()
        },
        "incomplete_ticker_ratio": incomplete_ratio,
    }


def build_latest_window(
    df: pd.DataFrame,
    ticker: str,
    window_size: int,
    sequence_columns: list[str] | None = None,
    tabular_columns: list[str] | None = None,
) -> tuple[np.ndarray, pd.Series, str]:
    """Build the most recent `window_size`-day window for one ticker.

    Mirrors `StockSequenceDataset`'s per-window slicing (src/ml/dataset.py) but
    standalone: no label, no PyTorch Dataset wrapper — inference has no target.

    Args:
        df: Feature rows for (at least) this ticker, any date range.
        ticker: Ticker to build the window for.
        window_size: Number of trailing trading days required.
        sequence_columns: Defaults to SEQUENCE_FEATURE_COLUMNS.
        tabular_columns: Defaults to TABULAR_FEATURE_COLUMNS.

    Returns:
        `(sequence_array, tabular_row, sector)`: sequence has shape
        `(window_size, len(sequence_columns))` (NaN -> 0.0); `tabular_row`
        is the last row's raw Series over `tabular_columns` (featurized by
        the caller); `sector` is the ticker's sector string (defaults to
        `"non_financial"` if the column is absent).

    Raises:
        ValueError: If fewer than `window_size` rows exist for `ticker`.
    """
    sequence_columns = sequence_columns or SEQUENCE_FEATURE_COLUMNS
    tabular_columns = tabular_columns or TABULAR_FEATURE_COLUMNS

    ticker_df = df.loc[df[_TICKER_COLUMN] == ticker].sort_values(_DATE_COLUMN)
    if len(ticker_df) < window_size:
        raise ValueError(
            f"Ticker {ticker} has {len(ticker_df)} rows, need >= {window_size} "
            "for a full window."
        )

    window = ticker_df.iloc[-window_size:]
    sequence = features.sequence_features(window, columns=sequence_columns)
    tabular_row = window[tabular_columns].iloc[-1]
    sector = (
        str(ticker_df["sector"].iloc[-1])
        if "sector" in ticker_df.columns
        else "non_financial"
    )
    return sequence, tabular_row, sector


def predict_from_payload(bundle: tuple, payload: dict) -> dict:
    """Run one forward pass for a single ticker payload.

    Args:
        bundle: `(model, medians, sector_vocab)` — see
            `src/ml/serve.py::model_fn`.
        payload: `{"ticker", "sequence": [[...]], "tabular": {col: val},
            "sector"}`.

    Returns:
        `{"predicted_return": float | None}` — `None` when the model output
        is non-finite, so the downstream Redshift COPY loads `NULL` rather
        than choking on the literal `NaN`.
    """
    import math

    import torch

    model, medians, _sector_vocab = bundle
    sector = str(payload["sector"])
    values, flags = features.build_tabular_features(payload["tabular"], sector, medians)
    sequence = torch.tensor([payload["sequence"]], dtype=torch.float32)
    tabular = torch.tensor([list(values) + list(flags)], dtype=torch.float32)
    sector_idx = torch.tensor([features.sector_index(sector)], dtype=torch.long)
    with torch.no_grad():
        prediction = model(sequence, tabular, sector_idx)
    value = prediction.item()
    if not math.isfinite(value):
        return {"predicted_return": None}
    return {"predicted_return": value}
