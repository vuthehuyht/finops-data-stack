"""Inference-time utilities: data quality gate, latest-window builder, prediction.

No AWS dependency — reused by both the Dagster daily inference pipeline
(src/dagster/inference_job.py) and the SageMaker serving entrypoint
(src/ml/serve.py).
"""

import numpy as np
import pandas as pd
import torch

try:
    # Package-relative import: used when pytest imports this module as
    # `src.ml.inference` from the repo root, where the `src` package resolves.
    from src.ml.config import SEQUENCE_FEATURE_COLUMNS, TABULAR_FEATURE_COLUMNS
except ImportError:
    # Sibling import: SageMaker script mode copies `source_dir`'s contents
    # flat into /opt/ml/input/data/code/, so there is no `src` package there
    # — config.py is a plain sibling of inference.py in that directory.
    from config import SEQUENCE_FEATURE_COLUMNS, TABULAR_FEATURE_COLUMNS

_DATE_COLUMN = "TRADING_DATE"
_TICKER_COLUMN = "TICKER"


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


def build_latest_window(
    df: pd.DataFrame,
    ticker: str,
    window_size: int,
    sequence_columns: list[str] | None = None,
    tabular_columns: list[str] | None = None,
) -> tuple[np.ndarray, np.ndarray]:
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
        `(sequence_array, tabular_array)`: sequence has shape
        `(window_size, len(sequence_columns))`; tabular has shape
        `(len(tabular_columns),)` — the last row's snapshot.

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
    sequence = window[sequence_columns].to_numpy(dtype=np.float32)
    tabular = window[tabular_columns].iloc[-1].to_numpy(dtype=np.float32)
    return sequence, tabular


def predict_from_payload(model: torch.nn.Module, payload: dict) -> dict:
    """Run a forward pass given a JSON-deserialized inference payload.

    Args:
        model: A `FusionModel` instance in `eval()` mode.
        payload: `{"sequence": [[...]], "tabular": [...]}`.

    Returns:
        `{"predicted_return": <float>}`.
    """
    sequence = torch.tensor([payload["sequence"]], dtype=torch.float32)
    tabular = torch.tensor([payload["tabular"]], dtype=torch.float32)
    with torch.no_grad():
        prediction = model(sequence, tabular)
    return {"predicted_return": prediction.item()}
