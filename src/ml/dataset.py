"""Dataset utilities for ML training: sequence windowing and time-based splitting."""

import numpy as np
import pandas as pd
import torch
from torch.utils.data import Dataset

try:
    # Package-relative import: used when pytest imports this module as
    # `src.ml.dataset` from the repo root, where the `src` package resolves.
    from src.ml.config import (
        SEQUENCE_FEATURE_COLUMNS,
        TABULAR_FEATURE_COLUMNS,
        TARGET_COLUMN,
        WINDOW_SIZE,
    )
except ImportError:
    # Sibling import: SageMaker script mode copies `source_dir`'s contents
    # flat into /opt/ml/input/data/code/, so there is no `src` package there
    # — config.py is a plain sibling of dataset.py in that directory.
    from config import (
        SEQUENCE_FEATURE_COLUMNS,
        TABULAR_FEATURE_COLUMNS,
        TARGET_COLUMN,
        WINDOW_SIZE,
    )

_DATE_COLUMN = "TRADING_DATE"
_TICKER_COLUMN = "TICKER"


def time_based_split(
    df: pd.DataFrame,
    train_end_date: str,
    val_end_date: str,
    date_column: str = _DATE_COLUMN,
) -> tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame]:
    """Split a DataFrame into train/val/test by trading date cutoffs.

    No shuffling — the split is purely chronological to avoid look-ahead bias.

    Args:
        df: Input feature set, must contain `date_column`.
        train_end_date: Last date (inclusive) included in the training split.
        val_end_date: Last date (inclusive) included in the validation split.
        date_column: Name of the date column to split on.

    Returns:
        (train_df, val_df, test_df) — each a copy of the matching date range.

    Raises:
        ValueError: If `val_end_date` is before `train_end_date`.
    """
    train_end = pd.Timestamp(train_end_date)
    val_end = pd.Timestamp(val_end_date)
    if val_end < train_end:
        raise ValueError("val_end_date must not be before train_end_date")

    dates = pd.to_datetime(df[date_column])
    train_df = df.loc[dates <= train_end].copy()
    val_df = df.loc[(dates > train_end) & (dates <= val_end)].copy()
    test_df = df.loc[dates > val_end].copy()
    return train_df, val_df, test_df


def _build_ticker_windows(
    ticker_df: pd.DataFrame, window_size: int
) -> list[pd.DataFrame]:
    """Build all valid trailing windows of `window_size` rows for a single ticker."""
    sorted_df = ticker_df.sort_values(_DATE_COLUMN).reset_index(drop=True)
    if len(sorted_df) < window_size:
        return []
    return [
        sorted_df.iloc[end - window_size : end]
        for end in range(window_size, len(sorted_df) + 1)
    ]


class StockSequenceDataset(Dataset):
    """PyTorch Dataset yielding (sequence, tabular, target) samples per ticker window.

    Each sample is a `window_size`-day trailing window for one ticker: the
    sequence branch gets the full window over `sequence_columns`, the tabular
    branch gets only the last row's `tabular_columns` (latest snapshot), and
    the target is the last row's `target_column`.
    """

    def __init__(
        self,
        df: pd.DataFrame,
        window_size: int = WINDOW_SIZE,
        sequence_columns: list[str] | None = None,
        tabular_columns: list[str] | None = None,
        target_column: str = TARGET_COLUMN,
    ) -> None:
        self._window_size = window_size
        self._sequence_columns = sequence_columns or SEQUENCE_FEATURE_COLUMNS
        self._tabular_columns = tabular_columns or TABULAR_FEATURE_COLUMNS
        self._target_column = target_column
        self._windows = self._build_windows(df)

    def _build_windows(self, df: pd.DataFrame) -> list[pd.DataFrame]:
        clean_df = df.dropna(subset=[self._target_column])
        windows: list[pd.DataFrame] = []
        for _, ticker_df in clean_df.groupby(_TICKER_COLUMN):
            windows.extend(_build_ticker_windows(ticker_df, self._window_size))
        return windows

    def __len__(self) -> int:
        return len(self._windows)

    def __getitem__(
        self, index: int
    ) -> tuple[torch.Tensor, torch.Tensor, torch.Tensor]:
        window = self._windows[index]
        sequence = window[self._sequence_columns].fillna(0.0).to_numpy(dtype=np.float32)
        tabular = (
            window[self._tabular_columns]
            .iloc[-1]
            .fillna(0.0)
            .to_numpy(dtype=np.float32)
        )
        target = np.float32(window[self._target_column].iloc[-1])
        return (
            torch.from_numpy(sequence),
            torch.from_numpy(tabular),
            torch.tensor(target),
        )
