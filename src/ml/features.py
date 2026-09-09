"""Shared feature construction for training and serving.

Torch-free on purpose: imported by the Dagster orchestrator (which runs in
a lighter image without ``torch``) as well as the SageMaker containers.
Also copied flat into ``model.tar.gz/code/``, so the config import uses the
dual package/sibling form the rest of ``src/ml`` uses.
"""

from collections.abc import Mapping

import numpy as np
import pandas as pd

try:
    # Package-relative import: used when pytest imports this module as
    # ``src.ml.features`` from the repo root.
    from src.ml.config import (
        FEATURE_APPLICABILITY,
        SECTOR_VOCAB,
        SEQUENCE_FEATURE_COLUMNS,
        TABULAR_FEATURE_COLUMNS,
    )
except ImportError:
    # Sibling import: SageMaker copies the bundled ``code/`` directory flat,
    # so config.py is a plain sibling of features.py there.
    from config import (
        FEATURE_APPLICABILITY,
        SECTOR_VOCAB,
        SEQUENCE_FEATURE_COLUMNS,
        TABULAR_FEATURE_COLUMNS,
    )

_DEFAULT_SECTOR = "non_financial"


def sector_index(sector: str) -> int:
    """Embedding index for ``sector``; unknown values map to non_financial."""
    try:
        return SECTOR_VOCAB.index(sector)
    except ValueError:
        return SECTOR_VOCAB.index(_DEFAULT_SECTOR)


def _applies(col: str, sector: str) -> bool:
    """True unless ``col`` has an explicit applicability set excluding ``sector``."""
    allowed = FEATURE_APPLICABILITY.get(col)
    return allowed is None or sector in allowed


def median_lookup(medians: Mapping[str, float], sector: str, col: str) -> float:
    """Sector median, then global median, then 0.0."""
    if f"{sector}::{col}" in medians:
        return float(medians[f"{sector}::{col}"])
    if col in medians:
        return float(medians[col])
    return 0.0


def _is_missing(value: object) -> bool:
    return value is None or (isinstance(value, float) and np.isnan(value))


def build_tabular_features(
    row: Mapping[str, float] | pd.Series,
    sector: str,
    medians: Mapping[str, float],
) -> tuple[np.ndarray, np.ndarray]:
    """Return (values, applicability_flags) for TABULAR_FEATURE_COLUMNS.

    Per column, in order:

    - not applicable to ``sector`` -> value 0.0, flag 0.0
    - applicable but missing        -> value ``median_lookup(...)``, flag 0.0
    - otherwise                     -> value ``float(row[col])``, flag 1.0
    """
    values = np.zeros(len(TABULAR_FEATURE_COLUMNS), dtype=np.float32)
    flags = np.zeros(len(TABULAR_FEATURE_COLUMNS), dtype=np.float32)
    for i, col in enumerate(TABULAR_FEATURE_COLUMNS):
        if not _applies(col, sector):
            continue
        value = row[col] if col in row else None
        if _is_missing(value):
            values[i] = median_lookup(medians, sector, col)
        else:
            values[i] = np.float32(value)
            flags[i] = 1.0
    return values, flags


def sequence_features(window_df: pd.DataFrame) -> np.ndarray:
    """Sequence branch input: NaN -> 0.0, float32."""
    return window_df[SEQUENCE_FEATURE_COLUMNS].fillna(0.0).to_numpy(dtype=np.float32)


def compute_training_medians(train_df: pd.DataFrame) -> dict[str, float]:
    """Per-sector and global medians over applicable, non-null values.

    Keys: ``"{sector}::{col}"`` and ``"{col}"``. Computed on the training
    split only -- never val/test -- to avoid leakage.
    """
    medians: dict[str, float] = {}
    for col in TABULAR_FEATURE_COLUMNS:
        applicable = train_df[train_df["sector"].map(lambda s, _c=col: _applies(_c, s))]
        global_series = applicable[col].dropna()
        if not global_series.empty:
            medians[col] = float(global_series.median())
        for sector, group in applicable.groupby("sector"):
            series = group[col].dropna()
            if not series.empty:
                medians[f"{sector}::{col}"] = float(series.median())
    return medians
