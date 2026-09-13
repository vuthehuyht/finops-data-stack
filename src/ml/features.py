"""Shared feature construction for training and serving.

Torch-free on purpose: imported by the Dagster orchestrator (which runs in
a lighter image without ``torch``) as well as the SageMaker containers.
Also copied flat into ``model.tar.gz/code/``, so the config import uses the
dual package/sibling form the rest of ``src/ml`` uses.
"""

import math
from collections.abc import Mapping

import numpy as np
import pandas as pd

try:
    # Package-relative import: used when pytest imports this module as
    # ``src.ml.features`` from the repo root.
    from src.ml.config import (
        FEATURE_APPLICABILITY,
        FEATURE_SCHEMA_VERSION,
        SECTOR_VOCAB,
        SEQUENCE_FEATURE_COLUMNS,
        TABULAR_FEATURE_COLUMNS,
        WINDOW_SIZE,
    )
except ImportError:
    # Sibling import: SageMaker copies the bundled ``code/`` directory flat,
    # so config.py is a plain sibling of features.py there.
    from config import (
        FEATURE_APPLICABILITY,
        FEATURE_SCHEMA_VERSION,
        SECTOR_VOCAB,
        SEQUENCE_FEATURE_COLUMNS,
        TABULAR_FEATURE_COLUMNS,
        WINDOW_SIZE,
    )

_DEFAULT_SECTOR = "non_financial"


def validate_model_metadata(metadata: Mapping) -> None:
    """Reject an incompatible artifact before constructing payloads or tensors."""
    if metadata.get("feature_schema_version") != FEATURE_SCHEMA_VERSION:
        raise ValueError(
            "Active model feature_schema_version mismatch; retrain before inference"
        )
    if (
        metadata.get("sector_vocab") != SECTOR_VOCAB
        or metadata.get("feature_columns")
        != {"sequence": SEQUENCE_FEATURE_COLUMNS, "tabular": TABULAR_FEATURE_COLUMNS}
        or metadata.get("hyperparameters", {}).get("window_size") != WINDOW_SIZE
    ):
        raise ValueError(
            "Active model feature schema/window mismatch; retrain before inference"
        )
    medians = metadata.get("tabular_medians", {})
    if not isinstance(medians, dict) or not all(
        is_valid_number(v) for v in medians.values()
    ):
        raise ValueError("Invalid model tabular medians")


def sector_index(sector: str) -> int:
    """Embedding index for ``sector``; unknown values map to non_financial."""
    try:
        return SECTOR_VOCAB.index(sector)
    except ValueError:
        return SECTOR_VOCAB.index(_DEFAULT_SECTOR)


def applies(col: str, sector: str) -> bool:
    """True unless ``col`` has an explicit applicability set excluding ``sector``."""
    sector = sector if sector in SECTOR_VOCAB else _DEFAULT_SECTOR
    allowed = FEATURE_APPLICABILITY.get(col)
    return allowed is None or sector in allowed


# Back-compat private alias — kept for existing internal call sites.
_applies = applies


def median_lookup(medians: Mapping[str, float], sector: str, col: str) -> float:
    """Sector median, then global median, then 0.0."""
    for key in (f"{sector}::{col}", col):
        if is_valid_number(medians.get(key)):
            return float(medians[key])
    return 0.0


def is_valid_number(value: object) -> bool:
    """Whether a scalar can safely enter a float32 model tensor."""
    if value is None or isinstance(value, (bool, str)) or bool(pd.isna(value)):
        return False
    try:
        number = float(value)
        return math.isfinite(number) and abs(number) <= float(np.finfo(np.float32).max)
    except (TypeError, ValueError, OverflowError):
        return False


def tabular_completeness(row: Mapping, sector: str, columns=None) -> float:
    """Fraction of applicable features observed before imputation."""
    applicable = [c for c in (columns or TABULAR_FEATURE_COLUMNS) if applies(c, sector)]
    return (
        sum(is_valid_number(row.get(c)) for c in applicable) / len(applicable)
        if applicable
        else 1.0
    )


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
        if not is_valid_number(value):
            values[i] = median_lookup(medians, sector, col)
        else:
            values[i] = np.float32(value)
            flags[i] = 1.0
    return values, flags


def sequence_features(
    window_df: pd.DataFrame, columns: list[str] | None = None
) -> np.ndarray:
    """Sequence branch input: require finite observations, then cast to float32.

    ``columns`` defaults to ``SEQUENCE_FEATURE_COLUMNS``; callers slicing on
    non-standard columns (tests) may override it.
    """
    cols = columns or SEQUENCE_FEATURE_COLUMNS
    raw = window_df[cols]
    if not raw.apply(lambda col: col.map(is_valid_number)).to_numpy().all():
        raise ValueError("Invalid sequence: missing, non-finite or overflowing value")
    return raw.to_numpy(dtype=np.float32)


def compute_training_medians(train_df: pd.DataFrame) -> dict[str, float]:
    """Per-sector and global medians over applicable, non-null values.

    Keys: ``"{sector}::{col}"`` and ``"{col}"``. Computed on the training
    split only -- never val/test -- to avoid leakage.
    """
    medians: dict[str, float] = {}
    for col in TABULAR_FEATURE_COLUMNS:
        applicable = train_df[train_df["sector"].map(lambda s, _c=col: _applies(_c, s))]
        global_series = applicable.loc[applicable[col].map(is_valid_number), col]
        if not global_series.empty:
            medians[col] = float(global_series.median())
        for sector, group in applicable.groupby("sector"):
            series = group.loc[group[col].map(is_valid_number), col]
            if not series.empty:
                medians[f"{sector}::{col}"] = float(series.median())
    return medians
