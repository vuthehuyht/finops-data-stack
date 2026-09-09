"""Tests for src/ml/features.py — the shared train/serve featurization module."""

import numpy as np
import pandas as pd
import pytest

from src.ml import features
from src.ml.config import (
    SECTOR_VOCAB,
    SEQUENCE_FEATURE_COLUMNS,
    TABULAR_FEATURE_COLUMNS,
)


def _row(**overrides):
    base = dict.fromkeys(TABULAR_FEATURE_COLUMNS, 1.0)
    base.update(overrides)
    return pd.Series(base)


def test_sector_index_known_and_unknown():
    assert features.sector_index("bank") == SECTOR_VOCAB.index("bank")
    assert features.sector_index("nonsense") == SECTOR_VOCAB.index("non_financial")


def test_applicable_present_value_gets_flag_one():
    values, flags = features.build_tabular_features(_row(roe=0.15), "bank", {})
    i = TABULAR_FEATURE_COLUMNS.index("roe")
    assert values[i] == pytest.approx(0.15)
    assert flags[i] == 1.0
    assert values.dtype == np.float32 and flags.dtype == np.float32


def test_not_applicable_feature_zeroed_and_flag_zero():
    # gross_margin is not defined for banks
    values, flags = features.build_tabular_features(_row(gross_margin=0.42), "bank", {})
    i = TABULAR_FEATURE_COLUMNS.index("gross_margin")
    assert values[i] == 0.0
    assert flags[i] == 0.0


def test_applicable_but_nan_uses_sector_median_and_flag_zero():
    medians = {"non_financial::roe": 0.11, "roe": 0.09}
    values, flags = features.build_tabular_features(
        _row(roe=float("nan")), "non_financial", medians
    )
    i = TABULAR_FEATURE_COLUMNS.index("roe")
    assert values[i] == pytest.approx(0.11)
    assert flags[i] == 0.0


def test_median_lookup_fallback_chain():
    assert features.median_lookup({"bank::roe": 1.0, "roe": 2.0}, "bank", "roe") == 1.0
    assert features.median_lookup({"roe": 2.0}, "bank", "roe") == 2.0
    assert features.median_lookup({}, "bank", "roe") == 0.0


def test_sequence_features_fills_nan_with_zero():
    window = pd.DataFrame(
        {c: [1.0, float("nan"), 3.0] for c in SEQUENCE_FEATURE_COLUMNS}
    )
    arr = features.sequence_features(window)
    assert arr.shape == (3, len(SEQUENCE_FEATURE_COLUMNS))
    assert arr.dtype == np.float32
    assert not np.isnan(arr).any()
    assert arr[1, 0] == 0.0


def test_compute_training_medians_only_applicable_nonnull():
    df = pd.DataFrame(
        {
            "sector": ["bank", "bank", "non_financial"],
            "gross_margin": [0.5, 0.7, 0.3],  # not applicable to banks
            "roe": [0.10, float("nan"), 0.20],
        }
    )
    # give every other tabular column a value so median computation is defined
    for c in TABULAR_FEATURE_COLUMNS:
        if c not in df.columns:
            df[c] = 1.0
    medians = features.compute_training_medians(df)
    # banks excluded from gross_margin -> only the single non_financial row
    assert medians["non_financial::gross_margin"] == pytest.approx(0.3)
    assert "bank::gross_margin" not in medians
    # roe global median over non-null applicable values {0.10, 0.20}
    assert medians["roe"] == pytest.approx(0.15)
    assert medians["bank::roe"] == pytest.approx(0.10)


def test_train_serve_parity():
    """The same row through a pandas Series and through a plain dict must
    produce an identical vector."""
    medians = {"non_financial::roe": 0.11}
    row_series = _row(roe=float("nan"), gross_margin=0.4)
    row_dict = dict(row_series)
    v1, f1 = features.build_tabular_features(row_series, "non_financial", medians)
    v2, f2 = features.build_tabular_features(row_dict, "non_financial", medians)
    assert np.array_equal(v1, v2)
    assert np.array_equal(f1, f2)
