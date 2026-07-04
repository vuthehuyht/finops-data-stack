"""Tests for src/ml/config.py."""


def test_sequence_and_tabular_columns_do_not_overlap() -> None:
    from src.ml.config import SEQUENCE_FEATURE_COLUMNS, TABULAR_FEATURE_COLUMNS

    overlap = set(SEQUENCE_FEATURE_COLUMNS) & set(TABULAR_FEATURE_COLUMNS)
    assert overlap == set()


def test_feature_columns_are_non_empty() -> None:
    from src.ml.config import SEQUENCE_FEATURE_COLUMNS, TABULAR_FEATURE_COLUMNS

    assert len(SEQUENCE_FEATURE_COLUMNS) > 0
    assert len(TABULAR_FEATURE_COLUMNS) > 0


def test_target_column_not_in_feature_columns() -> None:
    from src.ml.config import (
        SEQUENCE_FEATURE_COLUMNS,
        TABULAR_FEATURE_COLUMNS,
        TARGET_COLUMN,
    )

    assert TARGET_COLUMN not in SEQUENCE_FEATURE_COLUMNS
    assert TARGET_COLUMN not in TABULAR_FEATURE_COLUMNS
    assert TARGET_COLUMN == "LABEL_NEXT_5D_RETURN"


def test_window_size_default() -> None:
    from src.ml.config import WINDOW_SIZE

    assert WINDOW_SIZE == 30


def test_model_dimension_constants() -> None:
    from src.ml.config import (
        DROPOUT_RATE,
        FUSION_HIDDEN_SIZE,
        LSTM_HIDDEN_SIZE,
        LSTM_NUM_LAYERS,
        MLP_HIDDEN_SIZES,
    )

    assert LSTM_HIDDEN_SIZE > 0
    assert LSTM_NUM_LAYERS > 0
    assert len(MLP_HIDDEN_SIZES) >= 1
    assert FUSION_HIDDEN_SIZE > 0
    assert 0.0 <= DROPOUT_RATE < 1.0
