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
    assert TARGET_COLUMN == "label_next_5d_return"


def test_window_size_default() -> None:
    from src.ml.config import WINDOW_SIZE

    assert WINDOW_SIZE == 30


def test_sector_and_feature_constants() -> None:
    from src.ml import config

    assert config.SECTOR_VOCAB[-1] == "non_financial"
    assert len(config.SECTOR_VOCAB) == len(set(config.SECTOR_VOCAB))
    assert config.TABULAR_VECTOR_SIZE == 2 * len(config.TABULAR_FEATURE_COLUMNS)
    assert config.FEATURE_SCHEMA_VERSION == 2
    # every applicability key must be a real tabular feature
    assert set(config.FEATURE_APPLICABILITY) <= set(config.TABULAR_FEATURE_COLUMNS)
    # every listed sector must be in the vocab
    for sectors in config.FEATURE_APPLICABILITY.values():
        assert sectors <= set(config.SECTOR_VOCAB)
    # the two known structural exceptions
    assert config.FEATURE_APPLICABILITY["gross_margin"] == {
        "real_estate",
        "non_financial",
    }
    assert config.FEATURE_APPLICABILITY["debt_to_equity"] == {
        "real_estate",
        "non_financial",
    }


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
