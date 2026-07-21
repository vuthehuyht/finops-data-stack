"""Tests for src/ml/dataset.py."""

import pandas as pd


def _make_dates_df(start: str, periods: int) -> pd.DataFrame:
    dates = pd.date_range(start=start, periods=periods, freq="D")
    return pd.DataFrame(
        {"TRADING_DATE": dates.strftime("%Y-%m-%d"), "VALUE": range(periods)}
    )


def test_time_based_split_partitions_by_date_cutoffs() -> None:
    from src.ml.dataset import time_based_split

    df = _make_dates_df("2026-01-01", 10)  # 2026-01-01 .. 2026-01-10
    train_df, val_df, test_df = time_based_split(
        df, train_end_date="2026-01-05", val_end_date="2026-01-07"
    )

    assert list(train_df["TRADING_DATE"]) == [
        "2026-01-01",
        "2026-01-02",
        "2026-01-03",
        "2026-01-04",
        "2026-01-05",
    ]
    assert list(val_df["TRADING_DATE"]) == ["2026-01-06", "2026-01-07"]
    assert list(test_df["TRADING_DATE"]) == [
        "2026-01-08",
        "2026-01-09",
        "2026-01-10",
    ]


def test_time_based_split_no_leakage_across_cutoffs() -> None:
    """No training row's date is after the train cutoff (look-ahead bias check)."""
    from src.ml.dataset import time_based_split

    df = _make_dates_df("2026-01-01", 30)
    train_df, val_df, _test_df = time_based_split(
        df, train_end_date="2026-01-15", val_end_date="2026-01-20"
    )

    assert (
        pd.to_datetime(train_df["TRADING_DATE"]) <= pd.Timestamp("2026-01-15")
    ).all()
    assert (pd.to_datetime(val_df["TRADING_DATE"]) > pd.Timestamp("2026-01-15")).all()
    assert (pd.to_datetime(val_df["TRADING_DATE"]) <= pd.Timestamp("2026-01-20")).all()


def test_time_based_split_raises_when_val_before_train() -> None:
    from src.ml.dataset import time_based_split

    df = _make_dates_df("2026-01-01", 10)
    try:
        time_based_split(df, train_end_date="2026-01-05", val_end_date="2026-01-03")
        raise AssertionError("expected ValueError")
    except ValueError:
        pass


def _make_feature_df(tickers: list[str], days: int) -> pd.DataFrame:
    from src.ml.config import SEQUENCE_FEATURE_COLUMNS, TABULAR_FEATURE_COLUMNS

    rows = []
    for ticker in tickers:
        dates = pd.date_range(start="2026-01-01", periods=days, freq="D")
        for i, date in enumerate(dates):
            row = {"TICKER": ticker, "TRADING_DATE": date.strftime("%Y-%m-%d")}
            for col in SEQUENCE_FEATURE_COLUMNS:
                row[col] = float(i)
            for col in TABULAR_FEATURE_COLUMNS:
                row[col] = float(i) * 0.1
            row["label_next_5d_return"] = 0.01 * i
            rows.append(row)
    return pd.DataFrame(rows)


def test_stock_sequence_dataset_length_per_ticker() -> None:
    from src.ml.dataset import StockSequenceDataset

    df = _make_feature_df(["AAA", "BBB"], days=35)
    dataset = StockSequenceDataset(df, window_size=30)

    # Each ticker with 35 rows yields (35 - 30 + 1) = 6 windows -> 12 total.
    assert len(dataset) == 12


def test_stock_sequence_dataset_skips_tickers_shorter_than_window() -> None:
    from src.ml.dataset import StockSequenceDataset

    df = _make_feature_df(["AAA"], days=10)
    dataset = StockSequenceDataset(df, window_size=30)

    assert len(dataset) == 0


def test_stock_sequence_dataset_getitem_shapes() -> None:
    from src.ml.config import SEQUENCE_FEATURE_COLUMNS, TABULAR_FEATURE_COLUMNS
    from src.ml.dataset import StockSequenceDataset

    df = _make_feature_df(["AAA"], days=30)
    dataset = StockSequenceDataset(df, window_size=30)

    sequence, tabular, target = dataset[0]
    assert sequence.shape == (30, len(SEQUENCE_FEATURE_COLUMNS))
    assert tabular.shape == (len(TABULAR_FEATURE_COLUMNS),)
    assert target.shape == ()


def test_stock_sequence_dataset_getitem_values() -> None:
    """Verify that __getitem__ returns correct row values, not just shapes.

    Specifically:
    - Tabular tensor: last row's tabular columns (not first row or some other).
    - Sequence tensor: preserves chronological order (first day < last day).
    - Target value: matches the last row's label.
    """
    import numpy as np

    from src.ml.dataset import StockSequenceDataset

    df = _make_feature_df(["AAA"], days=30)
    dataset = StockSequenceDataset(df, window_size=30)

    sequence, tabular, target = dataset[0]

    # Verify the last row (day 29 in the fixture, where fixture sets float(i)):
    # SEQUENCE_FEATURE_COLUMNS all equal 29.0 for the last day.
    assert np.allclose(sequence[-1].numpy(), 29.0)

    # TABULAR_FEATURE_COLUMNS from last row: 29 * 0.1 = 2.9.
    assert np.allclose(tabular.numpy(), 2.9)

    # label_next_5d_return from last row: 29 * 0.01 = 0.29.
    assert np.isclose(float(target), 0.29)

    # Verify chronological order: first row (day 0) has values 0.0, last row has 29.0.
    assert np.allclose(sequence[0].numpy(), 0.0)
    assert (sequence[0] < sequence[-1]).all()


def test_stock_sequence_dataset_drops_rows_with_null_target() -> None:
    from src.ml.dataset import StockSequenceDataset

    df = _make_feature_df(["AAA"], days=31)
    df.loc[df.index[-1], "label_next_5d_return"] = None

    dataset = StockSequenceDataset(df, window_size=30)

    # 31 rows -> 30 valid after dropping the last null-target row -> 1 window.
    assert len(dataset) == 1
