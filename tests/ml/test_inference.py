"""Tests for src/ml/inference.py."""

import pandas as pd
import pytest


def test_check_feature_null_rate_returns_rates_when_within_threshold() -> None:
    from src.ml.inference import check_feature_null_rate

    df = pd.DataFrame({"A": [1.0, None, 3.0, 4.0], "B": [1.0, 2.0, 3.0, 4.0]})

    rates = check_feature_null_rate(df, ["A", "B"], threshold=0.5)

    assert rates == {"A": 0.25, "B": 0.0}


def test_check_feature_null_rate_raises_when_column_exceeds_threshold() -> None:
    from src.ml.inference import check_feature_null_rate

    df = pd.DataFrame({"A": [None, None, None, 4.0], "B": [1.0, 2.0, 3.0, 4.0]})

    with pytest.raises(ValueError, match="A=75.00%"):
        check_feature_null_rate(df, ["A", "B"], threshold=0.2)


def test_check_feature_null_rate_raises_on_empty_dataframe() -> None:
    from src.ml.inference import check_feature_null_rate

    df = pd.DataFrame({"A": [], "B": []})

    with pytest.raises(ValueError, match="empty"):
        check_feature_null_rate(df, ["A", "B"], threshold=0.2)


def test_build_latest_window_returns_last_window_size_rows() -> None:
    from src.ml.inference import build_latest_window

    df = pd.DataFrame(
        {
            "ticker": ["AAA"] * 5 + ["BBB"] * 5,
            "trading_date": list(pd.date_range("2026-01-01", periods=5)) * 2,
            "sector": ["bank"] * 5 + ["non_financial"] * 5,
            "SEQ_COL": [10, 20, 30, 40, 50, 100, 200, 300, 400, 500],
            "TAB_COL": [1, 2, 3, 4, 5, 6, 7, 8, 9, 10],
        }
    )

    sequence, tabular_row, sector = build_latest_window(
        df,
        "AAA",
        window_size=3,
        sequence_columns=["SEQ_COL"],
        tabular_columns=["TAB_COL"],
    )

    assert sequence.tolist() == [[30.0], [40.0], [50.0]]
    assert float(tabular_row["TAB_COL"]) == 5.0
    assert sector == "bank"


def test_build_latest_window_raises_when_insufficient_history() -> None:
    from src.ml.inference import build_latest_window

    df = pd.DataFrame(
        {
            "ticker": ["AAA", "AAA"],
            "trading_date": pd.date_range("2026-01-01", periods=2),
            "sector": ["bank", "bank"],
            "SEQ_COL": [10, 20],
            "TAB_COL": [1, 2],
        }
    )

    with pytest.raises(ValueError, match="need >= 5"):
        build_latest_window(
            df,
            "AAA",
            window_size=5,
            sequence_columns=["SEQ_COL"],
            tabular_columns=["TAB_COL"],
        )


def _payload(sector: str = "non_financial") -> dict:
    from src.ml.config import SEQUENCE_FEATURE_COLUMNS, TABULAR_FEATURE_COLUMNS

    return {
        "ticker": "ACB",
        "sequence": [[0.0] * len(SEQUENCE_FEATURE_COLUMNS) for _ in range(30)],
        "tabular": dict.fromkeys(TABULAR_FEATURE_COLUMNS, 1.0),
        "sector": sector,
    }


def test_predict_from_payload_returns_float_when_finite() -> None:
    import torch

    from src.ml import inference

    class _ConstModel(torch.nn.Module):
        def forward(self, sequence, tabular, sector_idx):
            return torch.full((sequence.shape[0], 1), 0.0123)

    out = inference.predict_from_payload(
        (_ConstModel(), {}, ["non_financial"]), _payload()
    )
    assert out["predicted_return"] == pytest.approx(0.0123)
    import json

    assert "NaN" not in json.dumps({"ticker": "HPG", **out})


def test_predict_from_payload_none_on_non_finite() -> None:
    import torch

    from src.ml import inference

    class _NanModel(torch.nn.Module):
        def forward(self, sequence, tabular, sector_idx):
            return torch.full((sequence.shape[0], 1), float("nan"))

    out = inference.predict_from_payload((_NanModel(), {}, ["bank"]), _payload("bank"))
    assert out == {"predicted_return": None}


def _gate_df(rows):
    from src.ml.config import SEQUENCE_FEATURE_COLUMNS, TABULAR_FEATURE_COLUMNS

    base_cols = SEQUENCE_FEATURE_COLUMNS + TABULAR_FEATURE_COLUMNS
    records = []
    for ticker, sector, overrides in rows:
        rec = dict.fromkeys(base_cols, 1.0)
        rec.update(overrides)
        rec["ticker"] = ticker
        rec["sector"] = sector
        records.append(rec)
    return pd.DataFrame(records)


def test_gate_passes_when_bank_missing_only_structural_features() -> None:
    from src.ml.inference import check_sector_aware_completeness

    df = _gate_df(
        [
            ("ACB", "bank", {"gross_margin": None, "debt_to_equity": None}),
            ("HPG", "non_financial", {}),
        ]
    )
    out = check_sector_aware_completeness(
        df,
        null_rate_threshold=0.6,
        min_ticker_completeness=0.7,
        max_incomplete_ticker_ratio=0.3,
    )
    assert out["incomplete_ticker_ratio"] == 0.0
    assert set(out["sector_breakdown"]) == {"bank", "non_financial"}


def test_gate_fails_on_dirty_sequence_feature() -> None:
    from src.ml.config import SEQUENCE_FEATURE_COLUMNS
    from src.ml.inference import check_sector_aware_completeness

    df = _gate_df(
        [
            ("ACB", "bank", {SEQUENCE_FEATURE_COLUMNS[0]: None}),
            ("BID", "bank", {SEQUENCE_FEATURE_COLUMNS[0]: None}),
        ]
    )
    with pytest.raises(ValueError, match="sequence feature"):
        check_sector_aware_completeness(
            df,
            null_rate_threshold=0.6,
            min_ticker_completeness=0.7,
            max_incomplete_ticker_ratio=0.3,
        )


def test_gate_fails_when_too_many_tickers_incomplete() -> None:
    from src.ml.config import TABULAR_FEATURE_COLUMNS
    from src.ml.inference import check_sector_aware_completeness

    half = dict.fromkeys(
        TABULAR_FEATURE_COLUMNS[: len(TABULAR_FEATURE_COLUMNS) // 2 + 2]
    )
    df = _gate_df([("ACB", "non_financial", half)])
    with pytest.raises(ValueError, match="incomplete"):
        check_sector_aware_completeness(
            df,
            null_rate_threshold=0.6,
            min_ticker_completeness=0.7,
            max_incomplete_ticker_ratio=0.3,
        )


def test_next_trading_day_skips_to_next_weekday() -> None:
    import datetime

    from src.ml.inference import next_trading_day

    # Tuesday -> Wednesday
    assert next_trading_day(datetime.date(2026, 7, 7)) == datetime.date(2026, 7, 8)


def test_next_trading_day_skips_weekend_after_friday() -> None:
    import datetime

    from src.ml.inference import next_trading_day

    # Friday -> Monday (skips Sat/Sun)
    assert next_trading_day(datetime.date(2026, 7, 3)) == datetime.date(2026, 7, 6)


def test_next_trading_day_from_saturday_skips_to_monday() -> None:
    import datetime

    from src.ml.inference import next_trading_day

    # Saturday input (shouldn't occur given the ingest cron, but must not crash)
    assert next_trading_day(datetime.date(2026, 7, 4)) == datetime.date(2026, 7, 6)
