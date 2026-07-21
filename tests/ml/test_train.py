"""Tests for src/ml/train.py."""

import pandas as pd
import torch


def test_compute_regression_metrics_zero_error() -> None:
    from src.ml.train import compute_regression_metrics

    predictions = torch.tensor([1.0, 2.0, 3.0])
    targets = torch.tensor([1.0, 2.0, 3.0])

    metrics = compute_regression_metrics(predictions, targets)

    assert metrics.rmse == 0.0
    assert metrics.mae == 0.0


def test_compute_regression_metrics_known_error() -> None:
    from src.ml.train import compute_regression_metrics

    predictions = torch.tensor([0.0, 0.0])
    targets = torch.tensor([3.0, 4.0])

    metrics = compute_regression_metrics(predictions, targets)

    # errors = [-3, -4]; MAE = 3.5; RMSE = sqrt((9+16)/2) = sqrt(12.5)
    assert metrics.mae == 3.5
    assert abs(metrics.rmse - 12.5**0.5) < 1e-6


def test_load_training_dataframe_concatenates_parquet_parts(tmp_path) -> None:
    from src.ml.train import _load_training_dataframe

    df_a = pd.DataFrame({"TICKER": ["AAA"], "TRADING_DATE": ["2026-01-01"]})
    df_b = pd.DataFrame({"TICKER": ["BBB"], "TRADING_DATE": ["2026-01-02"]})
    df_a.to_parquet(tmp_path / "part-0.parquet")
    df_b.to_parquet(tmp_path / "part-1.parquet")

    result = _load_training_dataframe(str(tmp_path))

    assert len(result) == 2
    assert set(result["TICKER"]) == {"AAA", "BBB"}


def test_load_training_dataframe_raises_when_empty(tmp_path) -> None:
    from src.ml.train import _load_training_dataframe

    try:
        _load_training_dataframe(str(tmp_path))
        raise AssertionError("expected FileNotFoundError")
    except FileNotFoundError:
        pass


def test_bundle_serving_code_copies_serve_and_dependencies(tmp_path) -> None:
    from src.ml.train import _bundle_serving_code

    _bundle_serving_code(str(tmp_path))

    code_dir = tmp_path / "code"
    assert (code_dir / "serve.py").is_file()
    assert (code_dir / "inference.py").is_file()
    assert (code_dir / "model.py").is_file()
    assert (code_dir / "config.py").is_file()
