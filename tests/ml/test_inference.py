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
            "TICKER": ["AAA"] * 5 + ["BBB"] * 5,
            "TRADING_DATE": list(pd.date_range("2026-01-01", periods=5)) * 2,
            "SEQ_COL": [10, 20, 30, 40, 50, 100, 200, 300, 400, 500],
            "TAB_COL": [1, 2, 3, 4, 5, 6, 7, 8, 9, 10],
        }
    )

    sequence, tabular = build_latest_window(
        df,
        "AAA",
        window_size=3,
        sequence_columns=["SEQ_COL"],
        tabular_columns=["TAB_COL"],
    )

    assert sequence.tolist() == [[30.0], [40.0], [50.0]]
    assert tabular.tolist() == [5.0]


def test_build_latest_window_raises_when_insufficient_history() -> None:
    from src.ml.inference import build_latest_window

    df = pd.DataFrame(
        {
            "TICKER": ["AAA", "AAA"],
            "TRADING_DATE": pd.date_range("2026-01-01", periods=2),
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


def test_predict_from_payload_returns_float_prediction() -> None:
    from src.ml.inference import predict_from_payload
    from src.ml.model import FusionModel

    model = FusionModel(sequence_input_size=2, tabular_input_size=2)
    model.eval()
    payload = {
        "sequence": [[0.1, 0.2], [0.3, 0.4], [0.5, 0.6]],
        "tabular": [0.7, 0.8],
    }

    result = predict_from_payload(model, payload)

    assert isinstance(result, dict)
    assert isinstance(result["predicted_return"], float)


def test_load_model_from_s3_success() -> None:
    import io
    import tarfile
    import unittest.mock

    from src.ml.inference import load_model_from_s3

    mock_s3_client = unittest.mock.MagicMock()

    buffer = io.BytesIO()
    with tarfile.open(fileobj=buffer, mode="w:gz") as tar:
        content = b"fake state dict"
        info = tarfile.TarInfo(name="model.pt")
        info.size = len(content)
        tar.addfile(info, io.BytesIO(content))
    tarball_bytes = buffer.getvalue()

    def download_file_side_effect(Bucket, Key, Filename):
        with open(Filename, "wb") as f:
            f.write(tarball_bytes)

    mock_s3_client.download_file.side_effect = download_file_side_effect

    fake_state_dict = {}
    with (
        unittest.mock.patch(
            "torch.load", return_value=fake_state_dict
        ) as mock_torch_load,
        unittest.mock.patch("src.ml.model.FusionModel") as mock_fusion_model_class,
    ):
        mock_model = unittest.mock.MagicMock()
        mock_fusion_model_class.return_value = mock_model

        model = load_model_from_s3(mock_s3_client, "my-bucket", "my-key")

        assert model == mock_model
        mock_s3_client.download_file.assert_called_once_with(
            "my-bucket", "my-key", unittest.mock.ANY
        )
        mock_torch_load.assert_called_once_with(
            unittest.mock.ANY, map_location="cpu", weights_only=True
        )
        mock_model.load_state_dict.assert_called_once_with(fake_state_dict)
        mock_model.eval.assert_called_once()


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
