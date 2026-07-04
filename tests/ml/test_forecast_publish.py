"""Tests for src/ml/forecast_publish.py."""

import unittest.mock

import pytest


def test_publish_forecast_results_runs_copy_then_delete_then_insert() -> None:
    from src.ml.forecast_publish import publish_forecast_results

    mock_cursor = unittest.mock.MagicMock()

    publish_forecast_results(
        cursor=mock_cursor,
        s3_url="s3://bucket/ml-inference-output/2026-07-03/input.jsonl.out",
        iam_role_arn="arn:aws:iam::role",
        trading_date="2026-07-06",
        model_version="v1",
    )

    calls = mock_cursor.execute.call_args_list
    queries = [call.args[0] for call in calls]

    assert any("CREATE TEMPORARY TABLE" in q for q in queries)
    assert any(
        "COPY" in q
        and "s3://bucket/ml-inference-output/2026-07-03/input.jsonl.out" in q
        for q in queries
    )
    assert any("DELETE FROM MART.FCT_ML_FORECAST_RESULTS" in q for q in queries)
    delete_call = next(c for c in calls if "DELETE FROM" in c.args[0])
    assert delete_call.args[1] == ("2026-07-06",)

    insert_call = next(
        c for c in calls if "INSERT INTO MART.FCT_ML_FORECAST_RESULTS" in c.args[0]
    )
    assert insert_call.args[1] == (
        "2026-07-06",
        "v1",
        "publish_forecast_results",
        "publish_forecast_results",
        "2026-07-06",
    )


def test_publish_forecast_results_rejects_invalid_s3_url() -> None:
    from src.ml.forecast_publish import publish_forecast_results

    mock_cursor = unittest.mock.MagicMock()

    with pytest.raises(ValueError, match="Invalid S3 path"):
        publish_forecast_results(
            cursor=mock_cursor,
            s3_url="http://bucket/path",
            iam_role_arn="arn:aws:iam::role",
            trading_date="2026-07-06",
            model_version="v1",
        )


def test_publish_forecast_results_rollback_on_failure() -> None:
    from src.ml.forecast_publish import publish_forecast_results

    mock_cursor = unittest.mock.MagicMock()
    # Simulate failure during the COPY statement (3rd execute call)
    copy_error = RuntimeError("COPY failed: network error")
    mock_cursor.execute.side_effect = [None, None, copy_error]

    with pytest.raises(RuntimeError, match="COPY failed: network error"):
        publish_forecast_results(
            cursor=mock_cursor,
            s3_url="s3://bucket/ml-inference-output/2026-07-03/input.jsonl.out",
            iam_role_arn="arn:aws:iam::role",
            trading_date="2026-07-06",
            model_version="v1",
        )

    # Verify that ROLLBACK was called
    calls = mock_cursor.execute.call_args_list
    rollback_called = any("ROLLBACK" in str(call) for call in calls)
    assert rollback_called, "ROLLBACK should be called when an error occurs"
