"""Tests for src/ml/data_export.py."""

from unittest.mock import MagicMock


def test_build_unload_query_contains_source_table_and_destination() -> None:
    from src.ml.data_export import build_unload_query

    query = build_unload_query(
        s3_url="s3://bucket/ml-training-data/2026-07-03/",
        iam_role_arn="arn:aws:iam::123456789012:role/redshift-role",
    )

    assert "MART.FACT_ML_FEATURE_SET" in query
    assert "s3://bucket/ml-training-data/2026-07-03/" in query
    assert "arn:aws:iam::123456789012:role/redshift-role" in query
    assert "FORMAT AS PARQUET" in query
    assert "DATEADD(month, -24, CURRENT_DATE)" in query


def test_build_unload_query_respects_lookback_months() -> None:
    from src.ml.data_export import build_unload_query

    query = build_unload_query(
        s3_url="s3://bucket/prefix/",
        iam_role_arn="arn:aws:iam::123456789012:role/redshift-role",
        lookback_months=12,
    )

    assert "DATEADD(month, -12, CURRENT_DATE)" in query


def test_build_unload_query_rejects_non_s3_url() -> None:
    from src.ml.data_export import build_unload_query

    try:
        build_unload_query(s3_url="not-s3", iam_role_arn="arn:aws:iam::123:role/r")
        raise AssertionError("expected ValueError")
    except ValueError:
        pass


def test_unload_training_dataset_returns_row_count() -> None:
    from src.ml.data_export import unload_training_dataset

    cursor = MagicMock()
    cursor.fetchone.return_value = (1234,)

    row_count = unload_training_dataset(
        cursor=cursor,
        s3_url="s3://bucket/prefix/",
        iam_role_arn="arn:aws:iam::123456789012:role/redshift-role",
    )

    assert row_count == 1234
    assert cursor.execute.call_count == 2  # UNLOAD + COUNT
    # Verify both UNLOAD and COUNT queries use the same date-filter clause.
    all_calls_sql = [str(call) for call in cursor.execute.call_args_list]
    assert all("DATEADD(month, -24, CURRENT_DATE)" in sql for sql in all_calls_sql), (
        f"Both queries must use the same date filter; got {all_calls_sql}"
    )


def test_unload_training_dataset_raises_on_zero_rows() -> None:
    from src.ml.data_export import unload_training_dataset

    cursor = MagicMock()
    cursor.fetchone.return_value = (0,)

    try:
        unload_training_dataset(
            cursor=cursor,
            s3_url="s3://bucket/prefix/",
            iam_role_arn="arn:aws:iam::123456789012:role/redshift-role",
        )
        raise AssertionError("expected RuntimeError")
    except RuntimeError:
        pass
