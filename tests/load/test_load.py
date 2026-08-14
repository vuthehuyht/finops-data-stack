"""Unit tests for load.py."""

import unittest.mock

import pytest

from src.load import load


def test_is_valid_identifier() -> None:
    """Test database identifier validation."""
    assert load._is_valid_identifier("my_table") is True
    assert load._is_valid_identifier("my_table_123") is True
    assert load._is_valid_identifier("my$table") is True
    assert load._is_valid_identifier("123table") is False
    assert load._is_valid_identifier("table; drop table x;") is False
    assert load._is_valid_identifier("") is False


def test_build_copy_query() -> None:
    """Test dynamic SQL COPY query generation for different formats."""
    # Parquet
    query = load._build_copy_query(
        "temp_t", "s3://bucket/path", "parquet", "arn:aws:iam::role"
    )
    assert "COPY temp_t" in query
    assert "FROM 's3://bucket/path'" in query
    assert "IAM_ROLE 'arn:aws:iam::role'" in query
    assert "FORMAT AS PARQUET" in query

    # JSON
    query_json = load._build_copy_query(
        "temp_t", "s3://bucket/path", "json", "arn:aws:iam::role"
    )
    assert "FORMAT AS JSON 'auto'" in query_json

    # CSV
    query_csv = load._build_copy_query(
        "temp_t", "s3://bucket/path", "csv", "arn:aws:iam::role"
    )
    assert "CSV IGNOREHEADER 1" in query_csv

    # Invalid S3 paths should raise ValueError
    with pytest.raises(ValueError, match="Invalid S3 path"):
        load._build_copy_query("temp_t", "http://bucket/path", "parquet", "arn")

    # Unsupported file formats should raise ValueError
    with pytest.raises(ValueError, match="Unsupported file format"):
        load._build_copy_query("temp_t", "s3://bucket/path", "unknown", "arn")


@unittest.mock.patch("pyarrow.parquet.ParquetFile")
@unittest.mock.patch("boto3.client")
def test_load_s3_to_redshift_success(
    mock_boto_client: unittest.mock.Mock, mock_parquet_file: unittest.mock.Mock
) -> None:
    """Test successful load process (full SQL query sequence check)."""
    mock_s3 = unittest.mock.Mock()
    mock_boto_client.return_value = mock_s3
    mock_s3.list_objects_v2.return_value = {"Contents": [{"Key": "test.parquet"}]}
    mock_s3.get_object.return_value = {"Body": unittest.mock.Mock(read=lambda: b"")}

    mock_pq_instance = unittest.mock.Mock()
    mock_pq_instance.schema.names = ["TICKER"]
    mock_parquet_file.return_value = mock_pq_instance
    mock_cursor = unittest.mock.Mock()
    # First call to execute is for fetching columns
    mock_cursor.fetchall.return_value = [
        ("TICKER",),
        ("BATCH_DATE",),
        ("_CONATA_SOURCE",),
    ]
    mock_cursor.execute.side_effect = [
        None,  # SELECT column_name FROM information_schema.columns
        None,  # CREATE TEMPORARY TABLE...
        None,  # COPY
        None,  # TRUNCATE
        None,  # INSERT
    ]

    load.load_s3_to_redshift(
        cursor=mock_cursor,
        s3_url="s3://bucket/path",
        table_name="my_table",
        schema="my_schema",
        file_format="parquet",
        iam_role_arn="arn:aws:iam::role",
    )

    # Get executed SQL statements
    calls = [call[0][0] for call in mock_cursor.execute.call_args_list]

    assert any("CREATE TEMPORARY TABLE temp_my_table" in q for q in calls)
    assert any("COPY temp_my_table" in q for q in calls)
    assert any(
        'INSERT INTO "my_schema"."my_table" SELECT "TICKER", NULL AS BATCH_DATE, '
        "'s3://bucket/path' AS _CONATA_SOURCE FROM" in q
        for q in calls
    )


@unittest.mock.patch("pyarrow.parquet.ParquetFile")
@unittest.mock.patch("boto3.client")
def test_load_s3_to_redshift_failure(
    mock_boto_client: unittest.mock.Mock, mock_parquet_file: unittest.mock.Mock
) -> None:
    """Test rollback behaviour when the COPY or CREATE table command fails."""
    mock_s3 = unittest.mock.Mock()
    mock_boto_client.return_value = mock_s3
    mock_s3.list_objects_v2.return_value = {"Contents": [{"Key": "test.parquet"}]}
    mock_s3.get_object.return_value = {"Body": unittest.mock.Mock(read=lambda: b"")}

    mock_pq_instance = unittest.mock.Mock()
    mock_pq_instance.schema.names = ["TICKER"]
    mock_parquet_file.return_value = mock_pq_instance
    mock_cursor = unittest.mock.Mock()
    mock_cursor.query = b""

    # First call to execute is for fetching columns
    mock_cursor.fetchall.return_value = [
        ("TICKER",),
        ("BATCH_DATE",),
        ("_CONATA_SOURCE",),
    ]

    # Mock failure on COPY command
    mock_cursor.execute.side_effect = [
        None,  # SELECT columns
        None,  # CREATE TEMPORARY TABLE...
        ValueError("COPY statement failed"),  # COPY
    ]

    with pytest.raises(ValueError, match="COPY statement failed"):
        load.load_s3_to_redshift(
            cursor=mock_cursor,
            s3_url="s3://bucket/path",
            table_name="my_table",
            schema="my_schema",
            file_format="parquet",
            iam_role_arn="arn:aws:iam::role",
        )

    calls = [call[0][0] for call in mock_cursor.execute.call_args_list]
    assert any("CREATE TEMPORARY TABLE temp_my_table" in q for q in calls)


def test_load_s3_to_redshift_invalid_identifiers() -> None:
    """Verify that ValueError is raised early for invalid database identifiers."""
    mock_cursor = unittest.mock.Mock()

    with pytest.raises(ValueError, match="Invalid table name"):
        load.load_s3_to_redshift(
            cursor=mock_cursor,
            s3_url="s3://bucket/path",
            table_name="drop table x;",
            schema="my_schema",
            file_format="parquet",
            iam_role_arn="arn:aws:iam::role",
        )

    with pytest.raises(ValueError, match="Invalid schema name"):
        load.load_s3_to_redshift(
            cursor=mock_cursor,
            s3_url="s3://bucket/path",
            table_name="my_table",
            schema="sys; --",
            file_format="parquet",
            iam_role_arn="arn:aws:iam::role",
        )
