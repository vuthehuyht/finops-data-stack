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


def test_load_s3_to_redshift_success() -> None:
    """Test successful load process (full SQL query sequence check)."""
    mock_cursor = unittest.mock.Mock()
    mock_cursor.query = b""

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

    assert "BEGIN;" in calls
    assert any("CREATE TEMPORARY TABLE temp_my_table" in q for q in calls)
    assert any("COPY temp_my_table" in q for q in calls)
    assert any(
        "INSERT INTO my_schema.my_table SELECT * FROM temp_my_table" in q for q in calls
    )
    assert "COMMIT;" in calls


def test_load_s3_to_redshift_failure() -> None:
    """Test rollback behaviour when the COPY or CREATE table command fails."""
    mock_cursor = unittest.mock.Mock()
    mock_cursor.query = b""

    # Mock failure on COPY command
    mock_cursor.execute.side_effect = [
        None,  # BEGIN;
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
    assert "BEGIN;" in calls
    assert "ROLLBACK;" in calls


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
