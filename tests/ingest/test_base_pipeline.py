"""Unit tests for the BaseIngestPipeline lifecycle flow."""

import os
from unittest.mock import MagicMock, patch

import pandas as pd
import pytest

from src.ingest.pipeline.base import BaseIngestPipeline


class DummyIngestPipeline(BaseIngestPipeline):
    """Concrete mock pipeline for verification testing."""

    @property
    def table_name(self) -> str:
        return "RAW_DUMMY_TABLE"

    @property
    def source_uri_prefix(self) -> str:
        return "api://dummy/source"

    @property
    def schema_columns(self) -> list[str]:
        return ["symbol", "close"]

    def fetch(self) -> pd.DataFrame:
        return pd.DataFrame({"symbol": ["AAA", "BBB"], "close": [10.5, 12.0]})


def test_base_pipeline_standardize() -> None:
    """Verify columns are filtered to schema, uppercased, and metadata injected."""
    pipeline = DummyIngestPipeline(batch_date="2026-06-18")
    # Simulate API returning extra columns beyond schema
    raw_df = pd.DataFrame(
        {"symbol": ["TCB"], "close": [48.5], "extra_col": ["should_be_dropped"]}
    )

    standardized_df = pipeline.standardize(raw_df)

    # Only schema columns (uppercased) should be kept
    assert "SYMBOL" in standardized_df.columns
    assert "CLOSE" in standardized_df.columns
    # Extra columns from API must be dropped
    assert "EXTRA_COL" not in standardized_df.columns

    # Verify metadata fields
    assert standardized_df["_CONATA_SOURCE"].iloc[0] == "api://dummy/source"
    assert standardized_df["_CONATA_SOURCE_ROW_NUMBER"].iloc[0] == 1
    assert standardized_df["_CONATA_PARTITION_KEY"].iloc[0] == "2026-06-18"
    assert "_CONATA_LOADED_AT" in standardized_df.columns
    assert isinstance(standardized_df["_CONATA_LOADED_AT"].dtype, pd.DatetimeTZDtype)


def test_base_pipeline_standardize_injects_batch_date() -> None:
    """Verify BATCH_DATE column is injected by standardize() matching batch_date."""
    pipeline = DummyIngestPipeline(batch_date="2026-06-18")
    raw_df = pd.DataFrame({"symbol": ["TCB"], "close": [48.5]})

    standardized_df = pipeline.standardize(raw_df)

    assert "BATCH_DATE" in standardized_df.columns
    assert standardized_df["BATCH_DATE"].iloc[0] == "2026-06-18"


def test_base_pipeline_standardize_missing_column() -> None:
    """Verify missing schema columns are filled with None and a warning is logged."""
    pipeline = DummyIngestPipeline(batch_date="2026-06-18")
    # API returns only 'symbol', 'close' is missing
    raw_df = pd.DataFrame({"symbol": ["VNM"]})

    standardized_df = pipeline.standardize(raw_df)

    # Missing 'close' column must be added as None
    assert "CLOSE" in standardized_df.columns
    assert standardized_df["CLOSE"].iloc[0] is None


@patch("src.ingest.pipeline.base.upload_to_s3")
def test_base_pipeline_successful_run(mock_upload: MagicMock) -> None:
    """Verify that run() coordinates standard execution and cleans up temporary file."""
    pipeline = DummyIngestPipeline(
        batch_date="2026-06-18",
        s3_client=MagicMock(),
        bucket_name="my-test-bucket",
    )

    # Spy on serialization to track temp file path
    original_serialize = pipeline.serialize
    temp_files_created = []

    def spy_serialize(df: pd.DataFrame) -> str:
        path = original_serialize(df)
        temp_files_created.append(path)
        return path

    pipeline.serialize = spy_serialize  # type: ignore

    s3_url = pipeline.run()

    # Verify S3 URL formatting
    assert s3_url.startswith(
        "s3://my-test-bucket/raw/RAW_DUMMY_TABLE/batch_date=2026-06-18/"
    )
    assert s3_url.endswith("/RAW_DUMMY_TABLE.parquet")

    # Verify upload was called once
    mock_upload.assert_called_once()

    # Verify temp file cleanup
    assert len(temp_files_created) == 1
    assert not os.path.exists(temp_files_created[0])


@patch("src.ingest.pipeline.base.upload_to_s3")
def test_base_pipeline_cleanup_on_failure(mock_upload: MagicMock) -> None:
    """Verify that temporary file is cleaned up even if S3 upload raises an error."""
    pipeline = DummyIngestPipeline(
        batch_date="2026-06-18",
        s3_client=MagicMock(),
    )

    # Simulate S3 upload failure
    mock_upload.side_effect = RuntimeError("AWS Connection Lost")

    original_serialize = pipeline.serialize
    temp_files_created = []

    def spy_serialize(df: pd.DataFrame) -> str:
        path = original_serialize(df)
        temp_files_created.append(path)
        return path

    pipeline.serialize = spy_serialize  # type: ignore

    # Execution must fail loudly
    with pytest.raises(RuntimeError, match="AWS Connection Lost"):
        pipeline.run()

    # File should still be deleted
    assert len(temp_files_created) == 1
    assert not os.path.exists(temp_files_created[0])
