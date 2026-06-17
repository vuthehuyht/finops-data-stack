"""
Utility handling data saving and uploading to AWS S3 Bronze Data Lake.
"""

import logging
import os
import tempfile

import pandas as pd

from src.common.s3 import upload_file_to_s3

logger = logging.getLogger(__name__)


def save_and_upload_df(
    df: pd.DataFrame,
    s3_bucket: str,
    s3_key_prefix: str,
    file_name: str,
    file_format: str = "parquet",
) -> str:
    """Save a DataFrame locally as a temporary file and upload it to AWS S3.

    Args:
        df (pd.DataFrame): Data to upload.
        s3_bucket (str): Target S3 bucket name.
        s3_key_prefix (str): Destination folder key prefix.
        file_name (str): Filename without format extension.
        file_format (str): Destination file format ('parquet' or 'csv').

    Returns:
        str: S3 URI of the uploaded file.
    """
    ext = file_format.lower()
    full_file_name = f"{file_name}.{ext}"
    s3_key = os.path.join(s3_key_prefix, full_file_name).replace("\\", "/")

    # Create temporary local file
    with tempfile.NamedTemporaryFile(suffix=f".{ext}", delete=False) as temp_file:
        temp_path = temp_file.name

    try:
        # Save DataFrame locally
        if ext == "parquet":
            df.to_parquet(temp_path, index=False)
        else:
            df.to_csv(temp_path, index=False, encoding="utf-8")

        # Upload file to S3
        success = upload_file_to_s3(temp_path, s3_bucket, s3_key)
        if success:
            s3_uri = f"s3://{s3_bucket}/{s3_key}"
            logger.info(f"Successfully uploaded dataset to S3 URI: {s3_uri}")
            return s3_uri
        else:
            raise RuntimeError(
                f"Failed to upload temporary file to S3 path s3://{s3_bucket}/{s3_key}"
            )

    finally:
        # Clean temporary file
        if os.path.exists(temp_path):
            os.remove(temp_path)
            logger.debug(f"Removed temporary local file at: {temp_path}")
