"""
S3 utilities for FinOps Data Stack (S3 connector, uploads, downloads).
"""

import logging
import os

import boto3
from botocore.exceptions import ClientError

logger = logging.getLogger(__name__)


def get_s3_client():
    """Create and return an S3 client using environment credentials."""
    aws_key = os.getenv("AWS_ACCESS_KEY_ID")
    aws_secret = os.getenv("AWS_SECRET_ACCESS_KEY")
    region = os.getenv("AWS_DEFAULT_REGION", "ap-southeast-1")

    if aws_key and aws_secret:
        return boto3.client(
            "s3",
            aws_access_key_id=aws_key,
            aws_secret_access_key=aws_secret,
            region_name=region,
        )
    return boto3.client("s3", region_name=region)


def upload_file_to_s3(local_file_path: str, bucket_name: str, s3_key: str) -> bool:
    """Upload a local file to a specified S3 bucket.

    Args:
        local_file_path (str): Path to local file.
        bucket_name (str): S3 bucket name.
        s3_key (str): S3 destination key.

    Returns:
        bool: True if successful, False otherwise.
    """
    s3_client = get_s3_client()
    try:
        logger.info(f"Uploading {local_file_path} to s3://{bucket_name}/{s3_key}")
        s3_client.upload_file(local_file_path, bucket_name, s3_key)
        return True
    except ClientError as e:
        logger.error(f"Failed to upload to S3: {e}")
        return False
    except Exception as e:
        logger.error(f"Unexpected error uploading to S3: {e}")
        return False


def download_file_from_s3(bucket_name: str, s3_key: str, local_file_path: str) -> bool:
    """Download a file from an S3 bucket to local directory.

    Args:
        bucket_name (str): S3 bucket name.
        s3_key (str): S3 source key.
        local_file_path (str): Destination path on local system.

    Returns:
        bool: True if successful, False otherwise.
    """
    s3_client = get_s3_client()
    try:
        logger.info(f"Downloading s3://{bucket_name}/{s3_key} to {local_file_path}")
        os.makedirs(os.path.dirname(local_file_path), exist_ok=True)
        s3_client.download_file(bucket_name, s3_key, local_file_path)
        return True
    except ClientError as e:
        logger.error(f"Failed to download from S3: {e}")
        return False
    except Exception as e:
        logger.error(f"Unexpected error downloading from S3: {e}")
        return False
