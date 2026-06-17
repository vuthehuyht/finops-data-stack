"""Dagster job for moving input file."""

import datetime
import logging
import os
import posixpath
import urllib.parse
import zoneinfo
from collections.abc import Iterator
from pathlib import Path
from typing import Any

import more_itertools
from mypy_boto3_s3.client import S3Client
from mypy_boto3_s3.type_defs import ObjectTypeDef


def join_paths(*paths: str) -> str:
    """Join URL components into a single URL."""
    return posixpath.join(*paths)


def split_s3_url(s3_url: str) -> tuple[str, str]:
    """Split a given S3 URL into a pair of bucket and key.

    Args:
        s3_url (str): The S3 URL to be split.

    Returns:
        (str, str): The pair of S3 bucket and S3 key.
    """
    parts = urllib.parse.urlparse(s3_url)
    if parts.scheme != "s3":
        raise ValueError(f'The given URL is not for S3: "{s3_url}"')
    return (parts.netloc, parts.path.lstrip("/"))


def get_s3_last_modified(
    s3_url: str,
    s3_client: S3Client,
    timezone: zoneinfo.ZoneInfo,
) -> datetime.datetime:
    """Get last modified time of the s3 object."""
    (bucket, key) = split_s3_url(s3_url)
    response = s3_client.get_object(
        Bucket=bucket,
        Key=key,
    )
    errors = response.get("Errors")
    if errors:
        raise RuntimeError(f"Failed to get last modified time: {errors}")

    return response["LastModified"].astimezone(timezone)


def exists(
    s3_url: str,
    s3_client: S3Client,
) -> bool:
    """Check if an S3 object exists."""
    (bucket, key) = split_s3_url(s3_url)
    if contents := s3_client.list_objects(Prefix=key, Bucket=bucket).get("Contents"):
        for content in contents:
            if content.get("Key") == key:
                return True
    return False


def list_s3_contents(
    s3_url: str,
    s3_client: S3Client,
) -> Iterator[ObjectTypeDef]:
    """List S3 contents with the prefix.

    Args:
        s3_url (str): The S3 URL to be split.
        s3_client (S3Client): The S3 client.

    Returns:
        Iterator[ObjectTypeDef]: The iterator of S3 content with the target S3 prefix.
    """
    (bucket, prefix) = split_s3_url(s3_url)
    paginator = s3_client.get_paginator("list_objects_v2")
    for page in paginator.paginate(Bucket=bucket, Prefix=prefix):
        yield from page.get("Contents", ())


def list_s3_keys(
    s3_url: str,
    s3_client: S3Client,
) -> Iterator[str]:
    """List S3 keys with the prefix.

    Args:
        s3_url (str): The S3 URL to list the content.
        s3_client (S3Client): The S3 client.

    Returns:
        Iterator[str]: The iterator of S3 key with the target S3 prefix.
    """
    for content in list_s3_contents(s3_url, s3_client):
        if "Key" in content:
            yield content["Key"]


def list_s3_urls(
    s3_url: str,
    s3_client: S3Client,
) -> Iterator[str]:
    """List S3 urls with the prefix.

    Args:
        s3_url (str): The S3 URL to list the content.
        s3_client (S3Client): The S3 client.

    Returns:
        Iterator[str]: An iterator of S3 URLs with the target S3 prefix.
    """
    (bucket, _) = split_s3_url(s3_url)
    for content in list_s3_contents(s3_url, s3_client):
        if "Key" in content:
            yield f"s3://{bucket}/{content['Key']}"


def copy_s3_object(
    source_s3_url: str,
    target_s3_url: str,
    s3_client: S3Client,
    logger: logging.Logger,
) -> None:
    """Copy an S3 object to target path.

    Args:
        source_s3_url (str): S3 URL of the source file.
        target_s3_url (str): S3 URL of the target to copy.
        s3_client (S3Client): S3 client.
        logger (Logger): Logger to write the progress.
    """
    if source_s3_url == target_s3_url:
        logger.info(
            'The source and target S3 URLs are the same. Skip copying "%s".',
            source_s3_url,
        )
        return

    logger.info(
        'Copying an S3 object "%s" to "%s".',
        source_s3_url,
        target_s3_url,
    )

    (source_bucket, source_key) = split_s3_url(source_s3_url)
    (target_bucket, target_key) = split_s3_url(target_s3_url)

    s3_client.copy(
        {"Bucket": source_bucket, "Key": source_key},
        target_bucket,
        target_key,
    )


def delete_s3_objects(
    s3_bucket: str,
    keys_to_be_deleted: list[str],
    s3_client: S3Client,
    logger: logging.Logger,
) -> None:
    """Delete specified S3 objects.

    Args:
        s3_bucket (str): Target S3 bucket.
        keys_to_be_deleted: (list[str]): List of keys to be deleted.
        s3_client (S3Client): S3 client.
        logger (Logger): Logger to write the progress.
    """
    # Split `keys_to_be_deleted` into lists of up to 1000 keys because
    # `delete_objects` can delete up to 1000 keys at once.
    for keys in more_itertools.chunked(keys_to_be_deleted, 1000):
        logger.info("Delete %s objects such as %s.", len(keys), keys[:2])
        response = s3_client.delete_objects(
            Bucket=s3_bucket,
            Delete={
                "Objects": tuple({"Key": key} for key in keys),
                "Quiet": True,
            },
        )
        errors = response.get("Errors")
        if errors:
            raise RuntimeError(f"Failed to delete objects: {errors}")


def delete_s3_object(
    s3_url: str,
    s3_client: S3Client,
    logger: logging.Logger,
) -> None:
    """Delete specified S3 objects.

    Args:
        s3_url (str): Target S3 URL.
        s3_client (S3Client): S3 client.
        logger (Logger): Logger to write the progress.
    """
    (bucket, s3_key) = split_s3_url(s3_url)

    delete_s3_objects(bucket, [s3_key], s3_client, logger)


def move_s3_object(
    source_s3_url: str,
    target_s3_url: str,
    s3_client: S3Client,
    logger: logging.Logger,
) -> None:
    """Move an S3 object to target path.

    Args:
        source_s3_url (str): S3 URL of the source file.
        target_s3_url (str): S3 URL of the target to copy.
        s3_client (S3Client): S3 client.
        logger (Logger): Logger to write the progress.
    """
    if source_s3_url == target_s3_url:
        logger.info(
            'The source and target S3 URLs are the same. Skip moving "%s".',
            source_s3_url,
        )
        return
    copy_s3_object(source_s3_url, target_s3_url, s3_client, logger)
    delete_s3_object(source_s3_url, s3_client, logger)


def download_from_s3(
    s3_url: str,
    output_file_path: str,
    s3_client: S3Client,
    logger: logging.Logger,
) -> None:
    """Download an S3 object to local.

    Args:
        s3_url (str): The S3 URL of the source file.
        output_file_path (str): The path to the output local file.
        s3_client (S3Client): S3 client.
        logger (Logger): The logger to write the progress.
    """
    logger.info(
        'Downloading S3 object "%s" to local "%s".',
        s3_url,
        output_file_path,
    )
    (s3_bucket, s3_prefix) = split_s3_url(s3_url)
    s3_client.download_file(s3_bucket, s3_prefix, output_file_path)


def upload_to_s3(
    file_path: str,
    output_s3_url: str,
    s3_client: S3Client,
    logger: logging.Logger,
    extra_args: dict[str, Any] | None = None,
) -> None:
    # Normalize paths to avoid mixed path separators (e.g. on Windows)
    ends_with_slash = file_path.endswith("/") or file_path.endswith("\\")
    file_path = os.path.normpath(file_path)
    if ends_with_slash and not file_path.endswith(os.sep):
        file_path += os.sep

    logger.info(
        'Uploading a local file "%s" to S3 "%s".',
        file_path,
        output_s3_url,
    )
    (output_s3_bucket, output_s3_prefix) = split_s3_url(output_s3_url)
    if Path(file_path).is_dir():
        for subdir, _, files in os.walk(file_path):
            for f in files:
                full_path = os.path.join(subdir, f)
                relpath = os.path.relpath(full_path, file_path)
                # S3 keys must use forward slashes
                relpath_s3 = relpath.replace("\\", "/")
                if ends_with_slash:
                    # If the file_path ends with "/",
                    # upload the files under the directory.
                    key = relpath_s3
                else:
                    # If the file_path does not end with "/",
                    # upload the directory and the files under it.
                    key = join_paths(os.path.basename(file_path), relpath_s3)
                s3_client.upload_file(
                    full_path,
                    output_s3_bucket,
                    join_paths(output_s3_prefix, key),
                    ExtraArgs=extra_args or {},
                )
    else:
        s3_client.upload_file(
            file_path,
            output_s3_bucket,
            output_s3_prefix,
            ExtraArgs=extra_args or {},
        )
