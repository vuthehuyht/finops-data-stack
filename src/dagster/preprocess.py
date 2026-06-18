"""Input file preprocess library."""

import enum
import logging
import os
import posixpath
import re
import shutil
import tempfile
import zoneinfo

from mypy_boto3_s3.client import S3Client

from src.common import s3_util


@enum.unique
class PreprocessType(enum.Enum):
    """Preprocess types."""

    Noop = "NOOP"
    Zip = "ZIP"
    RemoveNull = "REMOVE_NULL"
    RemoveNullUTF16 = "REMOVE_NULL_UTF16"
    RemoveInvalidFormula = "REMOVE_INVALID_FORMULA"
    Tar = "TAR"


def _remove_null_character(file_path: str, processed_file_path: str) -> None:
    if file_path == processed_file_path:
        raise ValueError(f"The input and output file paths are the same: {file_path}")
    with open(file_path, "rb") as f:
        with open(processed_file_path, "wb") as f_out:
            for line in f:
                f_out.write(line.replace(b"\x00", b""))


def _remove_null_character_utf16(file_path: str, processed_file_path: str) -> None:
    if file_path == processed_file_path:
        raise ValueError(f"The input and output file paths are the same: {file_path}")
    with open(file_path, encoding="utf16", newline="\n") as f:
        with open(processed_file_path, "w", encoding="utf16", newline="\n") as f_out:
            for line in f:
                f_out.write(line.replace("\x00", ""))


def _remove_invalid_formula(file_path: str, processed_file_path: str) -> None:
    if file_path == processed_file_path:
        raise ValueError(f"The input and output file paths are the same: {file_path}")
    with open(file_path, "rb") as f:
        with open(processed_file_path, "wb") as f_out:
            for line in f:
                if b"invalid formula" not in line:
                    f_out.write(line)


def _unpack_archive(
    src_file_path: str,
    stage_s3_url: str,
    format: str,
    s3_client: S3Client,
    logger: logging.Logger,
) -> None:
    logger.info("Unpacking %s %s", format, src_file_path)
    with tempfile.TemporaryDirectory() as unpacked_out_dir:
        shutil.unpack_archive(src_file_path, unpacked_out_dir, format)
        s3_util.upload_to_s3(
            f"{unpacked_out_dir}/",
            stage_s3_url,
            s3_client,
            logger,
        )


def preprocess_raw_file_on_s3(
    s3_url: str,
    key_regex_pattern: str,
    stage_s3_path: str,
    preprocess_type: PreprocessType,
    s3_client: S3Client,
    logger: logging.Logger,
) -> None:
    """Preprocess raw file on s3 and copy to stage."""
    match = re.search(
        key_regex_pattern,
        s3_url,
    )
    if not match:
        # Workaround for old data: remove unixtime
        key_regex_old_pattern = re.sub(r"\/\d{10}\/", "/", key_regex_pattern)
        match = re.search(
            key_regex_old_pattern,
            s3_url,
        )
        if match:
            modified_time = s3_util.get_s3_last_modified(
                s3_url, s3_client, zoneinfo.ZoneInfo("Asia/Ho_Chi_Minh")
            )
            new_s3_url = s3_util.join_paths(
                posixpath.dirname(s3_url),
                str(int(modified_time.timestamp())),
                posixpath.basename(s3_url),
            )
            logger.info("Workaround for old data; Moving %s to %s", s3_url, new_s3_url)
            s3_util.move_s3_object(
                s3_url,
                new_s3_url,
                s3_client,
                logger,
            )
            s3_url = new_s3_url
            match = re.search(
                key_regex_pattern,
                s3_url,
            )
    if not match:
        raise RuntimeError(
            f"data_storage key pattern {s3_url} "
            f"doesn't match expected pattern {key_regex_pattern}."
        )

    # example: TABLE_NAME/2024-07-01/1719821350/TABLE_NAME.csv.1719821350
    # or TABLE_NAME/2024-07-01/1719821350/TABLE_NAME.csv
    key_after_stage = match.group(1)
    stage_s3_url = s3_util.join_paths(
        stage_s3_path,
        key_after_stage,
    )

    if preprocess_type == PreprocessType.Noop:
        stage_s3_url = s3_util.join_paths(
            stage_s3_path,
            key_after_stage,
        )
        s3_util.copy_s3_object(
            s3_url,
            stage_s3_url,
            s3_client,
            logger,
        )
        return

    # Apply specific preprocessing if needed.
    with tempfile.TemporaryDirectory() as tmp_dir:
        src_file_path = os.path.join(tmp_dir, os.path.basename(key_after_stage))
        s3_util.download_from_s3(
            s3_url,
            src_file_path,
            s3_client,
            logger,
        )

        if preprocess_type == PreprocessType.Zip:
            _unpack_archive(
                src_file_path,
                stage_s3_url,
                "zip",
                s3_client,
                logger,
            )
            return

        if preprocess_type == PreprocessType.Tar:
            _unpack_archive(
                src_file_path,
                stage_s3_url,
                "tar",
                s3_client,
                logger,
            )
            return

        processed_file_path = os.path.join(
            tmp_dir,
            os.path.basename(key_after_stage) + ".out",
        )

        match preprocess_type:
            case PreprocessType.RemoveNull:
                logger.info("Removing null character: %s", src_file_path)
                _remove_null_character(src_file_path, processed_file_path)
            case PreprocessType.RemoveNullUTF16:
                logger.info("Removing null character in UTF-16: %s", src_file_path)
                _remove_null_character_utf16(src_file_path, processed_file_path)
            case PreprocessType.RemoveInvalidFormula:
                logger.info("Removing invalid formula: %s", src_file_path)
                _remove_invalid_formula(src_file_path, processed_file_path)
            case _:
                raise ValueError(f"Unexpected preprocess type: {preprocess_type}")

        s3_util.upload_to_s3(
            processed_file_path,
            stage_s3_url,
            s3_client,
            logger,
        )
