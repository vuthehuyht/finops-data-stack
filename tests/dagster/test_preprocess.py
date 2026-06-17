import os
import tempfile
import unittest

import pytest_mock

from src.dagster import preprocess


def test_remove_null_character() -> None:
    with tempfile.TemporaryDirectory() as tmp_dir:
        test_file = os.path.join(tmp_dir, "test_file")
        out_file = os.path.join(tmp_dir, "out_file")
        with open(test_file, "wb") as f:
            f.write(b"test\0\r\nt\0e\0s\0t\0_\n")
        preprocess._remove_null_character(test_file, out_file)
        with open(out_file, "rb") as f:
            assert f.read() == b"test\r\ntest_\n"


def test_remove_null_character_utf16() -> None:
    with tempfile.TemporaryDirectory() as tmp_dir:
        test_file = os.path.join(tmp_dir, "test_file")
        out_file = os.path.join(tmp_dir, "out_file")
        with open(test_file, "w", encoding="utf16", newline="\n") as f:
            f.write("test\0\r\nt\0e\0s\0t\0_\n")
        preprocess._remove_null_character_utf16(test_file, out_file)
        with open(out_file, encoding="utf16", newline="\n") as f:
            assert f.read() == "test\r\ntest_\n"


def test_remove_invalid_formula() -> None:
    with tempfile.TemporaryDirectory() as tmp_dir:
        test_file = os.path.join(tmp_dir, "test_file")
        out_file = os.path.join(tmp_dir, "out_file")
        with open(test_file, "wb") as f:
            f.write(b"test\r\ntest,invalid formula\r\ninvalid formula\ntest2\n")
        preprocess._remove_invalid_formula(test_file, out_file)
        with open(out_file, "rb") as f:
            assert f.read() == b"test\r\ntest2\n"


def test_copy_raw_file_to_snowflake_stage_noop(
    mocker: pytest_mock.MockerFixture,
) -> None:
    mock_s3_client = unittest.mock.Mock()
    mock_s3_client.download_file.return_value = {}
    mock_logger = unittest.mock.Mock()
    copy_s3_object_mock = mocker.patch(
        "src.dagster.preprocess.s3_util.copy_s3_object"
    )
    preprocess.preprocess_raw_file_on_s3(
        "s3://datacore-input-dev/data_storage/japan/batch/TABLE_NAME/2024-07-01/1719821350/TABLE_NAME.csv.20240701112233",
        ".*/(TABLE_NAME/2024-07-01/1719821350/[^/\\.]+\\.\\w+\\.[0-9]{14})$",
        "s3://datacore-input-dev/stages/batch/",
        preprocess.PreprocessType.Noop,
        mock_s3_client,
        mock_logger,
    )
    copy_s3_object_mock.assert_called_once_with(
        "s3://datacore-input-dev/data_storage/japan/batch/TABLE_NAME/2024-07-01/1719821350/TABLE_NAME.csv.20240701112233",
        "s3://datacore-input-dev/stages/batch/TABLE_NAME/2024-07-01/1719821350/TABLE_NAME.csv.20240701112233",
        mock_s3_client,
        mock_logger,
    )


def test_copy_raw_file_to_snowflake_stage_zip(
    mocker: pytest_mock.MockerFixture,
) -> None:
    mock_s3_client = unittest.mock.Mock()
    mock_s3_client.download_file.return_value = {}
    mock_logger = unittest.mock.Mock()
    download_from_s3_mock = mocker.patch(
        "src.dagster.preprocess.s3_util.download_from_s3"
    )
    upload_to_s3_mock = mocker.patch("src.dagster.preprocess.s3_util.upload_to_s3")
    unpack_archive_mock = mocker.patch("shutil.unpack_archive")

    preprocess.preprocess_raw_file_on_s3(
        "s3://datacore-input-dev/data_storage/japan/batch/TABLE_NAME/2024-07-01/1719821350/TABLE_NAME.zip.20240701112233",
        ".*/(TABLE_NAME/2024-07-01/1719821350/[^/\\.]+\\.\\w+\\.[0-9]{14})$",
        "s3://datacore-input-dev/stages/batch/",
        preprocess.PreprocessType.Zip,
        mock_s3_client,
        mock_logger,
    )

    download_from_s3_mock.assert_called_once()
    source_s3_path = download_from_s3_mock.call_args[0][0]
    assert (
        source_s3_path
        == "s3://datacore-input-dev/data_storage/japan/batch/TABLE_NAME/2024-07-01/1719821350/TABLE_NAME.zip.20240701112233"
    )

    unpack_archive_mock.assert_called_once()
    zipfile_in_tempdir = download_from_s3_mock.call_args[0][1]
    assert zipfile_in_tempdir == unpack_archive_mock.call_args[0][0]

    upload_to_s3_mock.assert_called_once()
    zip_out_dir = unpack_archive_mock.call_args[0][1]
    assert f"{zip_out_dir}/" == upload_to_s3_mock.call_args[0][0]

    destination_s3_path = upload_to_s3_mock.call_args[0][1]
    assert (
        destination_s3_path
        == "s3://datacore-input-dev/stages/batch/TABLE_NAME/2024-07-01/1719821350/TABLE_NAME.zip.20240701112233"
    )


def test_copy_raw_file_to_snowflake_stage_remove_null(
    mocker: pytest_mock.MockerFixture,
) -> None:
    mock_s3_client = unittest.mock.Mock()
    mock_s3_client.download_file.return_value = {}
    mock_logger = unittest.mock.Mock()
    download_from_s3_mock = mocker.patch(
        "src.dagster.preprocess.s3_util.download_from_s3"
    )
    upload_to_s3_mock = mocker.patch("src.dagster.preprocess.s3_util.upload_to_s3")
    remove_null_mock = mocker.patch(
        "src.dagster.preprocess._remove_null_character"
    )
    preprocess.preprocess_raw_file_on_s3(
        "s3://datacore-input-dev/data_storage/japan/batch/TABLE_NAME/2024-07-01/1719821350/TABLE_NAME.csv.20240701112233",
        ".*/(TABLE_NAME/2024-07-01/1719821350/[^/\\.]+\\.\\w+\\.[0-9]{14})$",
        "s3://datacore-input-dev/stages/batch/",
        preprocess.PreprocessType.RemoveNull,
        mock_s3_client,
        mock_logger,
    )
    download_from_s3_mock.assert_called_once()
    assert (
        download_from_s3_mock.call_args[0][0]
        == "s3://datacore-input-dev/data_storage/japan/batch/TABLE_NAME/2024-07-01/1719821350/TABLE_NAME.csv.20240701112233"
    )
    remove_null_mock.assert_called_once()
    assert download_from_s3_mock.call_args[0][1] == remove_null_mock.call_args[0][0]
    upload_to_s3_mock.assert_called_once()
    assert remove_null_mock.call_args[0][1] == upload_to_s3_mock.call_args[0][0]
    assert (
        upload_to_s3_mock.call_args[0][1]
        == "s3://datacore-input-dev/stages/batch/TABLE_NAME/2024-07-01/1719821350/TABLE_NAME.csv.20240701112233"
    )


def test_copy_raw_file_to_snowflake_stage_remove_null_utf16(
    mocker: pytest_mock.MockerFixture,
) -> None:
    mock_s3_client = unittest.mock.Mock()
    mock_s3_client.download_file.return_value = {}
    mock_logger = unittest.mock.Mock()
    download_from_s3_mock = mocker.patch(
        "src.dagster.preprocess.s3_util.download_from_s3"
    )
    upload_to_s3_mock = mocker.patch("src.dagster.preprocess.s3_util.upload_to_s3")
    remove_null_mock = mocker.patch(
        "src.dagster.preprocess._remove_null_character_utf16"
    )
    preprocess.preprocess_raw_file_on_s3(
        "s3://datacore-input-dev/data_storage/japan/batch/TABLE_NAME/2024-07-01/1719821350/TABLE_NAME.csv.20240701112233",
        ".*/(TABLE_NAME/2024-07-01/1719821350/[^/\\.]+\\.\\w+\\.[0-9]{14})$",
        "s3://datacore-input-dev/stages/batch/",
        preprocess.PreprocessType.RemoveNullUTF16,
        mock_s3_client,
        mock_logger,
    )
    download_from_s3_mock.assert_called_once()
    assert (
        download_from_s3_mock.call_args[0][0]
        == "s3://datacore-input-dev/data_storage/japan/batch/TABLE_NAME/2024-07-01/1719821350/TABLE_NAME.csv.20240701112233"
    )
    remove_null_mock.assert_called_once()
    assert download_from_s3_mock.call_args[0][1] == remove_null_mock.call_args[0][0]
    upload_to_s3_mock.assert_called_once()
    assert remove_null_mock.call_args[0][1] == upload_to_s3_mock.call_args[0][0]
    assert (
        upload_to_s3_mock.call_args[0][1]
        == "s3://datacore-input-dev/stages/batch/TABLE_NAME/2024-07-01/1719821350/TABLE_NAME.csv.20240701112233"
    )


def test_copy_raw_file_to_snowflake_stage_remove_invalid_formula(
    mocker: pytest_mock.MockerFixture,
) -> None:
    mock_s3_client = unittest.mock.Mock()
    mock_s3_client.download_file.return_value = {}
    mock_logger = unittest.mock.Mock()
    download_from_s3_mock = mocker.patch(
        "src.dagster.preprocess.s3_util.download_from_s3"
    )
    upload_to_s3_mock = mocker.patch("src.dagster.preprocess.s3_util.upload_to_s3")
    remove_invalid_formula_mock = mocker.patch(
        "src.dagster.preprocess._remove_invalid_formula"
    )
    preprocess.preprocess_raw_file_on_s3(
        "s3://datacore-input-dev/data_storage/japan/batch/TABLE_NAME/2024-07-01/1719821350/TABLE_NAME.csv.20240701112233",
        ".*/(TABLE_NAME/2024-07-01/1719821350/[^/\\.]+\\.\\w+\\.[0-9]{14})$",
        "s3://datacore-input-dev/stages/batch/",
        preprocess.PreprocessType.RemoveInvalidFormula,
        mock_s3_client,
        mock_logger,
    )
    download_from_s3_mock.assert_called_once()
    assert (
        download_from_s3_mock.call_args[0][0]
        == "s3://datacore-input-dev/data_storage/japan/batch/TABLE_NAME/2024-07-01/1719821350/TABLE_NAME.csv.20240701112233"
    )
    remove_invalid_formula_mock.assert_called_once()
    assert (
        download_from_s3_mock.call_args[0][1]
        == remove_invalid_formula_mock.call_args[0][0]
    )
    upload_to_s3_mock.assert_called_once()
    assert (
        remove_invalid_formula_mock.call_args[0][1] == upload_to_s3_mock.call_args[0][0]
    )
    assert (
        upload_to_s3_mock.call_args[0][1]
        == "s3://datacore-input-dev/stages/batch/TABLE_NAME/2024-07-01/1719821350/TABLE_NAME.csv.20240701112233"
    )


def test_copy_raw_file_to_snowflake_stage_tar(
    mocker: pytest_mock.MockerFixture,
) -> None:
    mock_s3_client = unittest.mock.Mock()
    mock_s3_client.download_file.return_value = {}
    mock_logger = unittest.mock.Mock()
    download_from_s3_mock = mocker.patch(
        "src.dagster.preprocess.s3_util.download_from_s3"
    )
    upload_to_s3_mock = mocker.patch("src.dagster.preprocess.s3_util.upload_to_s3")
    unpack_archive_mock = mocker.patch("shutil.unpack_archive")

    preprocess.preprocess_raw_file_on_s3(
        "s3://datacore-input-dev/data_storage/japan/batch/TABLE_NAME/2024-07-01/1719821350/TABLE_NAME.tar.20240701112233",
        ".*/(TABLE_NAME/2024-07-01/1719821350/[^/\\.]+\\.\\w+\\.[0-9]{14})$",
        "s3://datacore-input-dev/stages/batch/",
        preprocess.PreprocessType.Tar,
        mock_s3_client,
        mock_logger,
    )

    download_from_s3_mock.assert_called_once()
    source_s3_path = download_from_s3_mock.call_args[0][0]
    assert (
        source_s3_path
        == "s3://datacore-input-dev/data_storage/japan/batch/TABLE_NAME/2024-07-01/1719821350/TABLE_NAME.tar.20240701112233"
    )

    unpack_archive_mock.assert_called_once()
    tarfile_in_tempdir = download_from_s3_mock.call_args[0][1]
    assert tarfile_in_tempdir == unpack_archive_mock.call_args[0][0]

    upload_to_s3_mock.assert_called_once()
    tar_out_dir = unpack_archive_mock.call_args[0][1]
    assert f"{tar_out_dir}/" == upload_to_s3_mock.call_args[0][0]

    destination_s3_path = upload_to_s3_mock.call_args[0][1]
    assert (
        destination_s3_path
        == "s3://datacore-input-dev/stages/batch/TABLE_NAME/2024-07-01/1719821350/TABLE_NAME.tar.20240701112233"
    )
