import os
import tempfile
import unittest.mock
from typing import Any

import pytest

from src.common import s3_util


def test_join_paths() -> None:
    assert s3_util.join_paths("foo", "bar") == "foo/bar"
    assert s3_util.join_paths("s3://foo/", "bar") == "s3://foo/bar"


@pytest.mark.parametrize(
    [
        "s3_url",
        "expected",
    ],
    [
        pytest.param(
            "s3://test_bucket/foo/bar.json",
            ("test_bucket", "foo/bar.json"),
            id="With file",
        ),
        pytest.param(
            "s3://test_bucket/foo/bar/",
            ("test_bucket", "foo/bar/"),
            id="With directory",
        ),
        pytest.param("s3://test_bucket", ("test_bucket", ""), id="No path"),
    ],
)
def test_split_s3_url(s3_url: str, expected: tuple[str, str]) -> None:
    assert s3_util.split_s3_url(s3_url) == expected


@pytest.mark.parametrize(
    [
        "source_s3_url",
        "target_s3_url",
        "expected_copy_args",
    ],
    [
        pytest.param(
            "s3://source_bucket/foo/source.json",
            "s3://target_bucket/bar/target.json",
            (
                {"Bucket": "source_bucket", "Key": "foo/source.json"},
                "target_bucket",
                "bar/target.json",
            ),
        ),
    ],
)
def test_copy_s3_object(
    source_s3_url: str,
    target_s3_url: str,
    expected_copy_args: tuple[Any, ...],
) -> None:
    mock_s3_client = unittest.mock.Mock()
    mock_logger = unittest.mock.Mock()
    s3_util.copy_s3_object(source_s3_url, target_s3_url, mock_s3_client, mock_logger)
    mock_s3_client.copy.assert_called_once_with(*expected_copy_args)


@pytest.mark.parametrize(
    [
        "source_s3_url",
        "target_s3_url",
    ],
    [
        pytest.param(
            "s3://source_bucket/foo/source.json",
            "s3://source_bucket/foo/source.json",
        ),
    ],
)
def test_copy_same_s3_object(
    source_s3_url: str,
    target_s3_url: str,
) -> None:
    mock_s3_client = unittest.mock.Mock()
    mock_logger = unittest.mock.Mock()
    s3_util.copy_s3_object(source_s3_url, target_s3_url, mock_s3_client, mock_logger)
    mock_s3_client.copy.assert_not_called()


@pytest.mark.parametrize(
    [
        "s3_bucket",
        "keys_to_be_deleted",
        "expected_delete_objects_kwargs_list",
    ],
    [
        pytest.param(
            "target_bucket",
            ["0000.json", "0001.json"],
            [
                {
                    "Bucket": "target_bucket",
                    "Delete": {
                        "Objects": (
                            {
                                "Key": "0000.json",
                            },
                            {
                                "Key": "0001.json",
                            },
                        ),
                        "Quiet": True,
                    },
                },
            ],
            id="2 objects",
        ),
        pytest.param(
            "target_bucket",
            # 1100 objects from "0000.json" to "1099.json".
            [f"{i:04d}.json" for i in range(1100)],
            [
                {
                    "Bucket": "target_bucket",
                    # 1000 objects from "0000.json" to "0999.json".
                    "Delete": {
                        "Objects": tuple({"Key": f"{i:04d}.json"} for i in range(1000)),
                        "Quiet": True,
                    },
                },
                {
                    "Bucket": "target_bucket",
                    # 100 objects from "1000.json" to "1099.json".
                    "Delete": {
                        "Objects": tuple(
                            {"Key": f"{i:04d}.json"} for i in range(1000, 1100)
                        ),
                        "Quiet": True,
                    },
                },
            ],
            id="More than 1000 objects",
        ),
    ],
)
def test_delete_s3_objects(
    s3_bucket: str,
    keys_to_be_deleted: list[str],
    expected_delete_objects_kwargs_list: list[dict[str, Any]],
) -> None:
    mock_s3_client = unittest.mock.Mock()
    mock_s3_client.delete_objects.return_value = {}
    mock_logger = unittest.mock.Mock()
    s3_util.delete_s3_objects(
        s3_bucket, keys_to_be_deleted, mock_s3_client, mock_logger
    )
    for expected_delete_objects_kwargs in expected_delete_objects_kwargs_list:
        mock_s3_client.delete_objects.assert_any_call(**expected_delete_objects_kwargs)


@pytest.mark.parametrize(
    [
        "s3_url",
        "expected_delete_objects_kwargs",
    ],
    [
        pytest.param(
            "s3://target_bucket/bar/target.json",
            {
                "Bucket": "target_bucket",
                "Delete": {
                    "Objects": (
                        {
                            "Key": "bar/target.json",
                        },
                    ),
                    "Quiet": True,
                },
            },
        ),
    ],
)
def test_delete_s3_object(
    s3_url: str,
    expected_delete_objects_kwargs: dict[str, Any],
) -> None:
    mock_s3_client = unittest.mock.Mock()
    mock_s3_client.delete_objects.return_value = {}
    mock_logger = unittest.mock.Mock()
    s3_util.delete_s3_object(s3_url, mock_s3_client, mock_logger)
    mock_s3_client.delete_objects.assert_called_once_with(
        **expected_delete_objects_kwargs
    )


@pytest.mark.parametrize(
    [
        "source_s3_url",
        "target_s3_url",
        "expected_copy_args",
        "expected_delete_objects_kwargs",
    ],
    [
        pytest.param(
            "s3://source_bucket/foo/source.json",
            "s3://target_bucket/bar/target.json",
            (
                {"Bucket": "source_bucket", "Key": "foo/source.json"},
                "target_bucket",
                "bar/target.json",
            ),
            {
                "Bucket": "source_bucket",
                "Delete": {
                    "Objects": (
                        {
                            "Key": "foo/source.json",
                        },
                    ),
                    "Quiet": True,
                },
            },
        ),
    ],
)
def test_move_s3_object(
    source_s3_url: str,
    target_s3_url: str,
    expected_copy_args: tuple[Any, ...],
    expected_delete_objects_kwargs: dict[str, Any],
) -> None:
    mock_s3_client = unittest.mock.Mock()
    mock_s3_client.delete_objects.return_value = {}
    mock_logger = unittest.mock.Mock()
    s3_util.move_s3_object(source_s3_url, target_s3_url, mock_s3_client, mock_logger)
    mock_s3_client.copy.assert_called_once_with(*expected_copy_args)
    mock_s3_client.delete_objects.assert_called_once_with(
        **expected_delete_objects_kwargs
    )


@pytest.mark.parametrize(
    [
        "source_s3_url",
        "target_s3_url",
    ],
    [
        pytest.param(
            "s3://source_bucket/foo/source.json",
            "s3://source_bucket/foo/source.json",
        ),
    ],
)
def test_move_same_s3_object(
    source_s3_url: str,
    target_s3_url: str,
) -> None:
    mock_s3_client = unittest.mock.Mock()
    mock_s3_client.delete_objects.return_value = {}
    mock_logger = unittest.mock.Mock()
    s3_util.move_s3_object(source_s3_url, target_s3_url, mock_s3_client, mock_logger)
    mock_s3_client.copy.assert_not_called()
    mock_s3_client.delete_objects.assert_not_called()


@pytest.mark.parametrize(
    [
        "source_s3_url",
        "output_file_path",
        "expected_download_file_args",
    ],
    [
        pytest.param(
            "s3://source_bucket/foo/source.json",
            "bar/target.json",
            (
                "source_bucket",
                "foo/source.json",
                "bar/target.json",
            ),
        ),
    ],
)
def test_download_from_s3(
    source_s3_url: str,
    output_file_path: str,
    expected_download_file_args: tuple[Any, ...],
) -> None:
    mock_s3_client = unittest.mock.Mock()
    mock_s3_client.download_file.return_value = {}
    mock_logger = unittest.mock.Mock()
    s3_util.download_from_s3(
        source_s3_url, output_file_path, mock_s3_client, mock_logger
    )
    mock_s3_client.download_file.assert_called_once_with(*expected_download_file_args)


@pytest.mark.parametrize(
    [
        "file_path",
        "output_s3_url",
        "extra_args",
        "expected_upload_file_args",
        "expected_upload_file_kwargs",
    ],
    [
        pytest.param(
            "foo/target.json",
            "s3://target_bucket/bar/source.json",
            None,
            (
                "foo/target.json",
                "target_bucket",
                "bar/source.json",
            ),
            {"ExtraArgs": {}},
        ),
        pytest.param(
            "foo/target.json",
            "s3://target_bucket/bar/source.json",
            {"a": 1, "b": 2},
            (
                "foo/target.json",
                "target_bucket",
                "bar/source.json",
            ),
            {"ExtraArgs": {"a": 1, "b": 2}},
        ),
    ],
)
def test_upload_to_s3(
    file_path: str,
    output_s3_url: str,
    extra_args: dict[str, Any] | None,
    expected_upload_file_args: tuple[Any, ...],
    expected_upload_file_kwargs: dict[str, Any],
) -> None:
    mock_s3_client = unittest.mock.Mock()
    mock_s3_client.download_file.return_value = {}
    mock_logger = unittest.mock.Mock()
    s3_util.upload_to_s3(
        file_path,
        output_s3_url,
        mock_s3_client,
        mock_logger,
        extra_args=extra_args,
    )
    # Normalize the expected local file path for platform compatibility
    expected_args = list(expected_upload_file_args)
    expected_args[0] = os.path.normpath(expected_args[0])
    mock_s3_client.upload_file.assert_called_once_with(
        *expected_args,
        **expected_upload_file_kwargs,
    )


def test_upload_to_s3_recursive() -> None:
    mock_s3_client = unittest.mock.Mock()
    mock_s3_client.download_file.return_value = {}
    mock_logger = unittest.mock.Mock()
    with tempfile.TemporaryDirectory() as tmp_dir:
        # assert tmp_dir.endswith("/")
        os.mkdir(os.path.join(tmp_dir, "source"))
        with open(os.path.join(tmp_dir, "source", "a.txt"), "w"):
            pass
        os.mkdir(os.path.join(tmp_dir, "source", "subdir"))
        with open(os.path.join(tmp_dir, "source", "subdir", "b.txt"), "w"):
            pass
        s3_util.upload_to_s3(
            f"{tmp_dir}/source/",
            "s3://target_bucket/bar/",
            mock_s3_client,
            mock_logger,
        )
        mock_s3_client.upload_file.assert_any_call(
            os.path.join(tmp_dir, "source", "a.txt"),
            "target_bucket",
            "bar/a.txt",
            ExtraArgs={},
        )
        mock_s3_client.upload_file.assert_any_call(
            os.path.join(tmp_dir, "source", "subdir", "b.txt"),
            "target_bucket",
            "bar/subdir/b.txt",
            ExtraArgs={},
        )

        mock_s3_client.upload_file.reset_mock()

        s3_util.upload_to_s3(
            f"{tmp_dir}/source",
            "s3://target_bucket/bar/",
            mock_s3_client,
            mock_logger,
        )
        mock_s3_client.upload_file.assert_any_call(
            os.path.join(tmp_dir, "source", "a.txt"),
            "target_bucket",
            "bar/source/a.txt",
            ExtraArgs={},
        )
        mock_s3_client.upload_file.assert_any_call(
            os.path.join(tmp_dir, "source", "subdir", "b.txt"),
            "target_bucket",
            "bar/source/subdir/b.txt",
            ExtraArgs={},
        )


@pytest.mark.parametrize(
    [
        "s3_url",
        "mock_contents",
        "expected_keys",
    ],
    [
        pytest.param(
            "s3://test_bucket/path/",
            [
                {"Key": "path/file1.txt"},
                {"Key": "path/file2.txt"},
                {"Key": "path/subdir/file3.txt"},
            ],
            ["path/file1.txt", "path/file2.txt", "path/subdir/file3.txt"],
            id="Multiple files",
        ),
        pytest.param(
            "s3://test_bucket/path/",
            [],
            [],
            id="Empty result",
        ),
        pytest.param(
            "s3://test_bucket/path/",
            [
                {"Key": "path/file1.txt"},
                {"Size": 1024},  # Object without Key should be ignored
            ],
            ["path/file1.txt"],
            id="Mixed content with object without Key",
        ),
    ],
)
def test_list_s3_keys(
    s3_url: str,
    mock_contents: list[dict[str, Any]],
    expected_keys: list[str],
) -> None:
    mock_s3_client = unittest.mock.Mock()
    mock_paginator = unittest.mock.Mock()
    mock_s3_client.get_paginator.return_value = mock_paginator
    mock_paginator.paginate.return_value = [{"Contents": mock_contents}]

    result = list(s3_util.list_s3_keys(s3_url, mock_s3_client))

    assert result == expected_keys
    mock_s3_client.get_paginator.assert_called_once_with("list_objects_v2")
    mock_paginator.paginate.assert_called_once_with(
        Bucket="test_bucket", Prefix="path/"
    )


@pytest.mark.parametrize(
    [
        "s3_url",
        "mock_contents",
        "expected_urls",
    ],
    [
        pytest.param(
            "s3://test_bucket/path/",
            [
                {"Key": "path/file1.txt"},
                {"Key": "path/file2.txt"},
                {"Key": "path/subdir/file3.txt"},
            ],
            [
                "s3://test_bucket/path/file1.txt",
                "s3://test_bucket/path/file2.txt",
                "s3://test_bucket/path/subdir/file3.txt",
            ],
            id="Multiple files",
        ),
        pytest.param(
            "s3://test_bucket/path/",
            [],
            [],
            id="Empty result",
        ),
        pytest.param(
            "s3://test_bucket/path/",
            [
                {"Key": "path/file1.txt"},
                {"Size": 1024},  # Object without Key should be ignored
            ],
            ["s3://test_bucket/path/file1.txt"],
            id="Mixed content with object without Key",
        ),
    ],
)
def test_list_s3_urls(
    s3_url: str,
    mock_contents: list[dict[str, Any]],
    expected_urls: list[str],
) -> None:
    mock_s3_client = unittest.mock.Mock()
    mock_paginator = unittest.mock.Mock()
    mock_s3_client.get_paginator.return_value = mock_paginator
    mock_paginator.paginate.return_value = [{"Contents": mock_contents}]

    result = list(s3_util.list_s3_urls(s3_url, mock_s3_client))

    assert result == expected_urls
    mock_s3_client.get_paginator.assert_called_once_with("list_objects_v2")
    mock_paginator.paginate.assert_called_once_with(
        Bucket="test_bucket", Prefix="path/"
    )


def test_list_s3_keys_empty() -> None:
    mock_s3_client = unittest.mock.Mock()
    mock_paginator = unittest.mock.Mock()
    mock_s3_client.get_paginator.return_value = mock_paginator

    # Mock paginate to return empty results
    mock_paginator.paginate.return_value = [{}]

    result = list(s3_util.list_s3_keys("s3://test_bucket/folder/", mock_s3_client))

    assert result == []
    mock_s3_client.get_paginator.assert_called_once_with("list_objects_v2")
    mock_paginator.paginate.assert_called_once_with(
        Bucket="test_bucket", Prefix="folder/"
    )


def test_list_s3_urls_empty() -> None:
    mock_s3_client = unittest.mock.Mock()
    mock_paginator = unittest.mock.Mock()
    mock_s3_client.get_paginator.return_value = mock_paginator

    # Mock paginate to return empty results
    mock_paginator.paginate.return_value = [{}]

    result = list(s3_util.list_s3_urls("s3://test_bucket/folder/", mock_s3_client))

    assert result == []
    mock_s3_client.get_paginator.assert_called_once_with("list_objects_v2")
    mock_paginator.paginate.assert_called_once_with(
        Bucket="test_bucket", Prefix="folder/"
    )
