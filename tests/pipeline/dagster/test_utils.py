"""Tests for `utils.py`."""

import os
import unittest.mock

import dagster
import pytest

from src.pipeline.dagster import utils


@pytest.mark.parametrize(
    [
        "path",
        "expected",
    ],
    [
        pytest.param(
            ["test_schema", "test_table"],
            dagster.AssetKey(["test_schema", "test_table"]),
            id="No prefix",
        ),
    ],
)
def test_asset_key_without_prefix(path: list[str], expected: dagster.AssetKey) -> None:
    with unittest.mock.patch.dict(
        os.environ,
        {},
        clear=True,
    ):
        assert "DAGSTER_ASSET_PREFIX" not in os.environ
        assert utils.asset_key(path) == expected


@pytest.mark.parametrize(
    [
        "path",
        "prefix",
        "expected",
    ],
    [
        pytest.param(
            ["test_schema", "test_table"],
            "dev",
            dagster.AssetKey(["dev", "test_schema", "test_table"]),
            id="With prefix",
        ),
        pytest.param(
            ["test_schema", "test_table"],
            "",
            dagster.AssetKey(["test_schema", "test_table"]),
            id="With empty",
        ),
        pytest.param(
            ["test_schema", "test_table"],
            "test_schema",
            dagster.AssetKey(["test_schema", "test_table"]),
            id="With duplicate prefix",
        ),
    ],
)
def test_asset_key_with_prefix(
    path: list[str], prefix: str, expected: dagster.AssetKey
) -> None:
    with unittest.mock.patch.dict(
        os.environ,
        {
            "DAGSTER_ASSET_PREFIX": prefix,
        },
        clear=True,
    ):
        assert "DAGSTER_ASSET_PREFIX" in os.environ
        assert utils.asset_key(path) == expected


def test_asset_key_with_empty_path() -> None:
    with pytest.raises(ValueError, match="^Path must not be empty$"):
        assert utils.asset_key([]).parts
