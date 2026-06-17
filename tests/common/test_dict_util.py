import copy
from typing import Any

import pytest

from src.common import dict_util


@pytest.mark.parametrize(
    [
        "v1",
        "v2",
        "expected",
    ],
    [
        pytest.param(
            ["test1"],
            ["test2", "test3"],
            ["test1", "test2", "test3"],
            id="list + list",
        ),
        pytest.param(
            ["test1"],
            {"key2": "test2"},
            {"key2": "test2"},
            id="list + dict",
        ),
    ],
)
def test_list_concat_merger(v1: Any, v2: Any, expected: Any) -> None:
    raw_v1 = copy.deepcopy(v1)
    raw_v2 = copy.deepcopy(v2)
    assert dict_util.list_concat_merger(v1, v2) == expected
    # The input arguments should not be changed.
    assert raw_v1 == v1
    assert raw_v2 == v2


@pytest.mark.parametrize(
    [
        "onto_dict",
        "from_dict",
        "expected",
    ],
    [
        pytest.param(
            {"key1": "test1"},
            {"key2": "test2"},
            {"key1": "test1", "key2": "test2"},
        ),
        pytest.param(
            {"key1": "test1"},
            {"key1": "test2"},
            {"key1": "test2"},
        ),
        pytest.param(
            {"key1": ["test1"]},
            {"key1": ["test2", "test3"]},
            {"key1": ["test1", "test2", "test3"]},
        ),
        pytest.param(
            {"key1": ["test1"]},
            {"key1": {"key2": "test2"}},
            {"key1": {"key2": "test2"}},
        ),
        pytest.param(
            {
                "key1": {
                    "key2": ["test1"],
                },
            },
            {
                "key1": {
                    "key2": ["test2", "test3"],
                },
            },
            {
                "key1": {
                    "key2": ["test1", "test2", "test3"],
                },
            },
        ),
    ],
)
def test_deep_merge_dicts(
    onto_dict: dict[str, Any], from_dict: dict[str, Any], expected: dict[str, Any]
) -> None:
    raw_onto_dict = copy.deepcopy(onto_dict)
    raw_from_dict = copy.deepcopy(from_dict)
    assert (
        dict_util.deep_merge_dicts(
            onto_dict,
            from_dict,
            dict_util.list_concat_merger,
        )
        == expected
    )
    # The input dictionaries should not be changed.
    assert onto_dict == raw_onto_dict
    assert from_dict == raw_from_dict


def test_invert_many_to_many_mapping() -> None:
    original = {
        1: {"1", "one", "common"},
        2: {"2", "two", "common"},
        3: set(),
    }
    inverted = {
        "1": {1},
        "one": {1},
        "2": {2},
        "two": {2},
        "common": {1, 2},
    }
    assert dict_util.invert_many_to_many_mapping(original) == inverted


def test_optional_as_dict() -> None:
    assert dict_util.optional_as_dict("key", "value") == {"key": "value"}
    assert dict_util.optional_as_dict("key", "") == {"key": ""}
    assert dict_util.optional_as_dict("key", None) == {}
