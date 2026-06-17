"""Utilities for manipulating dictionaries."""

import copy
from collections.abc import Callable, Mapping, Set
from typing import Any


def list_concat_merger(v1: Any, v2: Any) -> Any:
    """Merges two values.

    If both values are lists, it concatenates them. Otherwise, it returns the
    second value.
    """
    if isinstance(v1, list) and isinstance(v2, list):
        return v1 + v2
    else:
        return v2


def deep_merge_dicts(
    onto_dict: dict[str, Any],
    from_dict: dict[str, Any],
    f: Callable[[Any, Any], Any],
) -> dict[str, Any]:
    """Merges two dictionary inputs.

    This method is almost same as dagster.utils.merger.deep_merge_dicts except that
    it receives a function to be invoked when both dicts have values at the same
    position.
    """
    return _deep_merge_dicts(copy.deepcopy(onto_dict), from_dict, f)


def _deep_merge_dicts(
    onto_dict: dict[str, Any],
    from_dict: dict[str, Any],
    f: Callable[[Any, Any], Any],
) -> dict[str, Any]:
    for from_key, from_value in from_dict.items():
        if from_key not in onto_dict:
            onto_dict[from_key] = from_value
        else:
            onto_value = onto_dict[from_key]

            if isinstance(from_value, dict) and isinstance(onto_value, dict):
                onto_dict[from_key] = _deep_merge_dicts(onto_value, from_value, f)
            else:
                onto_dict[from_key] = f(onto_value, from_value)

    return onto_dict


def invert_many_to_many_mapping[K, V](
    many_to_many_mapping: Mapping[K, Set[V]],
) -> dict[V, set[K]]:
    """Invert many-to-many mapping.

    Given a mapping that maps keys to sets of values, this function inverts the
    mapping to create a new dict that maps each value to a set of keys that
    were associated with it in the original mapping.

    Args:
        many_to_many_mapping: A mapping where each key maps to a set of values.

    Returns:
        A dict where each value maps to a set of keys.
    """
    inverted_mapping: dict[V, set[K]] = {}
    for key, values in many_to_many_mapping.items():
        for value in values:
            inverted_mapping.setdefault(value, set()).add(key)
    return inverted_mapping


def optional_as_dict[K, V](key: K, value: V | None) -> dict[K, V]:
    """Returns a dictionary with a single key-value pair if the value is not None.

    If the value is None, returns an empty dictionary.

    Args:
        key: The key for the dictionary.
        value: The value for the dictionary, which may be None.

    Returns:
        A dictionary containing the key-value pair if value is not None,
        otherwise an empty dictionary.

    Examples:
        ```
        {
            "static_field": 1,
            **optional_as_dict("optional_field", 2)
        }
        ```
    """
    return {key: value} if value is not None else {}
