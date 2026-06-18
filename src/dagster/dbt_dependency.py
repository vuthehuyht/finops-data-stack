"""Utility for function to get the dependency of a dbt asset."""

from __future__ import annotations

import functools
from collections.abc import Iterable, Iterator, Mapping
from dataclasses import dataclass

import dagster

from src.dagster import dbt_assets


@dataclass(frozen=True)
class Node:
    """Node of the dbt asset dependency graph."""

    key: dagster.AssetKey
    upstream: set[dagster.AssetKey]
    downstream: set[dagster.AssetKey]
    unique_id: str | None = None
    materialization_type: str | None = None

    @property
    def is_view(self) -> bool:
        """Check if the node is a view."""
        return self.materialization_type == "view"


@functools.cache
def get_dbt_asset_dependency() -> dict[dagster.AssetKey, Node]:
    """Get the dependency of dbt asset."""
    dbt_assets_def = dbt_assets.get_dbt_asset_dependency()
    upstream = {x.key: {d.asset_key for d in x.deps} for x in dbt_assets_def.specs}
    downstream: dict[dagster.AssetKey, set[dagster.AssetKey]] = {}
    for k, v in upstream.items():
        for dep in v:
            downstream.setdefault(dep, set()).add(k)

    return {
        s.key: Node(
            key=s.key,
            upstream=upstream.get(s.key, set()),
            downstream=downstream.get(s.key, set()),
            unique_id=s.metadata.get("dagster_dbt/unique_id"),
            materialization_type=s.metadata.get("dagster-dbt/materialization_type"),
        )
        for s in dbt_assets_def.specs
    }


def _get_upstream_iterator(
    keys: Iterable[dagster.AssetKey],
    all_assets: Mapping[dagster.AssetKey, Node],
    visited_upstream: dict[dagster.AssetKey, Node],
) -> Iterator[tuple[dagster.AssetKey, Node]]:
    """Get all upstream of a dbt assets."""
    for key in keys:
        if key not in all_assets:
            continue
        node = all_assets[key]
        if key not in visited_upstream:
            visited_upstream[key] = node
            yield (key, node)
            yield from _get_upstream_iterator(
                node.upstream, all_assets, visited_upstream
            )


def get_upstream_iterator(
    keys: Iterable[dagster.AssetKey],
    all_assets: Mapping[dagster.AssetKey, Node],
) -> Iterator[tuple[dagster.AssetKey, Node]]:
    """Get all upstream of a dbt assets."""
    return _get_upstream_iterator(
        keys,
        all_assets,
        visited_upstream={},
    )


def get_upstream(
    keys: Iterable[dagster.AssetKey], all_assets: Mapping[dagster.AssetKey, Node]
) -> dict[dagster.AssetKey, Node]:
    """Get all upstream of a dbt assets."""
    return dict(get_upstream_iterator(keys, all_assets))


def get_downstream(
    keys: Iterable[dagster.AssetKey], all_assets: Mapping[dagster.AssetKey, Node]
) -> dict[dagster.AssetKey, Node]:
    """Get all downstream of a dbt assets."""
    downstream: dict[dagster.AssetKey, Node] = {}
    for key in keys:
        if key not in all_assets:
            continue
        node = all_assets[key]
        downstream[key] = node
        downstream.update(get_downstream(node.downstream, all_assets))
    return downstream


def is_raw_asset(key: dagster.AssetKey) -> bool:
    """Check if the asset is a raw asset."""
    return key.parts[0] == "RAW" or key.parts[0] == "BRONZE"


def is_source_asset(
    key: dagster.AssetKey, all_assets: Mapping[dagster.AssetKey, Node]
) -> bool:
    """Check if the asset is a top level asset."""
    return key not in all_assets


def is_top_level_asset(
    key: dagster.AssetKey, all_assets: Mapping[dagster.AssetKey, Node]
) -> bool:
    """Check if the asset is a top level asset."""
    if key not in all_assets:
        return False
    node = all_assets[key]
    ul = len(node.upstream)
    return ul == 0 or (
        ul == 1 and is_source_asset(next(iter(node.upstream)), all_assets)
    )


def get_execution_plan_from_starts(
    starts: Iterable[dagster.AssetKey],
    all_assets: Mapping[dagster.AssetKey, Node],
    exclude_views: bool = False,
) -> list[set[dagster.AssetKey]]:
    """Get the execution plan from goals."""
    key_to_visit = set(starts)
    step = dict.fromkeys(key_to_visit, 0)
    # Do BFS to create the execution plan
    while key_to_visit:
        key = key_to_visit.pop()
        node = all_assets[key]
        for down_key in node.downstream:
            if down_key in all_assets:
                down_node = all_assets[down_key]
                diff = 1
                if exclude_views and down_node.is_view:
                    diff = 0
                if step.get(down_key, 0) < step[key] + diff:
                    step[down_key] = step[key] + diff
                    key_to_visit.add(down_key)
        if exclude_views and node.is_view:
            del step[key]

    max_step = max(step.values()) if step else 0
    plan = [set[dagster.AssetKey]() for _ in range(max_step + 1)]
    for key, value in step.items():
        plan[value].add(key)
    return plan


def get_execution_plan_from_goals(
    goals: Iterable[dagster.AssetKey],
    all_assets: Mapping[dagster.AssetKey, Node],
    exclude_views: bool = False,
) -> list[set[dagster.AssetKey]]:
    """Get the execution plan from goals."""
    targets = get_upstream(goals, all_assets)
    key_to_visit = {key for key in targets if is_top_level_asset(key, targets)}
    return get_execution_plan_from_starts(key_to_visit, targets, exclude_views)


def get_execution_plan_from_set(
    all_assets: Mapping[dagster.AssetKey, Node],
) -> list[set[dagster.AssetKey]]:
    """Get the execution plan from a set of asset keys."""
    return get_execution_plan_from_starts(all_assets.keys(), all_assets)


@functools.cache
def get_table_name_to_asset_key_mapping() -> dict[str, dagster.AssetKey]:
    """Get a mapping of table names to asset keys."""
    deps = get_dbt_asset_dependency()
    return {key.path[-1]: key for key in deps.keys()}


def get_asset_key_from_table_name(
    table_name: str,
) -> dagster.AssetKey:
    """Get an asset key from a table name."""
    return get_table_name_to_asset_key_mapping()[table_name]
