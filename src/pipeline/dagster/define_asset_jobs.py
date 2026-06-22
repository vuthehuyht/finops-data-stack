"""Utilities for creating dagster pipelines."""

from collections.abc import Mapping, Set
from typing import Any, overload

import dagster
from dagster import (
    ConfigMapping,
    ExecutorDefinition,
    HookDefinition,
    PartitionedConfig,
    PartitionsDefinition,
    RetryPolicy,
    RunConfig,
)
from dagster._core.definitions.asset_selection import (
    CoercibleToAssetSelection,
)
from dagster._core.definitions.metadata import RawMetadataValue
from dagster._core.definitions.unresolved_asset_job_definition import (
    UnresolvedAssetJobDefinition,
)

import src.pipeline.dagster.decorators
import src.pipeline.dagster.hooks
from src.pipeline.dagster.k8s import on_k8s


@overload
def define_asset_job(
    name: str,
    *,
    k8s_config: dict[str, Any] | None = ...,
    selection: CoercibleToAssetSelection | None = None,
    config: ConfigMapping
    | Mapping[str, Any]
    | PartitionedConfig
    | RunConfig
    | None = None,
    description: str | None = None,
    tags: Mapping[str, object] | None = None,
    run_tags: Mapping[str, object] | None = None,
    metadata: Mapping[str, RawMetadataValue] | None = None,
    partitions_def: PartitionsDefinition | None = None,
    executor_def: ExecutorDefinition | None = None,
    hooks: Set[HookDefinition] | None = None,
    op_retry_policy: RetryPolicy | None = None,
) -> UnresolvedAssetJobDefinition: ...


@overload
def define_asset_job(name: str, **kwargs: Any) -> UnresolvedAssetJobDefinition: ...


def define_asset_job(name: str, **kwargs: Any) -> UnresolvedAssetJobDefinition:
    """A wrapper of `dagster.define_asset_job` that adds hooks and tags."""
    # Ad-hoc extraction of `tags` from temporary _JobDecorator object.
    arg_tags = kwargs.setdefault("tags", {})
    k8s_config = kwargs.pop("k8s_config", {})
    d = src.pipeline.dagster.decorators._JobDecorator(k8s_config, tags=arg_tags)
    d._prepare_tags(name, False, "tags")
    kwargs["tags"] = d.kwargs["tags"]
    hooks = kwargs.setdefault("hooks", set())
    hooks.add(src.pipeline.dagster.hooks.op_metrics_hook)
    if on_k8s():
        hooks.add(src.pipeline.dagster.hooks.slack_on_op_failure_hook)
    return dagster.define_asset_job(name, **kwargs)
