"""Utilities for creating dagster pipelines."""

import os
from collections.abc import Iterable, Mapping, MutableMapping
from typing import Any, cast

import prometheus_client
from dagster import (
    AssetsDefinition,
    AssetSpec,
    JobDefinition,
    RepositoryDefinition,
    ScheduleDefinition,
    SensorDefinition,
    SourceAsset,
    create_repository_using_definitions_args,
    multiprocess_executor,
)
from dagster._core.definitions.assets.definition.cacheable_assets_definition import (
    CacheableAssetsDefinition,
)
from dagster._core.definitions.partitions.partitioned_schedule import (
    UnresolvedPartitionedAssetScheduleDefinition,
)
from dagster._core.definitions.repository_definition import (
    SINGLETON_REPOSITORY_NAME,
)
from dagster._core.definitions.unresolved_asset_job_definition import (
    UnresolvedAssetJobDefinition,
)
from dagster_aws.s3 import S3Resource
from dagster_k8s import k8s_job_executor

import src.pipeline.dagster.hooks
import src.pipeline.dagster.metrics
from src.pipeline.dagster.k8s import io_manager, on_k8s
from src.pipeline.dagster.resources import prometheus_resource


def definitions(
    code_location_name: str,
    repository_name: str | None = None,
    assets: Iterable[
        AssetsDefinition | SourceAsset | CacheableAssetsDefinition | AssetSpec
    ]
    | None = None,
    schedules: Iterable[
        ScheduleDefinition | UnresolvedPartitionedAssetScheduleDefinition
    ]
    | None = None,
    sensors: Iterable[SensorDefinition] | None = None,
    jobs: Iterable[JobDefinition | UnresolvedAssetJobDefinition] | None = None,
    resources: Mapping[str, Any] | None = None,
    asset_checks: Iterable[AssetsDefinition] | None = None,
) -> RepositoryDefinition:
    """Creates a dagster repository with some auxiliary resources.

    Note: `code_location_name` is necessary in order to correctly set s3 prefix for
    the IO manager. DAGSTER_WORKSPACE_NAME env var is available most of the time,
    but when materializing ad-hoc asset selection from dagit, it's not available.
    This results in an error stating that the previously-created assets are not found.

    Note: `repository_name` is a workaround for migrating from `@repository` decorator
    without changing the repository name to keep job history.
    Newly created repositories should not use this argument.
    """
    # `DAGSTER_WORKSPACE_NAME` should be present on dagster workspace pods
    # when running in k8s.
    if os.getenv("DAGSTER_WORKSPACE_NAME") is not None:
        prometheus_client.start_http_server(8000)
    # Set io_manager resource with the given `code_location_name` for non-asset jobs;
    # resources for assets are set below.
    # TODO: Since dagster 1.3.0, `resources` argument passed to `Definitions()` are
    #       automatically bound to jobs ( https://docs.dagster.io/migration#migrating-to-130
    #       ). Try to streamline the resource definitions once we upgrade to >=1.3.0.
    for job in jobs or []:
        # `UnresolvedAssetsJobDefinition` does not have `resource_defs` and instead
        # uses ones passed as `resources` argument of `Definitions()`.
        if hasattr(job, "resource_defs"):
            resources_defs = cast("JobDefinition", job).resource_defs
            if isinstance(resources_defs, MutableMapping):
                resources_defs["io_manager"] = io_manager(code_location_name)
            else:
                raise TypeError(
                    "Expected `resource_defs` to be a MutableMapping,"
                    f" got {type(resources_defs)}"
                )
    # create_repository_using_definitions_args()
    # ( https://docs.dagster.io/_apidocs/definitions#dagster.create_repository_using_definitions_args
    # ) is an alternative to Definitions()
    # ( https://docs.dagster.io/_apidocs/definitions#dagster.Definitions )
    # that allows us to set repository name manually.
    return create_repository_using_definitions_args(
        name=repository_name or SINGLETON_REPOSITORY_NAME,
        # `assets` does not accept `AssetSpec` by its type hint but supported.
        assets=cast("Any", assets),
        schedules=schedules,
        sensors=src.pipeline.dagster.hooks.append_slack_on_job_failure_sensor_if_on_k8s(
            list(sensors or []),
            # For an unknown reason, the sensor responds to jobs in other repositories
            # without `monitored_jobs`.
            monitored_jobs=list(jobs or []),
        ),
        jobs=jobs,
        # Default resources for assets; resources for non-asset jobs are set above.
        resources={
            "io_manager": io_manager(code_location_name),
            "s3": S3Resource(),
            "prometheus": prometheus_resource,
            **(resources or {}),
        },
        executor=k8s_job_executor if on_k8s() else multiprocess_executor,
        asset_checks=asset_checks,
    )
