import datetime
import json
from collections.abc import Iterable
from typing import Any

import pytest
from dagster_k8s.job import USER_DEFINED_K8S_CONFIG_KEY
from pytest_mock import MockerFixture

from dagster import (
    Field,
    JobDefinition,
    OpDefinition,
    OpExecutionContext,
    RepositoryDefinition,
    RunConfig,
    RunStatusSensorDefinition,
    config_mapping,
    daily_partitioned_config,
)
from src.k8s.manifest.utils import default_container_env
from src.pipeline import dagster as dagster_lib

K8S_CONFIG_FIELDS = {
    "job_spec_config",
    "pod_template_spec_metadata",
    "pod_spec_config",
    "container_config",
}


def _k8s_config(decorated: OpDefinition | JobDefinition) -> dict[str, Any]:
    return json.loads(decorated.tags[USER_DEFINED_K8S_CONFIG_KEY])


def _exec_config(decorated: JobDefinition) -> dict[str, Any]:
    if decorated.partitioned_config is None:
        if decorated.config_mapping is None:
            raise ValueError(
                "JobDefinition must have either config_mapping or partitioned_config"
            )
        default_value = decorated.config_mapping.config_schema.default_value
        return default_value.get("execution", {}).get("config", {})
    else:
        c = decorated.partitioned_config
        k = c.get_partition_keys()[0]
        return (
            c.get_run_config_for_partition_key(k).get("execution", {}).get("config", {})
        )


def _app_label(k8s_config: dict[str, Any]) -> str:
    return k8s_config["pod_template_spec_metadata"]["labels"]["app"]


@dagster_lib.op()
def sample_op(context: OpExecutionContext) -> None:
    context.log.info("hello")


@dagster_lib.job()
def sample_job() -> None:
    sample_op()


def test_op_decorator() -> None:
    assert isinstance(sample_op, OpDefinition)
    assert sample_op.name == "sample_op"
    k8s_config = _k8s_config(sample_op)
    assert set(k8s_config.keys()) == K8S_CONFIG_FIELDS
    assert _app_label(k8s_config) == "dagster-op-sample-op"


@daily_partitioned_config(
    start_date=datetime.datetime(2023, 8, 22),
    timezone="Asia/Tokyo",
)
def _example_daily_schedule_config(
    _start: datetime.datetime, _end: datetime.datetime
) -> dict[str, Any]:
    return {}


@config_mapping(
    config_schema={
        "hoge": Field(str, default_value="hoge"),
        "fuga": Field(str, default_value="fuga"),
    }
)
def shared_config(launchpad_config: dict[str, Any]) -> dict[str, Any]:
    return {}


@pytest.mark.parametrize(
    ("config", "in_k8s"),
    [
        ({}, False),
        ({}, True),
        (RunConfig(), False),
        (RunConfig(), True),
        (_example_daily_schedule_config, False),
        (_example_daily_schedule_config, True),
        (shared_config, False),
        (shared_config, True),
    ],
)
def test_job_decorator(
    mocker: MockerFixture,
    config: Any,
    in_k8s: bool,
) -> None:
    mocker.patch.dict(
        "os.environ",
        {
            "DAGSTER_K8S_PIPELINE_RUN_NAMESPACE": "namespace",
            "DAGSTER_SLACK_API_TOKEN": "token",
        },
    )

    if in_k8s:
        mocker.patch(
            "src.pipeline.dagster.k8s.kubernetes_cluster_name",
            return_value="adastria-staging",
        )

    @dagster_lib.job(config=config)
    def sample_job_for_test() -> None:
        sample_op()

    assert isinstance(sample_job_for_test, JobDefinition)
    assert sample_job_for_test.name == "sample_job_for_test"
    k8s_config = _k8s_config(sample_job_for_test)
    assert set(k8s_config.keys()) == K8S_CONFIG_FIELDS
    assert _app_label(k8s_config) == "dagster-job-sample-job-for-test"
    assert sample_job_for_test.resource_defs.keys() == {
        "io_manager",
    }
    assert _exec_config(sample_job_for_test).get("max_concurrent") is None


def test_job_decorator_with_exec_config() -> None:
    @dagster_lib.job(
        config={
            "execution": {
                "config": {
                    "max_concurrent": 3,
                },
            },
        },
    )
    def job_with_concurrency_setting() -> None:
        pass

    assert _exec_config(job_with_concurrency_setting).get("max_concurrent") == 3


def test_k8s_label_value_truncation() -> None:
    @dagster_lib.op()
    def long_op_name_52_012345678901234567890123456789012345(
        context: OpExecutionContext,
    ) -> None:
        context.log.info("OK")

    k8s_config = _k8s_config(long_op_name_52_012345678901234567890123456789012345)
    assert (
        _app_label(k8s_config)
        == "dagster-op-long-op-name-52-012345678901234567890123456789012345"
    )

    @dagster_lib.op()
    def long_op_name_53_0123456789012345678901234567890123456(
        context: OpExecutionContext,
    ) -> None:
        context.log.info("too long, 1 char will be removed from label")

    k8s_config = _k8s_config(long_op_name_53_0123456789012345678901234567890123456)
    assert (
        _app_label(k8s_config)
        == "dagster-op-long-op-name-53-012345678901234567890123456789012345"
    )

    @dagster_lib.op()
    def long_op_name_ending_with_underscore_0123456789__________(
        context: OpExecutionContext,
    ) -> None:
        context.log.info("too long, trailing underscores are also truncated")

    k8s_config = _k8s_config(long_op_name_ending_with_underscore_0123456789__________)
    assert (
        _app_label(k8s_config)
        == "dagster-op-long-op-name-ending-with-underscore-0123456789"
    )


def _to_names(jobs: Iterable[JobDefinition]) -> list[str]:
    return [job.name for job in jobs]


def test_definitions_noop_in_non_k8s_env() -> None:
    defs1 = dagster_lib.definitions(
        code_location_name="defs1",
        jobs=[sample_job],
    )

    assert isinstance(defs1, RepositoryDefinition)
    assert _to_names(defs1.get_all_jobs()) == _to_names([sample_job])
    assert defs1.get_job(sample_job.name).resource_defs.keys() == {
        "s3",
        "prometheus",
        "io_manager",
    }
    assert defs1.sensor_defs == []


def test_definitions_add_sensor_in_k8s_env(mocker: MockerFixture) -> None:
    # TODO: Update the original implementation.
    mocker.patch(
        "src.pipeline.dagster.hooks.on_k8s",
        return_value=True,
    )

    defs2 = dagster_lib.definitions(
        code_location_name="defs2",
        jobs=[sample_job],
    )

    assert isinstance(defs2, RepositoryDefinition)
    assert _to_names(defs2.get_all_jobs()) == _to_names([sample_job])
    assert defs2.get_job(sample_job.name).resource_defs.keys() == {
        "s3",
        "prometheus",
        "io_manager",
    }
    # The element of `defs2.sensor_defs` is a decorated object,
    # not the original function
    assert len(defs2.sensor_defs) == 1
    assert isinstance(defs2.sensor_defs[0], RunStatusSensorDefinition)


DEFAULT_VOLUMES = [
    {
        # Used by dagster for bookkeeping
        "name": "dagster-home",
        "emptyDir": {},
    },
    {
        # For our op implementations
        "name": "tmp",
        "emptyDir": {},
    },
]


DEFAULT_VOLUME_MOUNTS = [
    {
        "name": "dagster-home",
        "mountPath": "/opt/dagster/dagster_home",
    },
    {
        "name": "tmp",
        # nosec
        "mountPath": "/tmp",  # noqa: S108
    },
]


DEFAULT_ENV_VARS = [
    *default_container_env(),
    {
        "name": "DAGSTER_K8S_PIPELINE_RUN_NAMESPACE",
        "valueFrom": {
            "fieldRef": {
                "fieldPath": "metadata.namespace",
            },
        },
    },
    {
        "name": "DAGSTER_SLACK_API_TOKEN",
        "valueFrom": {
            "secretKeyRef": {
                "name": "dagster-slack-api-token",
                "key": "token",
            },
        },
    },
]


USER_DEFINED_VOLUMES = [
    {
        "name": "user-defined-volume",
        "secret": {"secretName": "user-defined-secret"},
    },
]


USER_DEFINED_VOLUME_MOUNTS = [
    {
        "name": "user-defined-volume",
        "mountPath": "/user-defined-mount-point",
    },
]


USER_DEFINED_ENV_VARS = [
    {
        "name": "USER_DEFINED_ENV_VAR",
        "value": "some-value",
    },
]


@dagster_lib.op(
    k8s_config={
        "pod_spec_config": {
            "volumes": USER_DEFINED_VOLUMES,
        },
        "container_config": {
            "env": USER_DEFINED_ENV_VARS,
            "volume_mounts": USER_DEFINED_VOLUME_MOUNTS,
        },
    }
)
def deep_merge_dict_test_op(context: OpExecutionContext) -> None:
    context.log.info("hello")


def test_deep_merge_dicts() -> None:
    def sort(list: Any) -> Any:
        return sorted(list, key=lambda x: x.get("name"))

    k8s_config = _k8s_config(deep_merge_dict_test_op)
    assert sort(k8s_config["pod_spec_config"]["volumes"]) == sort(
        DEFAULT_VOLUMES + USER_DEFINED_VOLUMES
    )
    assert sort(k8s_config["container_config"]["volume_mounts"]) == sort(
        DEFAULT_VOLUME_MOUNTS + USER_DEFINED_VOLUME_MOUNTS
    )
    assert sort(k8s_config["container_config"]["env"]) == sort(
        DEFAULT_ENV_VARS + USER_DEFINED_ENV_VARS
    )
