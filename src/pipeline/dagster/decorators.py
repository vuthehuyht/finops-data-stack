"""Improved decorators for dagster entity."""

import abc
import os
import random
from collections.abc import Callable, Iterable, Mapping, Sequence, Set
from typing import Any, overload

import dagster_dbt
import dagster_dbt.dbt_manifest
from dagster_aws.s3 import S3Resource
from dagster_k8s import k8s_job_executor
from dagster_k8s.job import USER_DEFINED_K8S_CONFIG_KEY
from dagster_k8s.utils import sanitize_k8s_label

import dagster
from dagster import (
    AssetCheckKey,
    AssetCheckResult,
    AssetChecksDefinition,
    AssetCheckSpec,
    AssetIn,
    AssetKey,
    AssetOut,
    AssetsDefinition,
    AssetSpec,
    AutomationCondition,
    BackfillPolicy,
    ConfigMapping,
    DagsterType,
    ExecutorDefinition,
    FreshnessPolicy,
    HookDefinition,
    In,
    JobDefinition,
    LoggerDefinition,
    OpDefinition,
    Out,
    PartitionedConfig,
    PartitionsDefinition,
    RetryPolicy,
    RunConfig,
    SourceAsset,
    multiprocess_executor,
)
from dagster._config import UserConfigSchema
from dagster._core.definitions.assets.definition.asset_dep import CoercibleToAssetDep
from dagster._core.definitions.assets.job.asset_in import CoercibleToAssetIn
from dagster._core.definitions.events import (
    CoercibleToAssetKey,
    CoercibleToAssetKeyPrefix,
)
from dagster._core.definitions.metadata import (
    ArbitraryMetadataMapping,
    RawMetadataValue,
)
from src.common.dict_util import deep_merge_dicts, list_concat_merger
from src.k8s.manifest.utils import (
    default_container_env,
    default_container_security_context,
)
from src.pipeline.dagster.hooks import (
    op_metrics_hook,
    slack_on_op_failure_hook,
)
from src.pipeline.dagster.k8s import io_manager, on_k8s
from src.pipeline.dagster.resources import prometheus_resource


class _DecoratorBase(abc.ABC):
    def __init__(self, k8s_config: dict[str, Any] | None, **kwargs: Any) -> None:
        self.k8s_config = k8s_config
        self.kwargs = kwargs

    def _prepare_tags(self, fn_name: str, op: bool, tags_key: str) -> None:
        tags = self.kwargs.setdefault(tags_key, {})
        tags[USER_DEFINED_K8S_CONFIG_KEY] = self._k8s_config(fn_name, op)
        if not op:
            limit_given = any(
                k.startswith("limit_concurrent_job_runs_to_") for k in tags.keys()
            )
            if not limit_given:
                tags["limit_concurrent_job_runs_to_5"] = "default"

    def _k8s_config(self, fn_name: str, op: bool) -> dict[str, Any]:
        default_config = self._default_k8s_config(fn_name, op)
        # Remove `service_account_name` if the value is `None` (which is the case
        # for job coordinator pods).
        if default_config["pod_spec_config"]["service_account_name"] is None:
            del default_config["pod_spec_config"]["service_account_name"]
        return deep_merge_dicts(
            default_config,
            self.k8s_config or {},
            list_concat_merger,
        )

    def _default_k8s_config(self, fn_name: str, op: bool) -> dict[str, Any]:
        uid = random.randint(68000, 68999)
        # Note that we can dynamically override part of the values using job run-level
        # tags (i.e. `RunRequest` tags) in the form of
        # - key: `k8s-config-override/{op_name}`
        # - value: `{json_of_k8s_config}`
        return {
            "job_spec_config": {
                # For op-level timeout. By default, a k8s job that is executing a
                # dagster op for longer than 12 hours is terminated. This timeout is
                # helpful for terminating hanged op executions so that other concurrent
                # job runs can be started.
                # Ops that could run longer than 12h must explicitly specify a longer
                # value.
                # Note that job coordinator pods does not have timeout.
                "active_deadline_seconds": 12 * 60 * 60 if op else None,
                # TTL of automatic removal of completed jobs. We use a shorter TTL than
                # the default of dagster-k8s (1 day) so that components that watch k8s
                # jobs/pods (e.g. monitoring components) do not have to keep track of
                # completed jobs/pods for 1 day (backfills could spawn many number of
                # k8s jobs).
                "ttl_seconds_after_finished": 3600,
            },
            "pod_template_spec_metadata": {
                "labels": self._k8s_labels(fn_name),
                "annotations": {
                    # Security hardening of the spawned containers.
                    "seccomp.security.alpha.kubernetes.io/pod": "runtime/default",
                    # Tell cluster-autoscaler not to terminate a node where a dagster
                    # job/op pod is running:
                    # https://github.com/kubernetes/autoscaler/blob/master/cluster-autoscaler/FAQ.md#what-types-of-pods-can-prevent-ca-from-removing-a-node
                    "cluster-autoscaler.kubernetes.io/safe-to-evict": "false",
                },
            },
            "pod_spec_config": {
                # Per-step k8s serviceaccount. The default serviceaccount comes
                # with minimal IAM permissions to manipulate S3 objects (for logs and io
                # managers).
                # Override this default when op needs additional AWS permissions.
                "service_account_name": self._default_k8s_service_account(),
                # Volumes for temporary files. Most ops should be fine with the default.
                "volumes": [
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
                ],
            },
            "container_config": {
                # Note that we cannot specify `image` here; it'd result in
                # `got multiple values for keyword argument 'image'` error.
                # Default container resource specifications.
                # It's recommended to override them by concrete ops.
                "resources": {
                    "requests": {
                        "cpu": "100m",
                        "memory": "800Mi",
                        "ephemeral-storage": "1Gi",
                    },
                    "limits": {
                        "memory": "800Mi",
                        "ephemeral-storage": "1Gi",
                    },
                },
                # To attach a specific key-value pair in a k8s secret as container's
                # environment variable. Note the camelCased dict keys.
                "env": default_container_env()
                + self._dagster_ws_name_envs()
                + [
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
                    # Example: attach a key in a k8s secret as an env var
                    # {
                    #     "name": "ENV_VAR_NAME",
                    #     "valueFrom": {
                    #         "secretKeyRef": {
                    #             "name": "some-secret-name",
                    #             "key": "key_in_the_specified_k8s_secret",
                    #         },
                    #     },
                    # },
                ],
                # To attach all key-value pairs in a k8s secret as container's
                # environment variables.
                # "env_from": [
                #     {
                #         "secret_ref": {
                #             "name": "some-secret-name",
                #         },
                #     },
                # ],
                # Volume mounts for temporary files. Most ops should be fine with the
                # default.
                "volume_mounts": [
                    {
                        "name": "dagster-home",
                        "mountPath": "/opt/dagster/dagster_home",
                    },
                    {
                        "name": "tmp",
                        # nosec
                        "mountPath": "/tmp",  # noqa: S108
                    },
                ],
                # Security hardening for the pods. Should not be specified in each op.
                "security_context": default_container_security_context(uid),
            },
        }

    def _dagster_ws_name_envs(self) -> list[dict[str, str]]:
        # Though `DAGSTER_WORKSPACE_NAME` is always present when evaluating this
        # function in k8s environment, skip adding the env var if it's not present
        # during e.g. tests.
        dagster_ws_name = os.getenv("DAGSTER_WORKSPACE_NAME")
        return (
            []
            if dagster_ws_name is None
            else [
                {
                    "name": "DAGSTER_WORKSPACE_NAME",
                    "value": dagster_ws_name,
                },
            ]
        )

    @abc.abstractmethod
    def _k8s_labels(self, fn_name: str) -> dict[str, str]:
        raise NotImplementedError()

    @abc.abstractmethod
    def _default_k8s_service_account(self) -> str | None:
        raise NotImplementedError()


class _OpDecorator(_DecoratorBase):
    def __call__(self, op_fn: Callable[..., Any]) -> OpDefinition:
        self._prepare_tags(op_fn.__name__, True, "tags")
        original_decorator_fn = dagster.op(**self.kwargs)
        return original_decorator_fn(op_fn)

    def _k8s_labels(self, fn_name: str) -> dict[str, str]:
        return {
            # `app` label is for better naming of cloudwatch log groups.
            "app": sanitize_k8s_label(f"dagster-op-{fn_name.replace('_', '-')}"),
        }

    def _default_k8s_service_account(self) -> str | None:
        return "dagster-op-executor-default"


@overload
def op(
    *,
    k8s_config: dict[str, Any] | None = None,
    compute_fn: Callable[..., Any] | None = None,
    name: str | None = None,
    description: str | None = None,
    ins: Mapping[str, In] | None = None,
    out: Out | Mapping[str, Out] | None = None,
    config_schema: UserConfigSchema | None = None,
    required_resource_keys: Set[str] | None = None,
    tags: Mapping[str, Any] | None = None,
    version: str | None = None,
    retry_policy: RetryPolicy | None = None,
    code_version: str | None = None,
    pool: str | None = None,
) -> Callable[[Callable[..., Any]], OpDefinition]: ...


@overload
def op(
    *,
    k8s_config: dict[str, Any] | None = None,
    **kwargs: Any,
) -> Callable[[Callable[..., Any]], OpDefinition]: ...


def op(
    *,
    k8s_config: dict[str, Any] | None = None,
    **kwargs: Any,
) -> Callable[[Callable[..., Any]], OpDefinition]:
    """Wrapper around `dagster.op` that fills default k8s configurations.

    When dagster is running on k8s, each dagster job run is executed in a dedicated k8s
    job. The job in turn spawns k8s jobs for each op execution.

    See the implementation of `_DecoratorBase._default_k8s_config()` for the expected
    data structure of `k8s_config`.
    """
    return _OpDecorator(k8s_config, **kwargs)


class _AssetDecorator(_DecoratorBase):
    def __call__(self, asset_fn: Callable[..., Any]) -> AssetsDefinition:
        self._prepare_tags(asset_fn.__name__, True, "op_tags")
        original_decorator_fn: Any = dagster.asset(**self.kwargs)
        return original_decorator_fn(asset_fn)

    def _k8s_labels(self, fn_name: str) -> dict[str, str]:
        return {
            # `app` label is for better naming of cloudwatch log groups.
            "app": sanitize_k8s_label(f"dagster-asset-{fn_name.replace('_', '-')}"),
        }

    def _default_k8s_service_account(self) -> str | None:
        return "dagster-op-executor-default"


@overload
def asset(
    *,
    k8s_config: dict[str, Any] | None = None,
    compute_fn: Callable[..., Any] | None = None,
    name: str | None = None,
    key_prefix: CoercibleToAssetKeyPrefix | None = None,
    ins: Mapping[str, AssetIn] | None = None,
    deps: Iterable[CoercibleToAssetDep] | None = None,
    metadata: ArbitraryMetadataMapping | None = None,
    tags: Mapping[str, str] | None = None,
    description: str | None = None,
    config_schema: UserConfigSchema | None = None,
    required_resource_keys: Set[str] | None = None,
    resource_defs: Mapping[str, object] | None = None,
    hooks: Set[HookDefinition] | None = None,
    io_manager_def: object | None = None,
    io_manager_key: str | None = None,
    dagster_type: DagsterType | None = None,
    partitions_def: PartitionsDefinition | None = None,
    op_tags: Mapping[str, Any] | None = None,
    group_name: str | None = None,
    output_required: bool = True,
    automation_condition: AutomationCondition[Any] | None = None,
    freshness_policy: FreshnessPolicy | None = None,
    backfill_policy: BackfillPolicy | None = None,
    retry_policy: RetryPolicy | None = None,
    code_version: str | None = None,
    key: CoercibleToAssetKey | None = None,
    check_specs: Sequence[AssetCheckSpec] | None = None,
    owners: Sequence[str] | None = None,
    kinds: Set[str] | None = None,
    pool: str | None = None,
    **kwargs: Any,
) -> Callable[[Callable[..., Any]], AssetsDefinition]: ...


@overload
def asset(
    *, k8s_config: dict[str, Any] | None = None, **kwargs: Any
) -> Callable[[Callable[..., Any]], AssetsDefinition]: ...


def asset(
    *, k8s_config: dict[str, Any] | None = None, **kwargs: Any
) -> Callable[[Callable[..., Any]], AssetsDefinition]:
    """Wrapper around `dagster.asset` that fills default k8s configurations.

    When dagster is running on k8s, each dagster job run is executed in a dedicated k8s
    job. The job in turn spawns k8s jobs for each asset execution.

    See the implementation of `_DecoratorBase._default_k8s_config()` for the expected
    data structure of `k8s_config`.

    This function is experimental. Note that, unlike op API, asset API does not support
    op hooks. Consider using op decorator in combination with AssetMaterialization
    (i.e., materialize assets at runtime without declaring beforehand).
    """
    return _AssetDecorator(k8s_config, **kwargs)


class _MultiAssetDecorator(_AssetDecorator):
    def __call__(self, asset_fn: Callable[..., Any]) -> AssetsDefinition:
        self._prepare_tags(asset_fn.__name__, True, "op_tags")
        original_decorator_fn: Any = dagster.multi_asset(**self.kwargs)
        return original_decorator_fn(asset_fn)


@overload
def multi_asset(
    *,
    k8s_config: dict[str, Any] | None,
    outs: Mapping[str, AssetOut] | None = None,
    name: str | None = None,
    ins: Mapping[str, AssetIn] | None = None,
    deps: Iterable[CoercibleToAssetDep] | None = None,
    description: str | None = None,
    config_schema: UserConfigSchema | None = None,
    required_resource_keys: Set[str] | None = None,
    internal_asset_deps: Mapping[str, set[AssetKey]] | None = None,
    partitions_def: PartitionsDefinition | None = None,
    hooks: Set[HookDefinition] | None = None,
    backfill_policy: BackfillPolicy | None = None,
    op_tags: Mapping[str, Any] | None = None,
    can_subset: bool = False,
    resource_defs: Mapping[str, object] | None = None,
    group_name: str | None = None,
    retry_policy: RetryPolicy | None = None,
    code_version: str | None = None,
    specs: Sequence[AssetSpec] | None = None,
    check_specs: Sequence[AssetCheckSpec] | None = None,
    pool: str | None = None,
    **kwargs: Any,
) -> Callable[[Callable[..., Any]], AssetsDefinition]: ...


@overload
def multi_asset(
    *, k8s_config: dict[str, Any] | None = None, **kwargs: Any
) -> Callable[[Callable[..., Any]], AssetsDefinition]: ...


def multi_asset(
    *, k8s_config: dict[str, Any] | None = None, **kwargs: Any
) -> Callable[[Callable[..., Any]], AssetsDefinition]:
    """Wrapper around `dagster.multi_asset` that fills default k8s configurations.

    When dagster is running on k8s, each dagster job run is executed in a dedicated k8s
    job. The job in turn spawns k8s jobs for each asset execution.

    See the implementation of `_DecoratorBase._default_k8s_config()` for the expected
    data structure of `k8s_config`.

    This function is experimental. Note that, unlike op API, asset API does not
    support op hooks. Consider using op decorator in combination with
    AssetMaterialization (i.e., materialize assets at runtime without declaring
    beforehand).
    """
    return _MultiAssetDecorator(k8s_config, **kwargs)


class _AssetCheckDecorator(_AssetDecorator):
    def __call__(self, asset_fn: Callable[..., Any]) -> AssetChecksDefinition:
        self._prepare_tags(asset_fn.__name__, True, "op_tags")
        original_decorator_fn: Any = dagster.asset_check(**self.kwargs)
        return original_decorator_fn(asset_fn)


@overload
def asset_check(
    *,
    k8s_config: dict[str, Any] | None,
    asset: CoercibleToAssetKey | AssetsDefinition | SourceAsset,
    name: str | None = None,
    description: str | None = None,
    blocking: bool = False,
    additional_ins: Mapping[str, AssetIn] | None = None,
    additional_deps: Iterable[CoercibleToAssetDep] | None = None,
    required_resource_keys: set[str] | None = None,
    resource_defs: Mapping[str, object] | None = None,
    config_schema: UserConfigSchema | None = None,
    compute_kind: str | None = None,
    op_tags: Mapping[str, Any] | None = None,
    retry_policy: RetryPolicy | None = None,
    metadata: Mapping[str, Any] | None = None,
    automation_condition: AutomationCondition[AssetCheckKey] | None = None,
    pool: str | None = None,
) -> Callable[[Callable[..., AssetCheckResult]], AssetChecksDefinition]: ...


@overload
def asset_check(
    *, k8s_config: dict[str, Any] | None = None, **kwargs: Any
) -> Callable[[Callable[..., Any]], AssetChecksDefinition]: ...


def asset_check(
    *, k8s_config: dict[str, Any] | None = None, **kwargs: Any
) -> Callable[[Callable[..., Any]], AssetChecksDefinition]:
    """Wrapper around `dagster.asset_check` that fills default k8s configurations.

    When dagster is running on k8s, each dagster job run is executed in a dedicated k8s
    job. The job in turn spawns k8s jobs for each asset execution.

    See the implementation of `_DecoratorBase._default_k8s_config()` for the expected
    data structure of `k8s_config`.

    This function is experimental. Note that, unlike op API, asset API does not
    support op hooks. Consider using op decorator in combination with
    AssetMaterialization (i.e., materialize assets at runtime without declaring
    beforehand).
    """
    return _AssetCheckDecorator(k8s_config, **kwargs)


class _MultiAssetCheckDecorator(_AssetCheckDecorator):
    def __call__(self, asset_fn: Callable[..., Any]) -> AssetChecksDefinition:
        self._prepare_tags(asset_fn.__name__, True, "op_tags")
        original_decorator_fn: Any = dagster.multi_asset_check(**self.kwargs)
        return original_decorator_fn(asset_fn)


@overload
def multi_asset_check(
    *,
    k8s_config: dict[str, Any] | None,
    name: str | None = None,
    specs: Sequence[AssetCheckSpec],
    description: str | None = None,
    can_subset: bool = False,
    compute_kind: str | None = None,
    op_tags: Mapping[str, Any] | None = None,
    resource_defs: Mapping[str, object] | None = None,
    required_resource_keys: set[str] | None = None,
    retry_policy: RetryPolicy | None = None,
    config_schema: UserConfigSchema | None = None,
    ins: Mapping[str, CoercibleToAssetIn] | None = None,
    pool: str | None = None,
) -> Callable[[Callable[..., Any]], AssetChecksDefinition]: ...


@overload
def multi_asset_check(
    *, k8s_config: dict[str, Any] | None = None, **kwargs: Any
) -> Callable[[Callable[..., Any]], AssetChecksDefinition]: ...


def multi_asset_check(
    *, k8s_config: dict[str, Any] | None = None, **kwargs: Any
) -> Callable[[Callable[..., Any]], AssetChecksDefinition]:
    """Wrapper around `dagster.asset_check` that fills default k8s configurations.

    When dagster is running on k8s, each dagster job run is executed in a dedicated k8s
    job. The job in turn spawns k8s jobs for each asset execution.

    See the implementation of `_DecoratorBase._default_k8s_config()` for the expected
    data structure of `k8s_config`.

    This function is experimental. Note that, unlike op API, asset API does not
    support op hooks. Consider using op decorator in combination with
    AssetMaterialization (i.e., materialize assets at runtime without declaring
    beforehand).
    """
    return _MultiAssetCheckDecorator(k8s_config, **kwargs)


class _DbtAssetDecorator(_MultiAssetDecorator):
    def __call__(self, asset_fn: Callable[..., Any]) -> AssetsDefinition:
        self._prepare_tags(asset_fn.__name__, True, "op_tags")
        original_decorator_fn: Any = dagster_dbt.dbt_assets(**self.kwargs)
        return original_decorator_fn(asset_fn)


@overload
def dbt_assets(
    *,
    k8s_config: dict[str, Any] | None,
    manifest: dagster_dbt.dbt_manifest.DbtManifestParam,
    select: str = "fqn:*",
    exclude: str | None = "",
    selector: str | None = "",
    name: str | None = None,
    io_manager_key: str | None = None,
    partitions_def: PartitionsDefinition | None = None,
    dagster_dbt_translator: dagster_dbt.DagsterDbtTranslator | None = None,
    backfill_policy: BackfillPolicy | None = None,
    op_tags: Mapping[str, Any] | None = None,
    required_resource_keys: set[str] | None = None,
    project: dagster_dbt.DbtProject | None = None,
    retry_policy: RetryPolicy | None = None,
    pool: str | None = None,
) -> Callable[..., AssetsDefinition]: ...


@overload
def dbt_assets(
    *, k8s_config: dict[str, Any] | None = None, **kwargs: Any
) -> Callable[[Callable[..., Any]], AssetsDefinition]: ...


def dbt_assets(
    *, k8s_config: dict[str, Any] | None = None, **kwargs: Any
) -> Callable[[Callable[..., Any]], AssetsDefinition]:
    """Wrapper around `dagster.multi_asset` that fills default k8s configurations.

    When dagster is running on k8s, each dagster job run is executed in a dedicated k8s
    job. The job in turn spawns k8s jobs for each asset execution.

    See the implementation of `_DecoratorBase._default_k8s_config()` for the expected
    data structure of `k8s_config`.

    This function is experimental. Note that, unlike op API, asset API does not support
    op hooks. Consider using op decorator in combination with AssetMaterialization
    (i.e., materialize assets at runtime without declaring beforehand).
    """
    return _DbtAssetDecorator(k8s_config, **kwargs)


class _JobDecorator(_DecoratorBase):
    def __call__(self, job_fn: Callable[..., Any]) -> JobDefinition:
        name = job_fn.__name__
        hooks = self.kwargs.setdefault("hooks", set())
        hooks.add(op_metrics_hook)
        if on_k8s():
            hooks.add(slack_on_op_failure_hook)
        self._prepare_executor_config()

        # When `resource_defs` is set in a job, it validates that all resources
        # required by its ops are in `resource_defs` at which JobDefinition is
        # build even though required resources will be set in `units.definition()`
        # later. Set default resources here only when it is given explicitly
        # as an argument of the decorator.
        if "resource_defs" in self.kwargs:
            self._prepare_resource_defs(name)
        self._prepare_tags(name, False, "tags")
        original_decorator_fn = dagster.job(**self.kwargs)
        return original_decorator_fn(job_fn)

    def _prepare_executor_config(self) -> None:
        self.kwargs["executor_def"] = (
            k8s_job_executor if on_k8s() else multiprocess_executor
        )
        config = self.kwargs.setdefault("config", {})
        if isinstance(config, PartitionedConfig):
            self.kwargs["config"] = self._wrap_partitioned_config(config)
        elif isinstance(config, RunConfig):
            d: dict[str, Any] = config.to_config_dict()
            self._set_k8s_execution_config(d)
            self.kwargs["config"] = d
        elif isinstance(config, ConfigMapping):
            config_fn = config.config_fn

            def _wrapped_config_mapping(_input: dict[str, Any]) -> dict[str, Any]:
                d = config_fn(_input)
                self._set_k8s_execution_config(d)
                return d

            self.kwargs["config"] = ConfigMapping(
                config_fn=_wrapped_config_mapping,
                config_schema=config.config_schema,
                receive_processed_config_values=config.receive_processed_config_values,
            )
        else:
            self._set_k8s_execution_config(config)

    def _set_k8s_execution_config(self, config: Mapping[str, Any]) -> None:
        if not isinstance(config, dict):
            raise ValueError(f"Expected a dict for run config but got {type(config)}")
        if on_k8s():
            config["execution"] = deep_merge_dicts(
                {
                    "config": {
                        "job_namespace": os.environ[
                            "DAGSTER_K8S_PIPELINE_RUN_NAMESPACE"
                        ],
                    },
                },
                config.get("execution", {}),
                list_concat_merger,
            )

    def _wrap_partitioned_config[T_str: str](
        self, config: PartitionedConfig[dagster.PartitionsDefinition[T_str]]
    ) -> PartitionedConfig[dagster.PartitionsDefinition[T_str]]:
        def wrapped_run_config_fn(partition_key: str) -> Mapping[str, Any]:
            d = config.get_run_config_for_partition_key(partition_key)
            self._set_k8s_execution_config(d)
            return d

        return PartitionedConfig(
            partitions_def=config.partitions_def,
            run_config_for_partition_key_fn=wrapped_run_config_fn,
        )

    def _prepare_resource_defs(self, name: str) -> None:
        resource_defs = self.kwargs.setdefault("resource_defs", {})
        resource_defs["s3"] = S3Resource()
        resource_defs["prometheus"] = prometheus_resource
        # This is a dummy io_manager which will be overridden in `utils.definitions()`.
        # This is added to avoid `KeyError: 's3'` on resources initialization when
        # starting a job, even if we have the s3 resource above. Specifying an s3-backed
        # io manager here somehow resolves the issue. This seems to be a bug in dagster
        # core, so we should revisit this when we upgrade dagster.
        resource_defs["io_manager"] = io_manager("to_be_overridden")

    def _k8s_labels(self, fn_name: str) -> dict[str, str]:
        return {
            # `app` label is for better naming of cloudwatch log groups.
            "app": sanitize_k8s_label(f"dagster-job-{fn_name.replace('_', '-')}"),
        }

    def _default_k8s_service_account(self) -> str | None:
        return None


@overload
def job(
    *,
    k8s_config: dict[str, Any] | None = None,
    name: str | None = None,
    description: str | None = None,
    resource_defs: Mapping[str, object] | None = None,
    config: ConfigMapping
    | Mapping[str, Any]
    | RunConfig
    | PartitionedConfig
    | None = None,
    tags: Mapping[str, str] | None = None,
    run_tags: Mapping[str, str] | None = None,
    metadata: Mapping[str, RawMetadataValue] | None = None,
    logger_defs: Mapping[str, LoggerDefinition] | None = None,
    executor_def: ExecutorDefinition | None = None,
    hooks: Set[HookDefinition] | None = None,
    op_retry_policy: RetryPolicy | None = None,
    partitions_def: PartitionsDefinition | None = None,
    input_values: Mapping[str, object] | None = None,
    owners: Sequence[str] | None = None,
) -> Callable[[Callable[..., Any]], JobDefinition]: ...


@overload
def job(
    *,
    k8s_config: dict[str, Any] | None = None,
    **kwargs: Any,
) -> Callable[[Callable[..., Any]], JobDefinition]: ...


def job(
    *,
    k8s_config: dict[str, Any] | None = None,
    **kwargs: Any,
) -> Callable[[Callable[..., Any]], JobDefinition]:
    """Wrapper around `dagster.job` that fills default configurations.

    When dagster is running on k8s, each dagster job run is executed in a dedicated k8s
    job.
    The job in turn launches k8s jobs for each op execution.

    Args:
      k8s_config: Optional configurations for the launcher pod. Usually users of this
        decorator do not have to specify anything.
      **kwargs: Keyword arguments to be passed to `dagster.job`.
    """
    return _JobDecorator(k8s_config, **kwargs)
