"""K8s infrastructure utilities and Dagster StepHandler monkey-patch.

Single point of truth for all dagster-k8s and AWS dependencies.
Imported by __init__.py to ensure the monkey-patch runs when the package is loaded.
"""

import json
import os
from collections.abc import Iterator, MutableMapping
from typing import Any

from dagster_aws.s3 import s3_pickle_io_manager

import dagster
from src.common import dict_util

try:
    from dagster_k8s.executor import K8sStepHandler
    from dagster_k8s.job import USER_DEFINED_K8S_CONFIG_KEY

    from dagster._core.executor.step_delegating import StepHandlerContext

    def launch_step(
        self: K8sStepHandler,
        step_handler_context: StepHandlerContext,
    ) -> Iterator[dagster.DagsterEvent]:
        def extract_k8s_config_override(op_name: str) -> dict[str, Any]:
            run_tags = step_handler_context.dagster_run.tags
            k8s_config = run_tags.get(f"k8s-config-override/{op_name}", "{}")
            return json.loads(k8s_config)

        def merge_k8s_configs(existing_config: str, new_config: dict[str, Any]) -> str:
            existing_config_dict = json.loads(existing_config)
            merged_config_dict = dict_util.deep_merge_dicts(
                existing_config_dict, new_config, dict_util.list_concat_merger
            )
            return json.dumps(merged_config_dict)

        # `step_tags` is a dict of the form `{step_name: tags_dict}`.
        # With the k8s executor there is only ever one pair.
        [(op_name, op_tags)] = step_handler_context.step_tags.items()
        k8s_config = extract_k8s_config_override(op_name)
        if k8s_config and USER_DEFINED_K8S_CONFIG_KEY in op_tags:
            if isinstance(op_tags, MutableMapping):
                op_tags[USER_DEFINED_K8S_CONFIG_KEY] = merge_k8s_configs(
                    op_tags[USER_DEFINED_K8S_CONFIG_KEY],
                    k8s_config,
                )
            else:
                raise TypeError(
                    f"Expected `op_tags` to be a MutableMapping, got {type(op_tags)}"
                )
        yield from self.launch_step_orig(step_handler_context)  # type: ignore[attr-defined]

    K8sStepHandler.launch_step_orig = K8sStepHandler.launch_step  # type: ignore[attr-defined]
    K8sStepHandler.launch_step = launch_step  # type: ignore[method-assign]
except ImportError:
    # dagster-k8s only required in K8s production, not local dev/test
    pass


def kubernetes_cluster_name() -> str | None:
    """Name of the k8s cluster. Return `None` when not running on k8s."""
    return os.getenv("KUBERNETES_CLUSTER_NAME")


def on_k8s() -> bool:
    """Return true if running on k8s."""
    return kubernetes_cluster_name() is not None


def _io_manager_bucket_name() -> str:
    cluster = kubernetes_cluster_name()
    if cluster is None:
        raise Exception("Not running within a Kubernetes cluster")
    elif cluster == "adastria-staging":
        return "fw-dagster-adastria-staging"
    elif cluster == "adastria-prod":
        return "fw-dagster-adastria-prod"
    raise Exception(f"Unsupported k8s cluster: {cluster}")


def io_manager(code_location_name: str) -> dagster.IOManagerDefinition:
    """Get the corresponding IO manager."""
    if on_k8s():
        return s3_pickle_io_manager.configured(
            {
                "s3_bucket": _io_manager_bucket_name(),
                "s3_prefix": f"io_manager/{code_location_name}",
            }
        )
    else:
        return dagster.fs_io_manager
