#
# Monkey-patch dagster-k8s to allow dynamic configuration of pod's service_account_name
#
# - For multi-tenant workloads, we want to use the same ops/jobs for different tenants,
#   while restricting AWS permissions of each op execution, so as not to accidentally
#   write to other tenant's data.
# - Current dagster-k8s does not support dynamic configuration of op executor pods.
#   The supported way to specify a k8s serviceaccount for an op execution is to specify
#   it in the k8s config passed to the op decorator (i.e. in the source code).
# - Here we modify `K8sStepHandler.launch_step()` method so that we can specify
#   each op's serviceaccount name in job run-level tags.
#
"""Module initializer."""

import json
from collections.abc import Iterator, MutableMapping
from typing import Any

try:
    from dagster_k8s.executor import K8sStepHandler
    from dagster_k8s.job import USER_DEFINED_K8S_CONFIG_KEY

    import dagster
    from dagster._core.executor.step_delegating import StepHandlerContext
    from src.common import dict_util

    def launch_step(
        self: K8sStepHandler,
        step_handler_context: StepHandlerContext,
    ) -> Iterator[dagster.DagsterEvent]:
        def extract_k8s_config_override(op_name: str) -> dict[str, Any]:
            run_tags = step_handler_context.dagster_run.tags
            k8s_config = run_tags.get(f"k8s-config-override/{op_name}", "{}")
            return json.loads(k8s_config)

        def merge_k8s_configs(existing_config: str, new_config: dict[str, Any]) -> str:
            """Merge the existing and new k8s configs."""
            existing_config_dict = json.loads(existing_config)
            merged_config_dict = dict_util.deep_merge_dicts(
                existing_config_dict, new_config, dict_util.list_concat_merger
            )
            return json.dumps(merged_config_dict)

        # `step_tags` là dict dạng `{step_name: tags_dict}`.
        # Với k8s executor chỉ có 1 cặp.
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
    # dagster-k8s chỉ cần trên môi trường K8s production, không cần cho local dev/test
    pass

#
# End of monkey-patch
#


from src.pipeline.dagster.decorators import (  # noqa: E402
    asset,
    asset_check,
    dbt_assets,
    job,
    multi_asset,
    multi_asset_check,
    op,
)
from src.pipeline.dagster.define_asset_jobs import define_asset_job  # noqa: E402
from src.pipeline.dagster.definitions import (  # noqa: E402
    definitions,
)
from src.pipeline.dagster.k8s import (  # noqa: E402
    kubernetes_cluster_name,
    on_k8s,
)
from src.pipeline.dagster.testing import (  # noqa: E402
    validate_definitions_and_run_configs,
)
from src.pipeline.dagster.utils import (  # noqa: E402
    asset_key,
    fetch_materializations,
)

__all__ = [
    "asset",
    "asset_check",
    "asset_key",
    "dbt_assets",
    "define_asset_job",
    "definitions",
    "fetch_materializations",
    "job",
    "kubernetes_cluster_name",
    "multi_asset",
    "multi_asset_check",
    "on_k8s",
    "op",
    "validate_definitions_and_run_configs",
]
