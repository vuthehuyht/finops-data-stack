"""Common hooks for dagster pipelines."""

import os
import traceback
from collections.abc import Sequence
from typing import Any

from dagster import (
    DagsterEvent,
    HookContext,
    HookDefinition,
    JobDefinition,
    RunFailureSensorContext,
)
from dagster._core.definitions.events import HookExecutionResult
from dagster._core.definitions.unresolved_asset_job_definition import (
    UnresolvedAssetJobDefinition,
)
from dagster_slack import make_slack_on_run_failure_sensor
from slack_sdk.web.client import WebClient

from src.pipeline.dagster.k8s import kubernetes_cluster_name, on_k8s
from src.pipeline.dagster.metrics import op_metrics_hook_impl

# The max length of text field in section block is 3000:
# https://api.slack.com/reference/block-kit/blocks#section
# This limit is for codeblock parts in messages.
_SLACK_CODEBLOCK_MAX_LENGTH = 2800
_SLACK_BLOCK_MAX_LENGTH = 2900


def _job_run_url(run_id: str) -> str:
    c = kubernetes_cluster_name()
    if c is None:
        raise Exception("Not running within a Kubernetes cluster")
    path = f"/runs/{run_id}"
    # Other deployments don't have public endpoints for the dagster web UI;
    # we need to make a port-forward connection to the k8s pod.
    # We use different local ports for prod and other environments.
    if c.endswith("-prod"):
        port = 3001
    else:
        port = 3000
    return f"http://localhost:{port}{path}"


def _truncate(text: str, max_length: int) -> str:
    if len(text) > max_length:
        half = max_length // 2
        text = "\n".join(
            [
                text[:half],
                f"... ({len(text) - max_length} characters truncated)",
                text[-half:],
            ]
        )
    return text


def _slack_codeblock_text(text: str) -> str:
    return "```" + _truncate(text, _SLACK_CODEBLOCK_MAX_LENGTH) + "```"


def _slack_section_block(text: str) -> dict[str, Any]:
    return {
        "type": "section",
        "text": {
            "type": "mrkdwn",
            "text": _truncate(text, _SLACK_BLOCK_MAX_LENGTH),
        },
    }


def _slack_blocks_fn(context: RunFailureSensorContext) -> list[dict[str, Any]]:
    run = context.dagster_run
    return [
        _slack_section_block(
            f"@channel Job <{_job_run_url(run.run_id)}|{run.job_name} failed>!"
        ),
        _slack_section_block(
            _slack_codeblock_text(f"Error: {context.failure_event.message}"),
        ),
    ]


def append_slack_on_job_failure_sensor_if_on_k8s(
    list: list[Any],
    monitored_jobs: None | (Sequence[JobDefinition | UnresolvedAssetJobDefinition]),
) -> list[Any]:
    """Utility function to add a pipeline failure sensor that posts messages to slack.

    Destination channel of the messages is: `dagster-{k8s_cluster_name}`.
    """
    # TODO: Update the original implementation.
    if not on_k8s():
        return list
    else:
        cluster = kubernetes_cluster_name()
        sensor = make_slack_on_run_failure_sensor(
            channel=f"#dagster-{cluster}",
            # Note that this code is evaluated at workspace pods.
            slack_token=os.getenv("DAGSTER_SLACK_API_TOKEN"),  # type: ignore[arg-type]
            blocks_fn=_slack_blocks_fn,
            monitored_jobs=monitored_jobs,
        )
        return [*list, sensor]


def _op_failure_message_blocks(context: HookContext) -> list[dict[str, Any]]:
    e = context.op_exception
    return [
        _slack_section_block(
            "\n".join(
                [
                    (
                        f"<{_job_run_url(context.run_id)}"
                        f"|Op {context.op.name} in job {context.job_name} failed!>"
                    ),
                    f"{type(e).__name__}: {e!s}",
                ]
            ),
        ),
        _slack_section_block(
            _slack_codeblock_text(
                "".join(
                    traceback.format_exception(type(e), e, e.__traceback__)
                    if e is not None
                    else []
                ).rstrip(),
            ),
        ),
    ]


def _notify_op_failure_of_slack(context: HookContext) -> None:
    cluster = kubernetes_cluster_name()
    if cluster is None:
        return
    slack = WebClient(os.getenv("DAGSTER_SLACK_API_TOKEN"))
    slack.chat_postMessage(
        channel=f"#dagster-{cluster}",
        attachments=[
            {
                "blocks": _op_failure_message_blocks(context),
            },
        ],
    )


def _slack_on_op_failure_hook(
    context: HookContext, events: list[DagsterEvent]
) -> HookExecutionResult:
    step_failure = any(e.is_step_failure for e in events)
    if not step_failure:
        return HookExecutionResult(
            hook_name="slack_on_op_failure_hook", is_skipped=True
        )
    _notify_op_failure_of_slack(context)
    return HookExecutionResult(hook_name="slack_on_op_failure_hook", is_skipped=False)


slack_on_op_failure_hook = HookDefinition(
    name="slack_on_op_failure_hook",
    hook_fn=_slack_on_op_failure_hook,
    decorated_fn=_slack_on_op_failure_hook,
)


def _op_metrics_hook(
    context: HookContext, events: list[DagsterEvent]
) -> HookExecutionResult:
    # When an op execution is failed and is marked as "up for retry", its status is not
    # yet determined (neither success nor failure). In such a case we don't have any
    # useful metrics; don't push to pushgateway.
    step_success = any(e.is_step_success for e in events)
    step_failure = any(e.is_step_failure for e in events)
    if not step_success and not step_failure:
        return HookExecutionResult(hook_name="op_metrics_hook", is_skipped=True)
    op_metrics_hook_impl(
        context.resources.prometheus,
        context.job_name,
        context.step_key,
        step_failure,
    )
    return HookExecutionResult(hook_name="op_metrics_hook", is_skipped=False)


op_metrics_hook = HookDefinition(
    name="op_metrics_hook",
    hook_fn=_op_metrics_hook,
    decorated_fn=_op_metrics_hook,
    required_resource_keys={"prometheus"},
)
