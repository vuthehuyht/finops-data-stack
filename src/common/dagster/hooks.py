"""
Dagster execution hooks for FinOps Data Stack.
"""

from dagster import HookContext, failure_hook


@failure_hook(name="finops_failure_hook")
def finops_failure_hook(context: HookContext) -> None:
    """A standard hook that catches op/asset failures and prints details.

    Can be extended to post alerts to Slack or Telegram when deployed on Cloud.
    """
    op_name = context.op.name
    error = context.op_exception
    context.log.error(
        "Pipeline Error Alert!\n"
        f"Operation: {op_name} failed.\n"
        f"Error Details: {str(error)}"
    )
