"""Utility variables and functions to manage Kubernetes manifests."""

import os
from typing import Any


def default_container_env() -> list[dict[str, Any]]:
    """Get the default container environment variables."""
    # The same set of env vars as specified in
    # https://github.com/flywheel-jp/monorepo/blob/master/kubernetes/manifests/libsonnet/container.libsonnet
    # See the comment there.
    return [
        {
            "name": "AWS_REGION",
            "value": os.environ.get("AWS_REGION", "ap-northeast-1"),
        },
        {
            "name": "AWS_DEFAULT_REGION",
            "value": os.environ.get("AWS_DEFAULT_REGION", "ap-northeast-1"),
        },
        {
            "name": "AWS_STS_REGIONAL_ENDPOINTS",
            "value": "regional",
        },
        {
            "name": "LOG4J_FORMAT_MSG_NO_LOOKUPS",
            "value": "true",
        },
    ]


def default_container_security_context(uid: int) -> dict[str, Any]:
    """Get the default container security context."""
    # Note that, with the current dagster-k8s (0.13.x), `security_context`
    # is kept around as a nested dict (i.e. not converted into k8s model
    # object) and thus fields must be in camelCase.
    return {
        "allowPrivilegeEscalation": False,
        "privileged": False,
        "capabilities": {
            "add": [],
            "drop": ["all"],
        },
        "readOnlyRootFilesystem": True,
        "runAsNonRoot": True,
        "runAsUser": uid,
    }
