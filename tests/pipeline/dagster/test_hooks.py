import os
import unittest.mock

import pytest

from src.pipeline.dagster import hooks


@pytest.mark.parametrize(
    [
        "kubernetes_cluster_name",
        "expected",
    ],
    [
        pytest.param(
            "test-staging",
            "http://localhost:3000/runs/test_job_id",
            id="Staging Kubernetes cluster",
        ),
        pytest.param(
            "test-prod",
            "http://localhost:3001/runs/test_job_id",
            id="Production Kubernetes cluster",
        ),
    ],
)
def test_job_run_url(kubernetes_cluster_name: str, expected: str) -> None:
    with unittest.mock.patch.dict(
        os.environ,
        {
            "KUBERNETES_CLUSTER_NAME": kubernetes_cluster_name,
        },
    ):
        assert "KUBERNETES_CLUSTER_NAME" in os.environ
        assert hooks._job_run_url("test_job_id") == expected


def test_job_run_url_exception() -> None:
    with unittest.mock.patch.dict(
        os.environ,
        {},
        clear=True,
    ):
        assert "KUBERNETES_CLUSTER_NAME" not in os.environ
        with pytest.raises(
            Exception, match="^Not running within a Kubernetes cluster$"
        ):
            hooks._job_run_url("test_job_id")
