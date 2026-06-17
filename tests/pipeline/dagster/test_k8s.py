"""Tests for `k8s.py`."""

import os
import unittest.mock

import pytest

import dagster
from src.pipeline.dagster import k8s


def test_kubernetes_cluster_name_when_env_set() -> None:
    with unittest.mock.patch.dict(
        os.environ, {"KUBERNETES_CLUSTER_NAME": "adastria-staging"}
    ):
        assert k8s.kubernetes_cluster_name() == "adastria-staging"


def test_kubernetes_cluster_name_when_env_unset() -> None:
    with unittest.mock.patch.dict(os.environ, {}, clear=True):
        assert "KUBERNETES_CLUSTER_NAME" not in os.environ
        assert k8s.kubernetes_cluster_name() is None


def test_on_k8s_returns_true_when_cluster_set() -> None:
    with unittest.mock.patch.dict(
        os.environ, {"KUBERNETES_CLUSTER_NAME": "adastria-prod"}
    ):
        assert k8s.on_k8s() is True


def test_on_k8s_returns_false_when_cluster_unset() -> None:
    with unittest.mock.patch.dict(os.environ, {}, clear=True):
        assert k8s.on_k8s() is False


@pytest.mark.parametrize(
    ["cluster", "expected_bucket"],
    [
        pytest.param("adastria-staging", "fw-dagster-adastria-staging", id="staging"),
        pytest.param("adastria-prod", "fw-dagster-adastria-prod", id="prod"),
    ],
)
def test_io_manager_bucket_name_known_clusters(
    cluster: str, expected_bucket: str
) -> None:
    with unittest.mock.patch.dict(os.environ, {"KUBERNETES_CLUSTER_NAME": cluster}):
        assert k8s._io_manager_bucket_name() == expected_bucket


def test_io_manager_bucket_name_raises_when_not_on_k8s() -> None:
    with unittest.mock.patch.dict(os.environ, {}, clear=True):
        with pytest.raises(Exception, match="Not running within a Kubernetes cluster"):
            k8s._io_manager_bucket_name()


def test_io_manager_bucket_name_raises_for_unknown_cluster() -> None:
    with unittest.mock.patch.dict(
        os.environ, {"KUBERNETES_CLUSTER_NAME": "unknown-cluster"}
    ):
        with pytest.raises(Exception, match="Unsupported k8s cluster"):
            k8s._io_manager_bucket_name()


def test_io_manager_returns_fs_io_manager_when_not_on_k8s() -> None:
    with unittest.mock.patch.dict(os.environ, {}, clear=True):
        result = k8s.io_manager("my-location")
        assert result is dagster.fs_io_manager


def test_io_manager_returns_s3_manager_when_on_k8s() -> None:
    with unittest.mock.patch.dict(
        os.environ, {"KUBERNETES_CLUSTER_NAME": "adastria-staging"}
    ):
        result = k8s.io_manager("my-location")
        # S3-backed IO manager is a configured instance, not the base fs_io_manager
        assert result is not dagster.fs_io_manager
