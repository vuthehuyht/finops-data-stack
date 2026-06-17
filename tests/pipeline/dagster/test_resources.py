"""Tests for `resources.py`."""

import dagster
from src.pipeline.dagster.metrics import PrometheusResource
from src.pipeline.dagster.resources import prometheus_resource


def test_prometheus_resource_returns_prometheus_resource_instance() -> None:
    result = prometheus_resource(dagster.build_init_resource_context())
    assert isinstance(result, PrometheusResource)


def test_prometheus_resource_returns_fresh_instance_each_call() -> None:
    a = prometheus_resource(dagster.build_init_resource_context())
    b = prometheus_resource(dagster.build_init_resource_context())
    assert a is not b
