"""Common Prometheus metrics for pipelines."""

import json
import os
import time
from typing import Any

import prometheus_client
import psutil
from prometheus_client import CollectorRegistry, Gauge

from src.pipeline.dagster.k8s import on_k8s

# For testing.
LOCAL_METRICS_STORAGE: dict[str, dict[str, Any]] = {}


def _add_custom_grouping_key_from_env(grouping_key: dict[str, str]) -> dict[str, str]:
    """Adds additional grouping keys to the given dictionary.

    Args:
        grouping_key (dict): The dictionary to update with the additional
            grouping keys.
        env_var (str, optional): The name of the environment variable that
            holds a JSON-like string representing the additional grouping keys.
            Defaults to 'CUSTOM_GROUPING_KEY_JSON'.

    Returns:
        dict: The updated grouping_key dictionary.
    """
    env_value = os.getenv("DAGSTER_METRICS_CUSTOM_GROUPING_KEY_JSON", "{}")
    return {**grouping_key, **json.loads(env_value)}


# We don't use `dagster_prometheus.prometheus_resource`, since it would
# require us to specify `gateway` every time we define run configs. Instead
# we define our wrapper around prometheus_client that works as a resource.
class PrometheusResource:
    """Wrapper around prometheus client library to interact with pushgateway."""

    # This is our k8s in-cluster URL that points to the k8s service for
    # pushgateway. The URL won't be used in local environment.
    GATEWAY = "http://pushgateway.monitoring.svc.cluster.local:9091"

    def __init__(self) -> None:
        """Initialize the resource."""
        self.registry = CollectorRegistry()
        self.collectors: dict[str, Gauge] = {}

    def gauge(
        self, name: str, description: str, labels: list[str] | None = None
    ) -> Gauge:
        """Create new or get existing gauge collector.

        This is a convenience method that keeps track of registered collectors
        in order to avoid "Duplicated timeseries in CollectorRegistry" error in local
        environment. The error occurs when multiple ops are executed within a single
        worker process, and thus it occurs only in local environment, since in k8s
        each op is executed in its own k8s pod.
        """
        if name in self.collectors:
            return self.collectors[name]
        else:
            g = Gauge(name, description, labels or [], registry=self.registry)
            self.collectors[name] = g
            return g

    def push_to_gateway(self, job_name: str, op_name: str) -> None:
        """Sends accumulated metrics to pushgateway if running in a k8s cluster.

        Store in-memory metrics storage (simple dict for testing purpose) in local
        environment.
        This method sets "dagster_job" and "op" labels as grouping keys of the metrics.
        """
        if on_k8s():
            prometheus_client.push_to_gateway(
                gateway=self.GATEWAY,
                job="dagster",
                registry=self.registry,
                grouping_key=_add_custom_grouping_key_from_env(
                    {
                        "workspace": os.environ["DAGSTER_WORKSPACE_NAME"],
                        # For the label name (which is also used as a grouping key),
                        # we avoid `job` since it'd be masked by the value passed above;
                        # use `dagster_job` instead.
                        "dagster_job": job_name,
                        "op": op_name,
                    }
                ),
            )
        else:
            j = LOCAL_METRICS_STORAGE.get(job_name, {})
            LOCAL_METRICS_STORAGE[job_name] = j
            o = j.get(op_name, {})
            j[op_name] = o
            for k, v in self.collectors.items():
                o[k] = next(iter(v.collect())).samples

    def push_to_gateway_for_sensor(self, sensor_name: str) -> None:
        """Sends accumulated metrics to pushgateway if running in a k8s cluster.

        Store in-memory metrics storage (simple dict for testing purpose) in local
        environment.
        This method sets "sensor_name" label as grouping keys of the metrics.
        """
        if on_k8s():
            prometheus_client.push_to_gateway(
                gateway=self.GATEWAY,
                job="dagster",
                registry=self.registry,
                grouping_key=_add_custom_grouping_key_from_env(
                    {
                        "workspace": os.environ["DAGSTER_WORKSPACE_NAME"],
                        "sensor": sensor_name,
                    }
                ),
            )
        else:
            s = LOCAL_METRICS_STORAGE.get(sensor_name, {})
            LOCAL_METRICS_STORAGE[sensor_name] = s
            for k, v in self.collectors.items():
                s[k] = next(iter(v.collect())).samples


def _add_failure_metrics(prometheus: PrometheusResource, failure: bool) -> None:
    # Note: To get notified when op execution fails, we need to monitor 2 metrics:
    # - `dagster_op_failure` defined here for python-level failures. In this case
    #   corresponding k8s job's status is success.
    # - k8s job metrics (by kube-state-metrics) for k8s job-level failures such as
    #   OOM-kill. When an op executor container is brutally killed, the hook to push
    #   `dagster_op_failure` does not run.
    prometheus.gauge(
        "dagster_op_failure",
        "1 if op execution failed",
    ).set(float(failure))


def _add_sensor_failure_metrics(prometheus: PrometheusResource, failure: bool) -> None:
    prometheus.gauge(
        "dagster_sensor_failure",
        "1 if sensor execution failed",
    ).set(float(failure))


def _container_duration_in_seconds() -> float:
    # Use start time of the toplevel process in the current container.
    # The value does not represent the correct op duration in local environment
    # but we don't care as the incorrect value won't be pushed to pushgateway.
    p = psutil.Process()
    start_time = ([p, *p.parents()])[-1].create_time()
    return time.time() - start_time


def _add_duration_metrics(prometheus: PrometheusResource) -> None:
    prometheus.gauge(
        "dagster_op_duration_seconds",
        "duration of op execution",
    ).set(_container_duration_in_seconds())


def op_metrics_hook_impl(
    prometheus: PrometheusResource,
    job_name: str,
    op_name: str,
    failure: bool,
) -> None:
    """Pushes op metrics to pushgateway."""
    _add_failure_metrics(prometheus, failure)
    _add_duration_metrics(prometheus)
    prometheus.push_to_gateway(job_name, op_name)


def sensor_metrics_hook_impl(
    prometheus: PrometheusResource,
    sensor_name: str,
    failure: bool,
) -> None:
    """Pushes sensor metrics to pushgateway."""
    _add_sensor_failure_metrics(prometheus, failure)
    prometheus.push_to_gateway_for_sensor(sensor_name)
