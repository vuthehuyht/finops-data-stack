import os

from src.pipeline.dagster.metrics import (
    PrometheusResource,
    _add_custom_grouping_key_from_env,
    op_metrics_hook_impl,
    sensor_metrics_hook_impl,
)


def test_op_metrics_hook_impl() -> None:
    prometheus = PrometheusResource()
    assert prometheus.collectors == {}
    op_metrics_hook_impl(prometheus, "test_job", "test_op", True)
    assert "dagster_op_failure" in prometheus.collectors
    assert "dagster_op_duration_seconds" in prometheus.collectors


def test_sensor_metrics_hook_impl() -> None:
    prometheus = PrometheusResource()
    assert prometheus.collectors == {}
    sensor_metrics_hook_impl(prometheus, "test_sensor", True)
    assert "dagster_sensor_failure" in prometheus.collectors


def test_add_grouping_key_with_valid_json() -> None:
    try:
        # Set up test data
        os.environ["DAGSTER_METRICS_CUSTOM_GROUPING_KEY_JSON"] = (
            '{"partner": "fw-partner", "project": "fw-project"}'
        )
        grouping_key = {
            "workspace": "myworkspace",
            "dagster_job": "myjob",
            "op": "myop",
        }

        # Call the function with the test data
        grouping_key = _add_custom_grouping_key_from_env(grouping_key)

        # Check that the grouping_key was updated as expected
        expected_grouping_key = {
            "workspace": "myworkspace",
            "dagster_job": "myjob",
            "op": "myop",
            "partner": "fw-partner",
            "project": "fw-project",
        }
        assert grouping_key == expected_grouping_key
    finally:
        del os.environ["DAGSTER_METRICS_CUSTOM_GROUPING_KEY_JSON"]


def test_add_grouping_key_with_missing_env_var() -> None:
    # Set up test data
    grouping_key = {"workspace": "myworkspace", "dagster_job": "myjob", "op": "myop"}
    assert _add_custom_grouping_key_from_env(grouping_key) == grouping_key
