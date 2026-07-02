"""Schema-validation test for infrastructure/helm/dagster.yaml.

Two layers of validation, both required:

1. `dagster_instance_config` loads the file and confirms its top-level
   structure (module/class/config per section) parses.
2. `validate_config` checks each section's `config` dict against the real
   `config_type()` of the class it configures (PostgresRunStorage,
   K8sRunLauncher, QueuedRunCoordinator, S3ComputeLogManager). Layer 1
   alone accepts unknown or missing keys inside `config` — it does not
   catch a config key the target class doesn't define. Layer 2 does,
   without needing a live Postgres or Kubernetes connection.
"""

from pathlib import Path

from dagster._config import validate_config
from dagster._core.instance.config import dagster_instance_config
from dagster._core.run_coordinator import QueuedRunCoordinator
from dagster_aws.s3.compute_log_manager import S3ComputeLogManager
from dagster_k8s import K8sRunLauncher
from dagster_postgres import (
    PostgresEventLogStorage,
    PostgresRunStorage,
    PostgresScheduleStorage,
)

DAGSTER_HELM_DIR = Path(__file__).resolve().parents[2] / "infrastructure" / "helm"


def _load_config():
    config, _ = dagster_instance_config(str(DAGSTER_HELM_DIR))
    return config


def _assert_schema_valid(config_type, config_value):
    result = validate_config(config_type, config_value)
    assert result.success, [error.message for error in result.errors]


def test_dagster_yaml_postgres_storage_matches_schema_and_uses_env_indirection():
    config = _load_config()

    assert config["run_storage"]["module"] == "dagster_postgres.run_storage"
    assert config["run_storage"]["class"] == "PostgresRunStorage"
    assert config["event_log_storage"]["module"] == "dagster_postgres.event_log"
    assert config["event_log_storage"]["class"] == "PostgresEventLogStorage"
    assert config["schedule_storage"]["module"] == "dagster_postgres.schedule_storage"
    assert config["schedule_storage"]["class"] == "PostgresScheduleStorage"

    storage_classes = {
        "run_storage": PostgresRunStorage,
        "event_log_storage": PostgresEventLogStorage,
        "schedule_storage": PostgresScheduleStorage,
    }
    for storage_key, storage_class in storage_classes.items():
        storage_config = config[storage_key]["config"]
        _assert_schema_valid(storage_class.config_type(), storage_config)

        pg_db = storage_config["postgres_db"]
        assert pg_db["hostname"] == {"env": "DAGSTER_PG_HOST"}
        assert pg_db["username"] == {"env": "DAGSTER_PG_USERNAME"}
        assert pg_db["password"] == {"env": "DAGSTER_PG_PASSWORD"}
        assert pg_db["db_name"] == {"env": "DAGSTER_PG_DB"}
        assert pg_db["port"] == 5432


def test_dagster_yaml_run_launcher_matches_schema_and_targets_worker_spot_nodes():
    config = _load_config()

    run_launcher = config["run_launcher"]
    assert run_launcher["module"] == "dagster_k8s"
    assert run_launcher["class"] == "K8sRunLauncher"

    launcher_config = run_launcher["config"]
    _assert_schema_valid(K8sRunLauncher.config_type(), launcher_config)

    assert launcher_config["service_account_name"] == "dagster-sa"
    assert launcher_config["job_namespace"] == "dagster"
    assert launcher_config["load_incluster_config"] is True
    assert launcher_config["job_image"] == {"env": "DAGSTER_CURRENT_IMAGE"}
    assert launcher_config["instance_config_map"] == "dagster-instance-config-map"

    pod_spec_config = launcher_config["run_k8s_config"]["pod_spec_config"]
    assert pod_spec_config["node_selector"] == {"node-group": "worker"}
    assert pod_spec_config["tolerations"] == [
        {
            "key": "spotWorker",
            "operator": "Equal",
            "value": "true",
            "effect": "NoSchedule",
        }
    ]


def test_dagster_yaml_run_coordinator_matches_schema_and_caps_concurrency_at_node_max():
    config = _load_config()

    run_coordinator = config["run_coordinator"]
    assert run_coordinator["module"] == "dagster.core.run_coordinator"
    assert run_coordinator["class"] == "QueuedRunCoordinator"

    coordinator_config = run_coordinator["config"]
    _assert_schema_valid(QueuedRunCoordinator.config_type(), coordinator_config)
    assert coordinator_config["max_concurrent_runs"] == 3


def test_dagster_yaml_compute_logs_matches_schema_and_uses_processed_data_lake_bucket():
    config = _load_config()

    compute_logs = config["compute_logs"]
    assert compute_logs["module"] == "dagster_aws.s3.compute_log_manager"
    assert compute_logs["class"] == "S3ComputeLogManager"

    compute_logs_config = compute_logs["config"]
    _assert_schema_valid(S3ComputeLogManager.config_type(), compute_logs_config)
    assert compute_logs_config["bucket"] == "finops-data-lake-processed"
    assert compute_logs_config["prefix"] == "dagster-compute-logs"
    assert compute_logs_config["region"] == "ap-southeast-1"


def test_dagster_yaml_telemetry_disabled():
    config = _load_config()

    assert config["telemetry"]["enabled"] is False
