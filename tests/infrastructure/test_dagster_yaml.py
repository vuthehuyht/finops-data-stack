"""Schema-validation test for infrastructure/helm/dagster.yaml.

Loads the file through Dagster's own instance-config loader so any
mistyped key or malformed value for a configured class (PostgresRunStorage,
K8sRunLauncher, QueuedRunCoordinator, S3ComputeLogManager) is caught,
without needing a live Postgres or Kubernetes connection.
"""

from pathlib import Path

from dagster._core.instance.config import dagster_instance_config

DAGSTER_HELM_DIR = Path(__file__).resolve().parents[2] / "infrastructure" / "helm"


def test_dagster_yaml_loads_and_matches_expected_config():
    config, custom_instance_class = dagster_instance_config(str(DAGSTER_HELM_DIR))

    assert custom_instance_class is None

    assert config["run_storage"]["module"] == "dagster_postgres.run_storage"
    assert config["run_storage"]["class"] == "PostgresRunStorage"
    assert config["event_log_storage"]["module"] == "dagster_postgres.event_log"
    assert config["event_log_storage"]["class"] == "PostgresEventLogStorage"
    assert config["schedule_storage"]["module"] == "dagster_postgres.schedule_storage"
    assert config["schedule_storage"]["class"] == "PostgresScheduleStorage"

    for storage_key in ("run_storage", "event_log_storage", "schedule_storage"):
        pg_db = config[storage_key]["config"]["postgres_db"]
        assert pg_db["hostname"] == {"env": "DAGSTER_PG_HOST"}
        assert pg_db["username"] == {"env": "DAGSTER_PG_USERNAME"}
        assert pg_db["password"] == {"env": "DAGSTER_PG_PASSWORD"}
        assert pg_db["db_name"] == {"env": "DAGSTER_PG_DB"}
        assert pg_db["port"] == 5432


def test_dagster_yaml_run_launcher_targets_worker_spot_nodes():
    config, _ = dagster_instance_config(str(DAGSTER_HELM_DIR))

    run_launcher = config["run_launcher"]
    assert run_launcher["module"] == "dagster_k8s"
    assert run_launcher["class"] == "K8sRunLauncher"

    launcher_config = run_launcher["config"]
    assert launcher_config["service_account_name"] == "dagster-sa"
    assert launcher_config["job_namespace"] == "dagster"
    assert launcher_config["load_incluster_config"] is True
    assert launcher_config["job_image"] == {"env": "DAGSTER_CURRENT_IMAGE"}
    assert launcher_config["node_selector"] == {"node-group": "worker"}
    assert launcher_config["tolerations"] == [
        {
            "key": "workload",
            "operator": "Equal",
            "value": "spot",
            "effect": "NoSchedule",
        }
    ]


def test_dagster_yaml_run_coordinator_caps_concurrency_at_worker_node_max():
    config, _ = dagster_instance_config(str(DAGSTER_HELM_DIR))

    run_coordinator = config["run_coordinator"]
    assert run_coordinator["module"] == "dagster.core.run_coordinator"
    assert run_coordinator["class"] == "QueuedRunCoordinator"
    assert run_coordinator["config"]["max_concurrent_runs"] == 3


def test_dagster_yaml_compute_logs_use_processed_data_lake_bucket():
    config, _ = dagster_instance_config(str(DAGSTER_HELM_DIR))

    compute_logs = config["compute_logs"]
    assert compute_logs["module"] == "dagster_aws.s3.compute_log_manager"
    assert compute_logs["class"] == "S3ComputeLogManager"
    assert compute_logs["config"]["bucket"] == "finops-data-lake-processed"
    assert compute_logs["config"]["prefix"] == "dagster-compute-logs"
    assert compute_logs["config"]["region"] == "ap-southeast-1"


def test_dagster_yaml_telemetry_disabled():
    config, _ = dagster_instance_config(str(DAGSTER_HELM_DIR))

    assert config["telemetry"]["enabled"] is False
