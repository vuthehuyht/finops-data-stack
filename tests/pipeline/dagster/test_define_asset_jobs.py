from src.pipeline import dagster as dagster_lib


def test_define_asset_job_set_concurrent_limit_by_default() -> None:
    job = dagster_lib.define_asset_job("test")
    assert job.tags is not None
    assert job.tags["dagster-k8s/config"] is not None
    assert job.tags["limit_concurrent_job_runs_to_5"] == "default"


def test_define_asset_job() -> None:
    job = dagster_lib.define_asset_job(
        "test", tags={"limit_concurrent_job_runs_to_1": "my_pipeline", "other": "tag"}
    )
    assert job.tags is not None
    assert job.tags["dagster-k8s/config"] is not None
    assert job.tags["limit_concurrent_job_runs_to_1"] == "my_pipeline"
    # Does not set the limit if already set.
    assert "limit_concurrent_job_runs_to_5" not in job.tags
    assert job.tags["other"] == "tag"
