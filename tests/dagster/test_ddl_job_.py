from src.dagster import ddl_job


def test_ddl_job_definition() -> None:
    """Test ddl_job is defined correctly."""
    assert ddl_job.execute_ddl_job is not None
