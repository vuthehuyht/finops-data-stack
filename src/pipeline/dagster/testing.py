"""Test utilities for Dagster pipeline testing.

Kept in src/ (not tests/) so other code locations can import this
when writing their own integration tests.
"""

import dagster


def validate_definitions_and_run_configs(defs: dagster.RepositoryDefinition) -> None:
    """Validate definitions and run configs for use in tests."""
    # Raises if any definition is malformed.
    defs.load_all_definitions()
    for job in defs.get_all_jobs():
        # Skip auto-generated asset jobs — they have no runtime config.
        if job.name.startswith("__ASSET_JOB"):
            continue
        if job.run_config is not None:
            dagster.validate_run_config(job, job.run_config)
        elif job.partitioned_config is not None:
            # https://docs.dagster.io/concepts/partitions-schedules-sensors/partitions#testing
            keys = job.partitioned_config.get_partition_keys()
            run_config = job.partitioned_config.get_run_config_for_partition_key(
                keys[0]
            )
            dagster.validate_run_config(job, run_config)
