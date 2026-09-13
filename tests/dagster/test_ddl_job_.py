from src.dagster import ddl_job


def test_ddl_job_definition() -> None:
    """Test ddl_job is defined correctly."""
    assert ddl_job.execute_ddl_job is not None


def test_ddl_job_seeds_sector_mapping_after_ddl() -> None:
    """The seed must be downstream of successful DDL execution."""
    node_names = {node.name for node in ddl_job.execute_ddl_job.graph.node_defs}
    assert node_names == {"execute_raw_layer_ddl_op", "seed_sector_mapping_op"}

    seed_node = next(
        node
        for node in ddl_job.execute_ddl_job.graph.node_defs
        if node.name == "seed_sector_mapping_op"
    )
    assert "ddl_complete" in seed_node.input_defs[0].name
