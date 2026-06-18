"""Execute DDL Job."""

import dagster

import src.pipeline.dagster as dagster_lib
from src.dagster import environment, resources


def _create_ddl_op(
    schema: environment.SchemaType,
    region: environment.RegionType,
) -> dagster.OpDefinition:
    @dagster_lib.op(
        name=f"execute_ddl_{schema.value}_{region.value}_op",
        config_schema={"query_template_file_paths": [str]},
    )
    def execute_ddl_op(
        context: dagster.OpExecutionContext,
        redshift: resources.RedshiftResource,
    ) -> None:
        """Execute DDL (Skeleton for Redshift)."""
        query_paths = context.op_config["query_template_file_paths"]
        context.log.info(
            "Executing DDL for %s %s on Redshift with paths: %s",
            schema.value,
            region.value,
            query_paths,
        )
        context.log.info("DDL Execution is skipped in this skeleton.")

    return execute_ddl_op


@dagster_lib.job(
    config=dagster.RunConfig(
        ops={
            "execute_ddl_batch_global_op": {
                "config": {"query_template_file_paths": []}
            },
            "execute_ddl_master_global_op": {
                "config": {"query_template_file_paths": []}
            },
            "execute_ddl_batch_asia_op": {
                "config": {"query_template_file_paths": []}
            },
            "execute_ddl_master_asia_op": {
                "config": {"query_template_file_paths": []}
            },
        },
    ),
)
def execute_ddl_job() -> None:
    """Execute DDL Job."""
    _create_ddl_op(environment.SchemaType.Batch, environment.RegionType.Global)()
    _create_ddl_op(environment.SchemaType.Master, environment.RegionType.Global)()
    _create_ddl_op(environment.SchemaType.Batch, environment.RegionType.Asia)()
    _create_ddl_op(environment.SchemaType.Master, environment.RegionType.Asia)()
