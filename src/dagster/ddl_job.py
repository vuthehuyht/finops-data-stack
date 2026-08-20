"""Execute DDL Job."""

import glob
import os

import dagster

import src.pipeline.dagster as dagster_lib
from src.redshift import ddl_executor


@dagster_lib.op(
    name="execute_raw_layer_ddl_op",
    config_schema={
        "schema_name_raw": dagster.Field(str, default_value="raw", is_required=False),
        "schema_name_staging": dagster.Field(
            str, default_value="staging", is_required=False
        ),
        "schema_name_mart": dagster.Field(str, default_value="mart", is_required=False),
    },
    k8s_config={
        "container_config": {
            "resources": {
                "requests": {"memory": "512Mi"},
                "limits": {"memory": "1Gi"},
            }
        }
    },
)
def execute_ddl_op(context: dagster.OpExecutionContext) -> None:
    """Execute DDL for Raw Layer on Redshift."""
    schema_name_raw = context.op_config["schema_name_raw"]
    schema_name_staging = context.op_config["schema_name_staging"]
    schema_name_mart = context.op_config["schema_name_mart"]

    parameters = {
        "schema_name_raw": schema_name_raw,
        "schema_name_staging": schema_name_staging,
        "schema_name_mart": schema_name_mart,
    }

    base_dir = os.path.join(os.path.dirname(__file__), "..", "redshift")
    setup_file = os.path.join(base_dir, "ddl", "dev", "setup.sql.jinja")
    raw_files = glob.glob(os.path.join(base_dir, "ddl", "raw", "*.sql.jinja"))

    input_files = [setup_file] + raw_files
    context.log.info(f"Rendering {len(input_files)} DDL templates...")

    file_queries = ddl_executor._render_ddl_queries(input_files, parameters)

    context.log.info("Executing DDL queries...")

    from src.common.redshift_util import get_redshift_connection

    with get_redshift_connection() as conn:
        conn.autocommit = False
        with conn.cursor() as cursor:
            try:
                for file_path, sql in file_queries:
                    context.log.info(f"Executing: {os.path.basename(file_path)}")
                    cursor.execute(sql)
                conn.commit()
                context.log.info(
                    "All DDL statements executed and committed successfully."
                )
            except Exception as e:
                context.log.error(
                    f"Error executing DDL from {file_path}. Rolling back."
                )
                conn.rollback()
                raise e


@dagster_lib.job(
    config=dagster.RunConfig(
        ops={
            "execute_raw_layer_ddl_op": {
                "config": {
                    "schema_name_raw": "raw",
                    "schema_name_staging": "staging",
                    "schema_name_mart": "mart",
                }
            }
        }
    ),
)
def execute_ddl_job() -> None:
    """Execute DDL Job."""
    execute_ddl_op()
