"""Execute Redshift DDL queries with Jinja2 rendering."""

import argparse
import json
import logging
import sys
from typing import ClassVar

import jinja2

from src.common.redshift_util import get_redshift_connection

logger = logging.getLogger(__name__)


def _render_query(query_template: str, parameters: dict[str, str]) -> str:
    """Render the DDL query using the parameters.

    Args:
        query_template: The SQL template string containing Jinja variables.
        parameters: A dictionary of key-value pairs to substitute.

    Returns:
        The rendered SQL query string.
    """
    return (
        jinja2.Environment(autoescape=True, undefined=jinja2.StrictUndefined)
        .from_string(query_template)
        .render(**parameters)
    )


def _make_concatenated_ddl_query_string(
    input_file_paths: list[str], parameters: dict[str, str]
) -> str:
    """Render DDL queries in the input files and concatenate them.

    Args:
        input_file_paths: List of absolute or relative paths to template files.
        parameters: Dictionary containing template parameter overrides.

    Returns:
        A single string containing all rendered SQL statements separated by semicolons.
    """
    sql_queries: list[str] = []
    for file_path in input_file_paths:
        logger.info("Processing template file: %s", file_path)
        with open(file_path, encoding="utf-8") as f_in:
            rendered_query = _render_query(f_in.read(), parameters)
            # Remove trailing semicolons to prevent syntax errors during concatenation
            cleaned_query = rendered_query.rstrip().rstrip(";")
            if cleaned_query:
                sql_queries.append(cleaned_query)

    return "\n;\n".join(sql_queries)


def confirm_execution(message: str) -> bool:
    """Prompt the user for confirmation before executing queries.

    Args:
        message: The message warning prompting the user.

    Returns:
        True if the user types 'y' or 'yes' (case-insensitive), False otherwise.
    """
    try:
        answer = input(f"{message} [y/N]: ")
        return answer.lower() in ("y", "yes")
    except EOFError:
        # Fallback to False if input stream is closed or non-interactive
        return False


def execute_ddl_queries(
    queries: str,
    skip_confirmation: bool,
    total_files: int,
    parameters: dict[str, str],
) -> None:
    """Execute concatenated DDL query string on Redshift database.

    Args:
        queries: Concatenated query string to run.
        skip_confirmation: Flag to bypass user interactive confirmation.
        total_files: Count of files processed for logging context.
        parameters: Template parameter dictionary for logging metadata.

    Raises:
        ProgrammingError: If database execution encounters an error.
    """
    if not skip_confirmation:
        warning_msg = (
            f"Are you certain you want to execute the {total_files} queries "
            f"with these parameters: {parameters}?"
        )
        if not confirm_execution(warning_msg):
            logger.warning("Execution cancelled by user.")
            sys.exit(1)

    logger.info("Connecting to Redshift database...")
    # Using psycopg2 connection context manager
    with get_redshift_connection() as conn:
        conn.autocommit = False
        with conn.cursor() as cursor:
            try:
                logger.info("Running DDL statements block...")
                # psycopg2 supports executing multiple semicolon-separated statements
                cursor.execute(queries)
                conn.commit()
                logger.info("All DDL statements executed and committed successfully.")
            except Exception as e:
                logger.error("Error executing DDL. Rolling back transaction.")
                conn.rollback()
                raise e


class CLIArgs:
    """Class wrapper for argparse arguments typing."""

    template_parameters: str
    log_level: str
    output_ddl_query_file_path: str | None
    skip_confirmation: bool
    input_ddl_query_template_file_paths: list[str]

    # Required class variable for class attribute reference
    __annotations__: ClassVar = {
        "template_parameters": str,
        "log_level": str,
        "output_ddl_query_file_path": str | None,
        "skip_confirmation": bool,
        "input_ddl_query_template_file_paths": list[str],
    }


def main() -> None:
    """Main entrypoint for DDL execution CLI."""
    parser = argparse.ArgumentParser(description="Execute Redshift DDL queries.")
    parser.add_argument(
        "--template_parameters",
        type=str,
        default="{}",
        help="JSON string map of template parameters.",
    )
    parser.add_argument(
        "--log_level",
        type=str,
        default="INFO",
        choices=["DEBUG", "INFO", "WARNING", "ERROR", "CRITICAL"],
        help="Level of output log messages.",
    )
    parser.add_argument(
        "--output_ddl_query_file_path",
        type=str,
        default=None,
        help="Optional path to output the compiled DDL queries to a file.",
    )
    parser.add_argument(
        "--skip_confirmation",
        default=False,
        action="store_true",
        help="Skip prompt verification prior to executing.",
    )
    parser.add_argument(
        "input_ddl_query_template_file_paths",
        nargs="+",
        help="The paths of the input DDL query files.",
    )

    args = parser.parse_args()
    logging.basicConfig(
        level=args.log_level,
        format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
    )

    parsed_parameters = json.loads(args.template_parameters)
    concatenated_queries = _make_concatenated_ddl_query_string(
        args.input_ddl_query_template_file_paths, parsed_parameters
    )

    if args.output_ddl_query_file_path:
        logger.info("Writing compiled DDLs to: %s", args.output_ddl_query_file_path)
        with open(args.output_ddl_query_file_path, "w", encoding="utf-8") as f_out:
            f_out.write(concatenated_queries)

    execute_ddl_queries(
        queries=concatenated_queries,
        skip_confirmation=args.skip_confirmation,
        total_files=len(args.input_ddl_query_template_file_paths),
        parameters=parsed_parameters,
    )


if __name__ == "__main__":
    main()
