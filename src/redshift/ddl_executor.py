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


def _render_ddl_queries(
    input_file_paths: list[str], parameters: dict[str, str]
) -> list[tuple[str, str]]:
    """Render DDL templates and return (file_path, sql) pairs.

    Args:
        input_file_paths: List of absolute or relative paths to template files.
        parameters: Dictionary containing template parameter overrides.

    Returns:
        List of (file_path, rendered_sql) tuples, one per input file.
    """
    result: list[tuple[str, str]] = []
    for file_path in input_file_paths:
        logger.info("Processing template file: %s", file_path)
        with open(file_path, encoding="utf-8") as f_in:
            rendered_query = _render_query(f_in.read(), parameters)
            if rendered_query.strip():
                result.append((file_path, rendered_query))
    return result


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
    file_queries: list[tuple[str, str]],
    skip_confirmation: bool,
    parameters: dict[str, str],
) -> None:
    """Execute rendered DDL queries file-by-file on Redshift database.

    Args:
        file_queries: List of (file_path, rendered_sql) tuples to execute in order.
        skip_confirmation: Flag to bypass user interactive confirmation.
        parameters: Template parameter dictionary for logging metadata.

    Raises:
        ProgrammingError: If database execution encounters an error.
    """
    if not skip_confirmation:
        warning_msg = (
            f"Are you certain you want to execute {len(file_queries)} files "
            f"with these parameters: {parameters}?"
        )
        if not confirm_execution(warning_msg):
            logger.warning("Execution cancelled by user.")
            sys.exit(1)

    logger.info("Connecting to Redshift database...")
    with get_redshift_connection() as conn:
        conn.autocommit = False
        with conn.cursor() as cursor:
            try:
                for file_path, sql in file_queries:
                    logger.info("Executing: %s", file_path)
                    cursor.execute(sql)
                conn.commit()
                logger.info("All DDL statements executed and committed successfully.")
            except Exception as e:
                logger.error(
                    "Error executing DDL from %s. Rolling back transaction.", file_path
                )
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
    file_queries = _render_ddl_queries(
        args.input_ddl_query_template_file_paths, parsed_parameters
    )

    if args.output_ddl_query_file_path:
        logger.info("Writing compiled DDLs to: %s", args.output_ddl_query_file_path)
        with open(args.output_ddl_query_file_path, "w", encoding="utf-8") as f_out:
            f_out.write("\n;\n".join(sql for _, sql in file_queries))

    execute_ddl_queries(
        file_queries=file_queries,
        skip_confirmation=args.skip_confirmation,
        parameters=parsed_parameters,
    )


if __name__ == "__main__":
    main()
