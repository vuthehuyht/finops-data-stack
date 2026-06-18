"""Functions and variables to transform data on Snowflake."""

from pathlib import Path

DBT_PROJECT_DIR = Path(__file__).joinpath("..", "dbt").resolve()
DBT_MANIFEST_FILE_PATH = DBT_PROJECT_DIR / "manifest.json"
