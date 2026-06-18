#!/usr/bin/env bash
#
# Update manifest.json of the dbt project.
set -euo pipefail -o posix

cd "$(dirname "$0")/.."  # Move to the dbt project directory.

# Ensure that the installed Python packages are synchronized with poetry.lock
# because the installed dbt-related Python packages change the output manifest.json.
poetry sync

# Ensure that the dbt packages specified in package-lock.yml are installed
# because the installed dbt packages change the output manifest.json.
poetry run dbt deps

TMP_DIR="$(mktemp -d)"
readonly TMP_DIR

delete_temporary_directory() {
  rm -rf "${TMP_DIR}"
}

# Delete the temporary directory at exit.
trap delete_temporary_directory EXIT

SNOWFLAKE_SCHEMA_FUNCTION='DB_UTILS' \
SNOWFLAKE_SCHEMA_RAW='RAW' \
SNOWFLAKE_SCHEMA_DWH='DWH' \
SNOWFLAKE_SCHEMA_MART='MART' \
poetry run dbt parse \
  --target no_snowflake_access \
  --target-path "${TMP_DIR}/target" \
  --log-path "${TMP_DIR}/logs" \
  --write-json \
  --no-send-anonymous-usage-stats

# Normalize the manifest.json to make it consistent between invocations.
jq '.nodes[].created_at = 0
    | .sources[].created_at = 0
    | .macros[].created_at = 0
    | .unit_tests[].created_at = 0
    | .metadata.invocation_id = "00000000-0000-0000-0000-000000000000"
    | .metadata.generated_at = "1970-01-01T00:00:00Z"' \
  --sort-keys \
  "${TMP_DIR}/target/manifest.json" > manifest.json
