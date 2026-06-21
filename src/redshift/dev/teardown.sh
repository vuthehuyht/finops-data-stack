#!/usr/bin/env bash
#
# Tear down Redshift resources for development.
set -euo pipefail -o posix

# Navigate to the redshift module root directory
cd "$(dirname "$0")/.."

ADDITIONAL_OPTIONS=''
for argument in "$@"; do
  if [[ "${argument}" = '--skip_confirmation' ]]; then
    ADDITIONAL_OPTIONS="${ADDITIONAL_OPTIONS} ${argument}"
  fi
done

# Execute the teardown DDL script via uv run python
uv run python ddl_executor.py \
  --template_parameters="{
    \"schema_name_function\": \"${REDSHIFT_SCHEMA_FUNCTION:-DB_UTILS}\",
    \"schema_name_operation\": \"${REDSHIFT_SCHEMA_OPERATION:-OPERATION}\",
    \"schema_name_raw\": \"${REDSHIFT_SCHEMA_RAW:-RAW}\",
    \"schema_name_dwh\": \"${REDSHIFT_SCHEMA_DWH:-DWH}\",
    \"schema_name_mart\": \"${REDSHIFT_SCHEMA_MART:-MART}\",
    \"schema_name_logs\": \"${REDSHIFT_SCHEMA_LOGS:-LOGS}\"
  }" \
  ${ADDITIONAL_OPTIONS} \
  ddl/dev/teardown.sql.jinja
