#!/usr/bin/env bash
#
# Set up Redshift resources for development.
set -euo pipefail -o posix

# Navigate to the redshift module root directory
cd "$(dirname "$0")/.."

# Load environment variables from .env if it exists and vars are not already set
ROOT_DIR="$(git rev-parse --show-toplevel 2>/dev/null || echo "../..")"
if [[ -f "${ROOT_DIR}/.env" ]]; then
  set -a
  # shellcheck source=/dev/null
  source "${ROOT_DIR}/.env"
  set +a
fi

ADDITIONAL_OPTIONS=''
for argument in "$@"; do
  if [[ "${argument}" = '--skip_confirmation' ]]; then
    ADDITIONAL_OPTIONS="${ADDITIONAL_OPTIONS} ${argument}"
  fi
done

# Execute the DDL script via uv run python
uv run python ddl_executor.py \
  --template_parameters="{
    \"schema_name_raw\": \"${REDSHIFT_SCHEMA_RAW:-RAW}\",
    \"schema_name_staging\": \"${REDSHIFT_STAGING_SCHEMA:-STAGING}\",
    \"schema_name_mart\": \"${REDSHIFT_SCHEMA_MART:-MART}\"
  }" \
  ${ADDITIONAL_OPTIONS} \
  ddl/dev/setup.sql.jinja \
  ddl/raw/*.sql.jinja
