# dbt

## Development

### Run

1. [Optional] Source environment variables for development.

    ```bash
    source "$(git rev-parse --show-toplevel)/.env"
    ```

1. Run `dbt` command.

    ```bash
    poetry run dbt run --vars '{"partition_key": "2024-01-01T00:00:00+09:00"}'
    ```

#### Optional variables

Name | Description
--- | ---
`statement_timeout_in_seconds` | Timeout in seconds for each SQL statement in Snowflake (default: `3600`). See [`STATEMENT_TIMEOUT_IN_SECONDS`](https://docs.snowflake.com/en/sql-reference/parameters#statement-timeout-in-seconds).

### How to update dbt packages

1. Edit [packages.yml](./packages.yml).
1. Lock packages.

    To update deprecated packages, all warnings (e.g. `PackageRedirectDeprecation`) are allowed.

    ```bash
    poetry run dbt deps --warn-error-options '{"error": []}' --lock
    ```

1. Install the updated dbt packages.

    ```bash
    "$(git rev-parse --show-toplevel)/tools/setup.sh"
    ```

### Update YAML files for models

1. [Optional] Source environment variables for development.

    ```bash
    source "$(git rev-parse --show-toplevel)/.env"
    ```

1. Run the command to automatically create or update YAML files for models.

    ```bash
    poetry run dbt-osmosis yaml refactor --numeric-precision-and-scale --string-length
    ```

### How to update the manifest file

1. Run the script.

    ```bash
    ./dev/update_manifest.sh
    ```

    The script updates [manifest.json](manifest.json) which will be used by Dagster.

### Generate Entity-Relationship Diagram (ERD)

1. [Optional] Source environment variables for development.

    ```bash
    source "$(git rev-parse --show-toplevel)/.env"
    ```

1. Run the command to generate `catalog.json`.

    ```bash
    poetry run dbt docs generate
    ```

1. Run [`dbterd`](https://dbterd.datnguyen.de/) to generate entity-relationship diagram.

    ```bash
    OUTPUT_DIR='/tmp'  # Change the output directory as you need.
    OUTPUT_FORMAT='mermaid'  # Choose one of [dbml, mermaid, plantuml, d2, graphviz] as you need.

    # The output directory must exist otherwise dbterd fails.
    mkdir -p "${OUTPUT_DIR}"
    poetry run dbterd run -ad target/ -rt source -rt model -t "${OUTPUT_FORMAT}" -o "${OUTPUT_DIR}"
    ```
