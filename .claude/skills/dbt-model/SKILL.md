---
name: dbt-model
description: Scaffold a new dbt model (SQL + .yml schema) following this project's Bronze/Silver/Gold layer convention. Use when creating a new dbt model in src/transform.
---

# dbt Model Scaffold

Generate a new dbt model and its `.yml` schema file following the conventions in `.agents/coding-rules.md` and `.agents/testing.md`.

## Conventions (from `.agents/coding-rules.md`)

- Layers: Bronze (raw) / Silver (staging/cleaned) / Gold (mart).
- Naming: `stg_<source>_<entity>` for staging models, `fct_`/`dim_` for mart-layer (Gold) models.
- Metadata: Raw layer tagged `_CONATA_*`, Cleaned layer tagged `DATACORE_*` (see `docs/architecture-design.md`).
- Every model must have a `.yml` file with at least `unique` + `not_null` tests on the primary key (per `.agents/testing.md`).
- The Gold-layer model feeding ML (`fact_ml_feature_set`) needs an additional custom test for the Data Quality Gate (null rate, value range) — see `docs/architecture-design.md` section 4.1.

## Steps

1. Ask the user (if not already clear from context):
   - Layer: staging or mart (Gold)?
   - Source table/entity name.
   - Primary key column(s).
2. Determine the model name:
   - Staging: `stg_<source>_<entity>.sql`
   - Mart fact: `fct_<entity>.sql` / Mart dimension: `dim_<entity>.sql`
3. Find the existing dbt project structure under `src/transform/` (e.g. `models/staging/`, `models/marts/`) and place the new file in the matching directory. If the directory structure doesn't exist yet, ask the user where to place it before creating.
4. Write the `.sql` model file with a minimal `select` skeleton sourcing from the upstream table.
5. Write the matching `.yml` schema file in the same directory, declaring:
   ```yaml
   version: 2

   models:
     - name: <model_name>
       columns:
         - name: <primary_key_column>
           tests:
             - unique
             - not_null
   ```
6. If this is the `fact_ml_feature_set` model (or another Gold-layer ML feature model), add a note reminding the user to add a custom Data Quality Gate test (do not invent the test logic — ask what thresholds to use, per `.agents/testing.md`).
7. Show the user the created files and ask them to fill in the actual SQL transformation logic — do not invent business logic.
