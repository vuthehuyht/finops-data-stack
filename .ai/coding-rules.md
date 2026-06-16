# FinOps Data Stack — Coding Rules

> Coding conventions for `src/`. The project is currently at skeleton stage — these are conventions set in advance (to-be), to be reviewed once the first real code lands (Phase 2 in `docs/architecture-design.md`).

## Python style

- Follow the ruff rules configured in `pyproject.toml`: `E`, `W`, `F`, `I`, `C`, `B`, `UP`.
- `line-length = 88`, `target-version = "py312"`.
- Type hints are required for public function/method signatures (both params and return type).
- Comments should only explain WHY (non-obvious reasoning), written in Vietnamese. Do not comment WHAT.
- Short docstrings for public functions/classes — state the purpose, don't restate the function name.

## Dagster asset convention

- Naming pattern: `<layer>__<entity>`, e.g. `bronze__stock_price_eod`, `silver__financial_ratio`, `gold__ml_feature_set`.
- Every asset must declare a clear `description` and `metadata` (e.g. data source, update frequency).
- Assets must declare dependencies explicitly via `AssetIn`/`deps` — no implicit side effects between assets.

## dbt model convention

- Organize by layer: Bronze (raw) / Silver (staging/cleaned) / Gold (mart), matching the Data Lake structure in `.ai/architecture.md`.
- Naming: `stg_<source>__<entity>` for staging, `fct_`/`dim_` for the mart layer (Gold).
- Every model must have a `.yml` file declaring at least `unique` and `not_null` tests on the primary key.
- Metadata: Raw layer tagged with `_CONATA_*`, Cleaned layer tagged with `DATACORE_*` (see `docs/architecture-design.md` for detail).

## Error handling

- Do not catch-and-ignore exceptions just to make code "run". Pipeline errors must raise clearly so Dagster retry/alerting can catch them — no silent failures.
- Validate input at boundaries (API responses, files read from S3, user input) before using it further in the pipeline.

## General naming

- Variable, function, class, file, and branch names: English, following PEP8 / standard Python conventions.
- Log/error messages shown to end users: Vietnamese, concise, no internal leakage (stack traces, secrets).
