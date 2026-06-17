# FinOps Data Stack — Coding Rules

> Coding conventions for `src/`. The project is currently at skeleton stage — these are conventions set in advance (to-be), to be reviewed once the first real code lands (Phase 2 in `docs/architecture-design.md`).

## Python style

- Follow the ruff rules configured in `pyproject.toml`: `E`, `W`, `F`, `I`, `C`, `B`, `UP`.
- `line-length = 88`, `target-version = "py312"`.
- Type hints are required for public function/method signatures (both params and return type). Avoid using `Any` in type hints unless absolutely necessary.
- Comments should only explain WHY (non-obvious reasoning), written in Vietnamese. Do not comment WHAT. Ensure comments are updated alongside code changes to avoid obsolete explanations.
- Inline comments must be kept to a minimum and separated by at least two spaces from the code statement.
- Follow Google Style for docstrings. For complex functions, include `Args`, `Returns`, and `Raises` sections.
- Short docstrings for simple public functions/classes — state the purpose, don't restate the function name.
- Use `# TODO(username): <description>` for pending tasks or planned enhancements.
- Keep functions small and focused on a single responsibility. Prefer functions under 50 lines.
- Limit the number of parameters per function (ideally <= 4). Use dataclasses or dicts for complex configurations.
- Favor pure functions for data transformations to avoid side effects; do not mutate mutable input arguments.
- Ensure consistent return types. Avoid mixed return types unless explicitly defined with `Union` or `Optional`.

## Dagster asset convention

- Naming pattern: `<layer>_<entity>`, e.g. `bronze_stock_price_eod`, `silver_financial_ratio`, `gold_ml_feature_set`.
- Every asset must declare a clear `description` and `metadata` (e.g. data source, update frequency).
- Assets must declare dependencies explicitly via `AssetIn`/`deps` — no implicit side effects between assets.
- Use Dagster's built-in context logger (`context.log`) instead of standard `print()` or Python's standard `logging` module to ensure logs are captured in the Dagster UI.

## dbt model convention

- Organize by layer: Bronze (raw) / Silver (staging/cleaned) / Gold (mart), matching the Data Lake structure in `.agents/architecture.md`.
- Naming: `stg_<source>_<entity>` for staging, `fct_`/`dim_` for the mart layer (Gold).
- Every model must have a `.yml` file declaring at least `unique` and `not_null` tests on the primary key.
- Metadata: Raw layer tagged with `_CONATA_*`, Cleaned layer tagged with `DATACORE_*` (see `docs/architecture-design.md` for detail).
- SQL keywords and syntax (e.g., SELECT, FROM, WHERE, JOIN) must be written in UPPERCASE.
- Prefer Common Table Expressions (CTEs) over nested subqueries for readability. The final output CTE should be named `final` or `select_final`.
- Column names and table aliases must be written in lowercase `snake_case`.

## Error handling

- Do not catch-and-ignore exceptions just to make code "run". Pipeline errors must raise clearly so Dagster retry/alerting can catch them — no silent failures.
- Validate input at boundaries (API responses, files read from S3, user input) before using it further in the pipeline.

## General naming

- Variable, function, class, file, and branch names: English, following PEP8 / standard Python conventions.
- Variable names must be meaningful and self-describing; avoid single-letter names (like `a`, `x`, `df`) unless used as temporary loop indices or in short comprehensions.
- Boolean variables should start with a prefix indicating status (e.g., `is_`, `has_`, `should_`, `can_`).
- Module-level constants must be named in UPPER_CASE_WITH_UNDERSCORES. If a module or project component contains too many constants, extract them into a dedicated constants file (e.g., `constants.py` or `config.py`) to keep the codebase clean.
- Avoid shadowing Python built-in functions or types (e.g., do not name variables `list`, `dict`, `str`, `id`, `type`).
- Log/error messages shown to end users: Vietnamese, concise, no internal leakage (stack traces, secrets).
