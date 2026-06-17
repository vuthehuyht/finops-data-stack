# FinOps Data Stack — Testing Rules

> Testing conventions for `src/`. The project has no test/coverage threshold yet — this is a minimal, actionable baseline, to be adjusted as the team/scope grows.

## Required test coverage

Any new transform logic in `src/transform`, `src/common`, `src/ml` must have a corresponding unit test before merging.

## Test framework

`pytest` (already listed in `[dependency-groups].dev` of `pyproject.toml`).

## Naming & location

- Test file: `test_<module>.py`, placed under `tests/` mirroring the `src/` structure (e.g. `src/transform/foo.py` → `tests/transform/test_foo.py`).
- Test function name: `test_<behavior>_<condition>`, e.g. `test_parse_eod_price_raises_on_missing_column`.

## Mock boundary

- Any S3/Redshift/SageMaker call in a unit test must be mocked (`moto` for AWS services, or a fixture stub) — never call real AWS.
- Real integration tests (when needed against real AWS) must be separated and marked `@pytest.mark.integration`, not run in the default CI.

## dbt test

- Every dbt model must have at least `unique` + `not_null` tests on its primary key, declared in a `.yml` file alongside the model.
- The Gold-layer model feeding ML (`fact_ml_feature_set`) needs a custom test for the Data Quality Gate: null rate within an acceptable threshold, values within a valid range (see Data Quality Gate detail: `docs/architecture-design.md` section 4.1).

## Not required (current stage)

- A hard coverage % target.
- Full pipeline e2e tests (single-person project, early stage).
