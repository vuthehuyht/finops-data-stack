# FinOps Data Stack — Context for Claude Code

Project: Data pipeline + ML for the Vietnamese stock market (Dagster, dbt, Redshift, SageMaker).

Detailed context lives in `.agents/` — read these files as needed:
- `.agents/project.md` — goals, scope, data inventory
- `.agents/architecture.md` — stack, data flow, directory structure
- `.agents/coding-rules.md` — Python/Dagster/dbt coding conventions
- `.agents/testing.md` — testing conventions

Full design documentation: see `docs/`.

## Language

Code comments, docstrings, and commit messages: **English only**.

## Project stage

When performing search/glob, use `src/`, `tests/`, `docs/`, `.agents/` as roots — ignore `.venv/`.

## Dev commands

All commands must be executed using `uv run` to ensure they run in the project's virtual environment.

```bash
uv run ruff check src/ tests/   # lint
uv run ruff format src/ tests/  # format
uv run pytest tests/            # run tests
uv run dagster dev              # start Dagster UI at localhost:3000
```

## Project-specific skills

- `/dagster-asset` — scaffold Dagster asset with correct naming + metadata conventions.
- `/dbt-model` — scaffold dbt model (SQL + `.yml`) with correct layer conventions.
- `/gen-pr` — automatically generate PR description from current git diff using template.

## Subagent Execution Rules

- **Superpowers Subagent Permissions**: When executing using the subagent skill of superpowers, set full execution permissions (`enable_write_tools = true`, `enable_subagent_tools = true`, `enable_mcp_tools = true`) without asking the user.

