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

Skeleton (Phase 1) — `src/` chỉ có `__init__.py` rỗng, chưa có code thật. Phase 2 mới bắt đầu implement.
Khi search/glob, dùng `src/`, `tests/`, `docs/`, `.agents/` làm root — bỏ qua `.venv/`.

## Dev commands

All commands must be executed using `uv run` to ensure they run in the project's virtual environment.

```bash
uv run ruff check src/ tests/   # lint
uv run ruff format src/ tests/  # format
uv run pytest tests/            # run tests
uv run dagster dev              # start Dagster UI at localhost:3000
```

## Project-specific skills

- `/dagster-asset` — scaffold Dagster asset đúng naming + metadata convention.
- `/dbt-model` — scaffold dbt model (SQL + `.yml`) đúng layer convention.
- `/gen-pr` — tự động tạo PR description từ git diff hiện tại theo template.
