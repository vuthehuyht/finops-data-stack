# FinOps Data Stack — Context for Claude Code

Project: Data pipeline + ML for the Vietnamese stock market (Dagster, dbt, Redshift, SageMaker).

Detailed context lives in `.ai/` — read these files as needed:
- `.ai/project.md` — goals, scope, data inventory
- `.ai/architecture.md` — stack, data flow, directory structure
- `.ai/coding-rules.md` — Python/Dagster/dbt coding conventions
- `.ai/testing.md` — testing conventions

Full design documentation: see `docs/`.

## Project stage

Skeleton (Phase 1) — `src/` chỉ có `__init__.py` rỗng, chưa có code thật. Phase 2 mới bắt đầu implement.
Khi search/glob, dùng `src/`, `tests/`, `docs/`, `.ai/` làm root — bỏ qua `.venv/`.

## Dev commands

```bash
ruff check src/ tests/   # lint
ruff format src/ tests/  # format
pytest tests/            # chạy tests
dagster dev              # Dagster UI tại localhost:3000
```

## Project-specific skills

- `/dagster-asset` — scaffold Dagster asset đúng naming + metadata convention.
- `/dbt-model` — scaffold dbt model (SQL + `.yml`) đúng layer convention.
