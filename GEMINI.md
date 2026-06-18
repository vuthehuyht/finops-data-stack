# FinOps Data Stack — Context for AI Coding Agent

This file serves as the main context entrypoint for AI Agents. It provides a comprehensive summary of the project to reduce file reads.

---

## 1. Project Summary & Market Scope
- **Goal**: Automate collection, processing, and prediction of stock valuation for the Vietnamese stock market (VN-Index, HNX, UPCOM) using Fundamental and Sentiment Analysis.
- **Data Groups**: 
  1. Market (EOD prices, foreign flow, proprietary flow).
  2. Fundamental (Financial statements, financial ratios).
  3. Macro & Commodities (GDP, CPI, FX, Oil, Spread).
  4. Qualitative (Corporate news, insider transactions).
- **AI/ML Model**: Multimodal Hybrid Neural Network (Time-Series branch LSTM/GRU + Tabular branch MLP) forecasting expected return or trend classification.

---

## 2. Technology Stack & Directory Structure
- **Stack**: AWS (S3, Redshift Serverless, SageMaker Serverless, EKS), **Dagster** (orchestrator), **dbt** (transformation).
- **Python Version**: `>=3.12`, package manager: `uv`.
- **Execution Rules**: All terminal commands (e.g., `python`, `pytest`, `ruff`, `dagster`, `dbt`) **MUST** be run using `uv run` (e.g., `uv run pytest`, `uv run dagster dev`).
- **Directory Layout (Flywheel Architecture)**:
  - `src/common/` — Shared AWS Utils, DB Utils, Logging.
  - `src/dagster/` — Software-Defined Assets, Jobs, Sensors, Resources.
  - `src/load/` — Ingestion layer (Crawl/API -> S3).
  - `src/transform/` — dbt project (transformation).
  - `src/ml/` — Machine Learning training and inference scripts.
  - `src/pipeline/` — End-to-end processing flows.

---

## 3. Core Coding Rules

### A. Python Conventions
- **Formatting**: Linter: `ruff` (line-length = 88, target-version = "py312").
- **Variable Naming**: English. Meaningful names, avoid single-letter variables. Booleans start with `is_`, `has_`, `should_`, `can_`. Avoid shadowing built-ins.
- **Function Design**: Single responsibility, ideally <= 50 lines, max 4 parameters. Prefer pure functions for data transformations (avoid mutating inputs).
- **Type Hints**: Required for public function signatures. Avoid `Any`.
- **Docstrings**: Use Google Style for complex functions (with `Args`, `Returns`, `Raises`). Written in English.
- **Comments**: Only explain WHY (non-obvious reasoning), written in English. Update comments along with code.
- **Constants**: `UPPER_CASE_WITH_UNDERSCORES`. Extract to `constants.py` or `config.py` if there are too many.
- **Error Handling**: Do not catch-and-ignore exceptions. Pipeline errors must fail loudly. Validate input at boundaries.
- **Logging**: Use Dagster's built-in `context.log` for asset/job execution.

### B. dbt & SQL Conventions
- **Keywords**: SQL keywords and syntax (e.g., `SELECT`, `FROM`, `WHERE`, `JOIN`) must be in **UPPERCASE**.
- **Indentation**: 2-space indentation (configured in `.editorconfig`).
- **Formatting**: Use CTEs (Common Table Expressions) instead of nested subqueries. The final CTE must be named `final` or `select_final`.
- **Naming**: Database columns and table aliases must be in UPPERCASE. dbt models: `STG_<entity>` for staging (e.g. `STG_STOCK_PRICE_EOD`), `fct_`/`dim_` for marts.
- **Testing**: Every model must have a `.yml` file with at least `unique` and `not_null` tests on the primary key.

---

## 4. Reference Files
Read detailed guidelines under these paths when performing deep work:
- Detailed Project Context: [.agents/project.md](.agents/project.md)
- Architecture details: [.agents/architecture.md](.agents/architecture.md)
- Detailed Coding Rules: [.agents/coding-rules.md](.agents/coding-rules.md)
- Testing Conventions: [.agents/testing.md](.agents/testing.md)
- Full Design Docs: `docs/architecture-design.md`, `docs/ml-architecture-design.md`, `docs/infrastructure-design.md`.
