ARG PYTHON_VERSION=3.12

# Stage 1: builder — install deps and bake dbt packages
FROM python:${PYTHON_VERSION}-slim-bookworm AS builder

COPY --from=ghcr.io/astral-sh/uv:0.6 /uv /usr/local/bin/uv

WORKDIR /app

# Install Python dependencies — exclude dev, dagster (webserver UI), and torch
# (only needed inside the SageMaker training/inference containers, not here;
# sagemaker SDK stays in base dependencies since the Dagster orchestrator
# calls it to launch training jobs -- see src/ml/training_job.py).
COPY pyproject.toml uv.lock README.md ./
RUN uv sync --no-dev --no-group dagster --no-group ml-torch --frozen --no-cache

ENV PATH="/app/.venv/bin:$PATH"

COPY src/ ./src/

# Pre-bake dbt packages (avoids fetching at runtime)
WORKDIR /app/src/transform/dbt
RUN dbt deps --log-path logs && dbt parse --log-path logs && rm -rf .user.yml logs

# Stage 2: runtime — clean image without uv binary
FROM python:${PYTHON_VERSION}-slim-bookworm

WORKDIR /app

COPY --from=builder /app/.venv /app/.venv
COPY --from=builder /app/src /app/src

ENV PATH="/app/.venv/bin:$PATH"
ENV PYTHONPATH=/app

EXPOSE 3030

CMD ["dagster", "api", "grpc", "--port", "3030", "-h", "0.0.0.0", "--module-name", "src.dagster.workspace"]
