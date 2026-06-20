ARG PYTHON_VERSION=3.12
FROM python:${PYTHON_VERSION}-slim-bookworm

# Install uv from the official image
COPY --from=ghcr.io/astral-sh/uv:0.6 /uv /usr/local/bin/uv

WORKDIR /app

# Install Python dependencies — exclude dev, dagster (webserver UI), and ml groups
COPY pyproject.toml uv.lock ./
RUN uv sync --no-dev --no-group dagster --no-group ml --frozen --no-cache

# Expose venv binaries (dagster, dbt, etc.)
ENV PATH="/app/.venv/bin:$PATH"
ENV PYTHONPATH=/app

# Copy source code
COPY src/ ./src/

# Pre-bake dbt packages into the image (avoids fetching packages at runtime)
WORKDIR /app/src/transform/dbt
RUN dbt deps --log-path logs && rm -rf .user.yml logs

WORKDIR /app

# Serve the Dagster code location as a gRPC server on port 3030
CMD ["dagster", "api", "grpc", "--port", "3030", "-h", "0.0.0.0", "--module-name", "src.dagster"]
