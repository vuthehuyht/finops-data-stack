"""Shared Dagster step-level retry policies.

These sit on top of the application-level retry in ``BaseClient.call_api_with_retry``
(tenacity). They catch transient failures that escape the API client — flaky S3
uploads, brief Redshift connection drops, run worker hiccups — without masking
deterministic data/logic errors (a low ``max_retries`` keeps those from looping).
"""

from dagster import Backoff, Jitter, RetryPolicy

# Ingestion assets: external API calls + S3 upload. The API client already retries
# internally, so this only covers what leaks past it (e.g. S3 5xx, DNS blips).
INGEST_RETRY = RetryPolicy(
    max_retries=2,
    delay=30,
    backoff=Backoff.EXPONENTIAL,
    jitter=Jitter.PLUS_MINUS,
)

# Load assets: Redshift COPY. Transient connection resets, lock waits, and
# serverless resume latency are the common transient causes here.
LOAD_RETRY = RetryPolicy(
    max_retries=3,
    delay=15,
    backoff=Backoff.EXPONENTIAL,
    jitter=Jitter.PLUS_MINUS,
)

# dbt build: only a transient Redshift disconnect is worth retrying. Data quality
# gate assertions are deterministic and must not be retried, hence a single try.
DBT_RETRY = RetryPolicy(max_retries=1, delay=20)

# SageMaker training / batch transform: expensive and long-running. Retry at most
# once, and only for infrastructure-level failures.
SAGEMAKER_RETRY = RetryPolicy(max_retries=1, delay=60)
