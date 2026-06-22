"""Dagster resource definitions.

Canonical place for all Dagster @resource declarations.
Add redshift_resource, sagemaker_resource, etc. here as the project grows.
"""

import dagster

from src.pipeline.dagster.metrics import PrometheusResource


@dagster.resource()
def prometheus_resource(
    init_context: dagster.InitResourceContext,
) -> PrometheusResource:
    """Get a PrometheusResource."""
    return PrometheusResource()
