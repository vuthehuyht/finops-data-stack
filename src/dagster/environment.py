"""Environment configuration for Dagster."""

from __future__ import annotations

import enum
import os


@enum.unique
class TaskType(enum.Enum):
    """Task types. Loader or Transformer."""

    Loader = "loader"
    Transformer = "transformer"
    Mart = "mart"
    T2TTest = "t2t_test"


@enum.unique
class SchemaType(enum.Enum):
    """Schema types."""

    Batch = "batch"
    Master = "master"
    RealTime = "realtime"


@enum.unique
class RegionType(enum.Enum):
    """Region types."""

    Global = "global"
    Asia = "asia"
    GlobalMart = "global_mart"


def is_prod() -> bool:
    """Check if execution environment is production."""
    if os.getenv("DAGSTER_WORKSPACE_ENVIRONMENT") == "prod":
        return True
    return False
