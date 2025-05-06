"""
Dependencies for FastAPI dependency injection.
"""

import os

from pyspark.sql import SparkSession

from src.delta_lake.spark_utils import get_spark_session as _get_spark_session
from src.service.http_bearer import KBaseHTTPBearer

# Initialize the KBase auth dependency for use in routes
auth = KBaseHTTPBearer()

DEFAULT_SPARK_POOL = "default"
DEFAULT_EXECUTOR_CORES = 1


def get_spark_session() -> SparkSession:
    """
    Get a SparkSession instance.
    """
    return _get_spark_session(
        yarn=False,
        scheduler_pool=str(os.getenv("SPARK_POOL", DEFAULT_SPARK_POOL)),
        executor_cores=int(os.getenv("EXECUTOR_CORES", DEFAULT_EXECUTOR_CORES)),
    )
