"""
Spark utilities for the Delta Lake service.
"""

## Copied from https://github.com/kbase/cdm-jupyterhub/blob/main/src/spark/utils.py

import os
import socket
from datetime import datetime
from typing import Dict, List, Optional
from urllib.parse import urlparse

from pyspark.conf import SparkConf
from pyspark.sql import SparkSession

from src.service.arg_checkers import not_falsy

# the default number of CPU cores that each Spark executor will use
# If not specified, Spark will typically use all available cores on the worker nodes
DEFAULT_EXECUTOR_CORES = 1
# Available Spark fair scheduler pools are defined in /config/spark-fairscheduler.xml
SPARK_DEFAULT_POOL = "default"
SPARK_POOLS = [SPARK_DEFAULT_POOL, "highPriority"]
DEFAULT_MAX_EXECUTORS = 5


def _get_jars(jar_names: List[str]) -> str:
    """
    Helper function to get the required JAR files as a comma-separated string.

    :param jar_names: List of JAR file names

    :return: A comma-separated string of JAR file paths
    """
    jar_dir = not_falsy(os.getenv("SPARK_JARS_DIR"), "SPARK_JARS_DIR")
    jars = [os.path.join(jar_dir, jar) for jar in jar_names]

    missing_jars = [jar for jar in jars if not os.path.exists(jar)]
    if missing_jars:
        raise FileNotFoundError(f"Some required jars are not found: {missing_jars}")

    return ", ".join(jars)


def _get_s3_conf() -> Dict[str, str]:
    """
    Helper function to get S3 configuration for MinIO.
    """
    minio_url = not_falsy(os.environ.get("MINIO_URL"), "MINIO_URL")
    minio_access_key = not_falsy(os.environ.get("MINIO_ACCESS_KEY"), "MINIO_ACCESS_KEY")
    minio_secret_key = not_falsy(os.environ.get("MINIO_SECRET_KEY"), "MINIO_SECRET_KEY")

    if not all([minio_url, minio_access_key, minio_secret_key]):
        raise EnvironmentError("Missing required MinIO environment variables")

    return {
        "spark.hadoop.fs.s3a.endpoint": minio_url,
        "spark.hadoop.fs.s3a.access.key": minio_access_key,
        "spark.hadoop.fs.s3a.secret.key": minio_secret_key,
        "spark.hadoop.fs.s3a.path.style.access": "true",
        "spark.hadoop.fs.s3a.impl": "org.apache.hadoop.fs.s3a.S3AFileSystem",
    }


def _get_delta_lake_conf() -> Dict[str, str]:
    """
    Helper function to get Delta Lake specific Spark configuration.

    :return: A dictionary of Delta Lake specific Spark configuration

    reference: https://blog.min.io/delta-lake-minio-multi-cloud/
    """

    return {
        "spark.sql.extensions": "io.delta.sql.DeltaSparkSessionExtension",
        "spark.sql.catalog.spark_catalog": "org.apache.spark.sql.delta.catalog.DeltaCatalog",
        "spark.databricks.delta.retentionDurationCheck.enabled": "false",
        "spark.sql.catalogImplementation": "hive",
    }


def _validate_env_vars(required_vars: List[str], context: str) -> None:
    """Validate required environment variables."""
    missing = [var for var in required_vars if var not in os.environ]
    if missing:
        raise EnvironmentError(
            f"Missing required environment variables for {context}: {missing}"
        )


def get_spark_session(
    app_name: Optional[str] = None,
    local: bool = False,
    yarn: bool = True,
    delta_lake: bool = True,
    executor_cores: int = DEFAULT_EXECUTOR_CORES,
    scheduler_pool: str = SPARK_DEFAULT_POOL,
    spark_master_url: str | None = None,
) -> SparkSession:
    """
    Helper to get and manage the SparkSession and keep all of our spark configuration params in one place.

    :param app_name: The name of the application. If not provided, a default name will be generated.
    :param local: Whether to run the spark session locally or not. Default is False.
    :param yarn: Whether to run the spark session on YARN or not. Default is True.
    :param delta_lake: Build the spark session with Delta Lake support. Default is True.
    :param executor_cores: The number of CPU cores that each Spark executor will use. Default is 1.
    :param scheduler_pool: The name of the scheduler pool to use. Default is "default".

    :return: A SparkSession object
    """

    app_name = (
        app_name or f"cdm_mcp_server_session_{datetime.now().strftime('%Y%m%d%H%M%S')}"
    )

    if local:
        return SparkSession.builder.appName(app_name).getOrCreate()

    config: Dict[str, str] = {
        "spark.app.name": app_name,
        "spark.executor.cores": str(executor_cores),
    }

    # Dynamic allocation configuration
    config.update(
        {
            "spark.dynamicAllocation.enabled": "true",
            "spark.dynamicAllocation.minExecutors": "1",
            "spark.dynamicAllocation.maxExecutors": os.getenv(
                "MAX_EXECUTORS", str(DEFAULT_MAX_EXECUTORS)
            ),
        }
    )

    # Fair scheduler configuration
    _validate_env_vars(["SPARK_FAIR_SCHEDULER_CONFIG"], "FAIR scheduler setup")
    config.update(
        {
            "spark.scheduler.mode": "FAIR",
            "spark.scheduler.allocation.file": os.environ[
                "SPARK_FAIR_SCHEDULER_CONFIG"
            ],
        }
    )

    # Kubernetes configuration
    _validate_env_vars(["SPARK_DRIVER_HOST"], "Kubernetes setup")
    hostname = os.environ["SPARK_DRIVER_HOST"]
    if os.environ.get("USE_KUBE_SPAWNER") == "true":
        yarn = False  # YARN is not used in the Kubernetes spawner
        # Since the Spark driver cannot resolve a pod's hostname without a dedicated service for each user pod,
        # use the pod IP as the identifier for the Spark driver host
        config["spark.driver.host"] = socket.gethostbyname(hostname)
        # In containerized environments, bind to all interfaces to allow connections
        config["spark.driver.bindAddress"] = "0.0.0.0"
    else:
        # General driver host configuration - hostname is resolvable
        config["spark.driver.host"] = hostname

    # YARN configuration
    if yarn:
        _validate_env_vars(
            ["YARN_RESOURCE_MANAGER_URL", "S3_YARN_BUCKET"], "YARN setup"
        )
        yarnparse = urlparse(os.environ["YARN_RESOURCE_MANAGER_URL"])

        yarn_config = {
            "spark.master": "yarn",
            "spark.hadoop.yarn.resourcemanager.hostname": yarnparse.hostname,
            "spark.hadoop.yarn.resourcemanager.address": yarnparse.netloc,
            "spark.yarn.stagingDir": f"s3a://{os.environ['S3_YARN_BUCKET']}",
        }
        config.update(yarn_config)
    else:
        _validate_env_vars(["SPARK_MASTER_URL"], "Standalone Spark setup")
        if spark_master_url:
            config["spark.master"] = spark_master_url
        else:
            config["spark.master"] = os.environ["SPARK_MASTER_URL"]

    # S3 configuration
    if yarn or delta_lake:
        config.update(_get_s3_conf())

    # Delta Lake configuration
    if delta_lake:
        _validate_env_vars(
            ["HADOOP_AWS_VER", "DELTA_SPARK_VER", "SCALA_VER"], "Delta Lake setup"
        )
        config.update(_get_delta_lake_conf())

        if not yarn:
            jars = _get_jars(
                [
                    f"delta-spark_{os.environ['SCALA_VER']}-{os.environ['DELTA_SPARK_VER']}.jar",
                    f"hadoop-aws-{os.environ['HADOOP_AWS_VER']}.jar",
                ]
            )
            config["spark.jars"] = jars

    print(f"Spark configuration: {config}")
    # Create SparkConf from accumulated configuration
    spark_conf = SparkConf().setAll(list(config.items()))

    # Initialize SparkSession
    spark = SparkSession.builder.config(conf=spark_conf).getOrCreate()
    spark.sparkContext.setLogLevel("ERROR")

    # Configure scheduler pool
    if scheduler_pool not in SPARK_POOLS:
        print(
            f"Warning: Scheduler pool {scheduler_pool} is not in the list of available pools: {SPARK_POOLS} "
            f"Defaulting to {SPARK_DEFAULT_POOL} pool"
        )
        scheduler_pool = SPARK_DEFAULT_POOL
    spark.sparkContext.setLocalProperty("spark.scheduler.pool", scheduler_pool)

    return spark
