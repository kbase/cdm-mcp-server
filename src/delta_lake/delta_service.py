"""
Service layer for interacting with Delta Lake tables via Spark.
"""

import logging
from typing import Any, Dict, List

from pyspark.sql import SparkSession

from src.delta_lake.data_store import database_exists, table_exists
from src.service.exceptions import (
    DeltaDatabaseNotFoundError,
    DeltaTableNotFoundError,
    SparkOperationError,
)

logger = logging.getLogger(__name__)

MAX_SAMPLE_ROWS = 1000

# Common SQL keywords that might indicate destructive operations
FORBIDDEN_KEYWORDS = {
    "drop",
    "truncate",
    "delete",
    "insert",
    "update",
    "create",
    "alter",
    "merge",
    "replace",
    "rename",
    "vacuum",
}


def _check_exists(database: str, table: str) -> bool:
    """
    Check if a table exists in a database.
    """
    if not database_exists(database):
        raise DeltaDatabaseNotFoundError(f"Database [{database}] not found")
    if not table_exists(database, table):
        raise DeltaTableNotFoundError(
            f"Table [{table}] not found in database [{database}]"
        )
    return True


def count_delta_table(spark: SparkSession, database: str, table: str) -> int:
    """
    Counts the number of rows in a specific Delta table.

    Args:
        spark: The SparkSession object.
        database: The database (namespace) containing the table.
        table: The name of the Delta table.

    Returns:
        The number of rows in the table.
    """
    _check_exists(database, table)
    full_table_name = f"`{database}`.`{table}`"
    logger.info(f"Counting rows in {full_table_name}")
    try:
        count = spark.table(full_table_name).count()
        logger.info(f"{full_table_name} has {count} rows.")
        return count
    except Exception as e:
        logger.error(f"Error counting rows in {full_table_name}: {e}")
        raise SparkOperationError(
            f"Failed to count rows in {full_table_name}: {str(e)}"
        )


def sample_delta_table(
    spark: SparkSession,
    database: str,
    table: str,
    limit: int = 10,
    columns: List[str] | None = None,
    where_clause: str | None = None,
) -> List[Dict[str, Any]]:
    """
    Retrieves a sample of rows from a specific Delta table.

    Args:
        spark: The SparkSession object.
        database: The database (namespace) containing the table.
        table: The name of the Delta table.
        limit: The maximum number of rows to return.
        columns: The columns to return. If None, all columns will be returned.
        where_clause: A SQL WHERE clause to filter the rows. e.g. "id > 100"

    Returns:
        A list of dictionaries, where each dictionary represents a row.
    """

    if not 0 < limit <= MAX_SAMPLE_ROWS:
        raise ValueError(f"Limit must be between 1 and {MAX_SAMPLE_ROWS}, got {limit}")

    _check_exists(database, table)
    full_table_name = f"`{database}`.`{table}`"
    logger.info(f"Sampling {limit} rows from {full_table_name}")
    try:
        df = spark.table(full_table_name)
        if columns:
            df = df.select(columns)
        if where_clause:
            for keyword in FORBIDDEN_KEYWORDS:
                if keyword in where_clause.lower():
                    raise ValueError(f"Filter expression contains forbidden keyword: {keyword}")
            df = df.filter(where_clause)

        df = df.limit(limit)

        results = [row.asDict() for row in df.collect()]
        logger.info(f"Sampled {len(results)} rows.")
        return results
    except Exception as e:
        logger.error(f"Error sampling rows from {full_table_name}: {e}")
        raise SparkOperationError(
            f"Failed to sample rows from {full_table_name}: {str(e)}"
        )
