"""
Service layer for interacting with Delta Lake tables via Spark.
"""

import hashlib
import json
import logging
from typing import Any, Dict, List, Optional

import sqlparse
from pyspark.sql import SparkSession

from src.delta_lake.data_store import database_exists, table_exists
from src.service.exceptions import (
    DeltaDatabaseNotFoundError,
    DeltaTableNotFoundError,
    SparkOperationError,
    SparkQueryError,
)

logger = logging.getLogger(__name__)

MAX_SAMPLE_ROWS = 1000
CACHE_EXPIRY_SECONDS = 3600  # Cache results for 1 hour by default

# Common SQL keywords that might indicate destructive operations
FORBIDDEN_KEYWORDS = {
    # NOTE: This might create false positives, legitemate queries might include these keywords
    # e.g. "SELECT * FROM orders ORDER BY created_at DESC"
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

DISALLOW_SQL_META_CHARS = {
    "--",
    "/*",
    "*/",
    ";",
    "\\",
}

ALLOWED_STATEMENTS = {
    "select",
}

FORBIDDEN_POSTGRESQL_SCHEMAS = {
    # NOTE: This might create false positives, legitemate queries might include these schemas
    # e.g. "SELECT * FROM jpg_files"
    # NOTE: may also need to expand this if other databases are used
    "pg_",
    "pg_catalog",
    "information_schema",
}


def _check_query_is_valid(query: str) -> bool:
    """
    Check if a query is valid.

    Please note that this function is not a comprehensive SQL query validator.
    It only checks for basic syntax and structure.
    MCP server should be configured to use read-only user for both PostgreSQL and MinIO.
    """

    try:
        # NOTE: sqlparse does not validate SQL syntax; what happens with unexpected syntax is unknown
        statements = sqlparse.parse(query)
    except Exception as e:
        raise SparkQueryError(f"Query {query} is not a valid SQL query: {e}")

    if len(statements) != 1:
        raise SparkQueryError(f"Query {query} must contain exactly one statement")

    statement = statements[0]
    # NOTE: statement might have subqueries, we only check the main statement here!
    if statement.get_type().lower() not in ALLOWED_STATEMENTS:
        raise SparkQueryError(
            f"Query {query} must be one of the following: {', '.join(ALLOWED_STATEMENTS)}, got {statement.get_type()}"
        )

    if any(schema in query.lower() for schema in FORBIDDEN_POSTGRESQL_SCHEMAS):
        raise SparkQueryError(
            f"Query {query} contains forbidden PostgreSQL schema: {', '.join(FORBIDDEN_POSTGRESQL_SCHEMAS)}"
        )

    if any(char in query for char in DISALLOW_SQL_META_CHARS):
        raise SparkQueryError(
            f"Query {query} contains disallowed metacharacter: {', '.join(char for char in DISALLOW_SQL_META_CHARS if char in query)}"
        )

    if any(keyword in query.lower() for keyword in FORBIDDEN_KEYWORDS):
        raise SparkQueryError(
            f"Query {query} contains forbidden keyword: {', '.join(keyword for keyword in FORBIDDEN_KEYWORDS if keyword in query.lower())}"
        )

    return True


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


def _generate_cache_key(prefix: str, params: Dict[str, Any]) -> str:
    """
    Generate a cache key from a prefix and parameters.
    """
    # Convert parameters to a sorted JSON string to ensure consistency
    param_str = json.dumps(params, sort_keys=True)
    # Create a hash of the parameters to avoid very long keys
    param_hash = hashlib.md5(param_str.encode()).hexdigest()
    return f"{prefix}:{param_hash}"


def _get_from_cache(
    spark: SparkSession, cache_key: str
) -> Optional[List[Dict[str, Any]]]:
    """
    Try to get data from Redis cache.
    """
    try:
        cache_df = (
            spark.read.format("org.apache.spark.sql.redis")
            .option("table", cache_key)
            .option("key.column", "id")
            .load()
        )

        if not cache_df.rdd.isEmpty():
            first_row = cache_df.first()
            if first_row and "value" in first_row:
                return json.loads(first_row["value"])

    except Exception as e:
        # Log the error but don't fail the operation if caching fails
        logger.debug(f"Cache miss or error for key {cache_key}: {e}")

    return None


def _store_in_cache(
    spark: SparkSession,
    cache_key: str,
    data: List[Dict[str, Any]],
    ttl: int = CACHE_EXPIRY_SECONDS,
) -> None:
    """
    Store data in Redis cache.
    """
    try:
        json_data = json.dumps(data)

        cache_data = [(1, json_data)]
        cache_df = spark.createDataFrame(cache_data, ["id", "value"])

        # Write to Redis
        (
            cache_df.write.format("org.apache.spark.sql.redis")
            .option("table", cache_key)
            .option("key.column", "id")
            .option("ttl", ttl)
            .mode("overwrite")
            .save()
        )

        logger.debug(f"Cached data under key {cache_key} with TTL {ttl}s")
    except Exception as e:
        # Log the error but don't fail the operation if caching fails
        logger.warning(f"Failed to cache data under key {cache_key}: {e}")


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
            equivalent_query = f"SELECT * FROM {full_table_name} WHERE {where_clause}"
            _check_query_is_valid(equivalent_query)
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


def query_delta_table(spark: SparkSession, query: str) -> List[Dict[str, Any]]:
    """
    Executes a SQL query against a specific Delta table after basic validation.

    Args:
        spark: The SparkSession object.
        query: The SQL query string to execute.

    Returns:
        A list of dictionaries, where each dictionary represents a row.
    """

    _check_query_is_valid(query)

    logger.info(f"Executing validated query: {query}")
    try:
        df = spark.sql(query)
        results = [row.asDict() for row in df.collect()]
        logger.info(f"Query returned {len(results)} rows.")
        return results
    except Exception as e:
        logger.error(f"Error executing query: {e}")
        raise SparkOperationError(f"Failed to execute query: {str(e)}")
