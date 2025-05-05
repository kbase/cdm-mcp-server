"""Module for interacting with Spark databases and tables.

This module provides functions to retrieve information about databases, tables,
and their schemas from a Spark cluster or directly from Hive metastore in PostgreSQL.
"""

## Mostly copied from https://github.com/kbase/cdm-jupyterhub/blob/main/src/spark/data_store.py

import json
from typing import Any, Dict, List, Optional, Union

from pyspark.sql import SparkSession

from src.delta_lake.spark_utils import get_spark_session
from src.postgres import hive_metastore
from src.service.exceptions import DeltaSchemaError


def _execute_with_spark(
    func: Any, spark: Optional[SparkSession] = None, *args, **kwargs
) -> Any:
    """
    Execute a function with a SparkSession, creating one if not provided.
    """
    if spark is None:
        with get_spark_session() as spark:
            return func(spark, *args, **kwargs)
    return func(spark, *args, **kwargs)


def _format_output(data: Any, return_json: bool = True) -> Union[str, Any]:
    """
    Format the output based on the return_json flag.
    """
    return json.dumps(data) if return_json else data


def _get_tables_with_schemas(
    db: str, tables: List[str], spark: SparkSession
) -> Dict[str, Any]:
    """
    Get schemas for a list of tables in a database.
    """
    return {
        table: get_table_schema(
            database=db, table=table, spark=spark, return_json=False
        )
        for table in tables
    }


def get_databases(
    spark: Optional[SparkSession] = None,
    use_postgres: bool = True,
    return_json: bool = True,
) -> Union[str, List[str]]:
    """
    Get the list of databases in the Hive metastore.

    Args:
        spark: Optional SparkSession to use (if use_postgres is False)
        use_postgres: Whether to use PostgreSQL direct query (faster) or Spark
        return_json: Whether to return JSON string or raw data

    Returns:
        List of database names, either as JSON string or raw list
    """

    def _get_dbs(session: SparkSession) -> List[str]:
        return [db.name for db in session.catalog.listDatabases()]

    if use_postgres:
        databases = hive_metastore.get_databases()
    else:
        databases = _execute_with_spark(_get_dbs, spark)

    return _format_output(databases, return_json)


def get_tables(
    database: str,
    spark: Optional[SparkSession] = None,
    use_postgres: bool = True,
    return_json: bool = True,
) -> Union[str, List[str]]:
    """
    Get the list of tables in a specific database.

    Args:
        database: Name of the database
        spark: Optional SparkSession to use (if use_postgres is False)
        use_postgres: Whether to use PostgreSQL direct query (faster) or Spark
        return_json: Whether to return JSON string or raw data

    Returns:
        List of table names, either as JSON string or raw list
    """

    def _get_tbls(session: SparkSession, db: str) -> List[str]:
        return [table.name for table in session.catalog.listTables(dbName=db)]

    if use_postgres:
        tables = hive_metastore.get_tables(database)
    else:
        tables = _execute_with_spark(_get_tbls, spark, database)

    return _format_output(tables, return_json)


def get_table_schema(
    database: str,
    table: str,
    spark: Optional[SparkSession] = None,
    return_json: bool = True,
) -> Union[str, List[str]]:
    """
    Get the schema of a specific table in a database.

    Args:
        database: Name of the database
        table: Name of the table
        spark: Optional SparkSession to use
        return_json: Whether to return JSON string or raw data

    Returns:
        List of column names, either as JSON string or raw list
    """

    def _get_schema(session: SparkSession, db: str, tbl: str) -> List[str]:
        try:
            return [
                column.name
                for column in session.catalog.listColumns(dbName=db, tableName=tbl)
            ]
        except Exception as e:
            raise DeltaSchemaError(
                f"Error retrieving schema for table {tbl} in database {db}: {e}"
            )

    columns = _execute_with_spark(_get_schema, spark, database, table)
    return _format_output(columns, return_json)


def get_db_structure(
    spark: Optional[SparkSession] = None,
    with_schema: bool = False,
    use_postgres: bool = True,
    return_json: bool = True,
) -> Union[str, Dict]:
    """Get the structure of all databases in the Hive metastore.

    Args:
        spark: Optional SparkSession to use for operations
        with_schema: Whether to include table schemas
        use_postgres: Whether to use PostgreSQL for metadata retrieval
        return_json: Whether to return the result as a JSON string

    Returns:
        Database structure as either JSON string or dictionary:
        {
            "database_name": ["table1", "table2"] or
            "database_name": {
                "table1": ["column1", "column2"],
                "table2": ["column1", "column2"]
            }
        }
    """

    def _get_structure(
        session: SparkSession,
    ) -> Dict[str, Union[List[str], Dict[str, List[str]]]]:
        db_structure = {}
        databases = get_databases(spark=session, return_json=False)

        for db in databases:
            tables = get_tables(database=db, spark=session, return_json=False)
            if with_schema and isinstance(tables, list):
                db_structure[db] = _get_tables_with_schemas(db, tables, session)
            else:
                db_structure[db] = tables

        return db_structure

    if use_postgres:
        db_structure = {}
        databases = hive_metastore.get_databases()

        for db in databases:
            tables = hive_metastore.get_tables(db)
            if with_schema and isinstance(tables, list):
                if spark is None:
                    with get_spark_session() as spark:
                        db_structure[db] = _get_tables_with_schemas(db, tables, spark)
                else:
                    db_structure[db] = _get_tables_with_schemas(db, tables, spark)
            else:
                db_structure[db] = tables

    else:
        db_structure = _execute_with_spark(_get_structure, spark)

    return _format_output(db_structure, return_json)


def database_exists(
    database: str,
    spark: Optional[SparkSession] = None,
    use_postgres: bool = True,
) -> bool:
    """
    Check if a database exists in the Hive metastore.
    """
    return database in get_databases(
        spark=spark, use_postgres=use_postgres, return_json=False
    )


def table_exists(
    database: str,
    table: str,
    spark: Optional[SparkSession] = None,
    use_postgres: bool = True,
) -> bool:
    """
    Check if a table exists in a database.
    """
    return table in get_tables(
        database=database,
        spark=spark,
        use_postgres=use_postgres,
        return_json=False,
    )
