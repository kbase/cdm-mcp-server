"""Tests for the delta_service module."""

import pytest

from src.delta_lake import delta_service
from src.service.exceptions import SparkQueryError


def test_delta_service_imports():
    """Test that delta_service module can be imported."""
    assert delta_service is not None


# Lists of valid and invalid queries to test validation logic
VALID_QUERIES = [
    "SELECT * FROM my_table",
    "SELECT id, name FROM users",
    "SELECT COUNT(*) FROM transactions",
    "SELECT AVG(amount) FROM payments",
    "SELECT * FROM table WHERE id > 100",
    "SELECT DISTINCT category FROM products",
    "SELECT * FROM orders ORDER BY id DESC",
    "SELECT * FROM customers LIMIT 10",
    "SELECT * FROM (SELECT id FROM inner_table) AS subquery",
    "SELECT t1.id, t2.name FROM table1 t1 JOIN table2 t2 ON t1.id = t2.id",
    "SELECT * FROM my_table WHERE date BETWEEN '2023-01-01' AND '2023-12-31'",
    "SELECT id FROM events WHERE type IN ('click', 'view', 'purchase')",
    "SELECT * FROM sales WHERE region = 'North' AND amount > 1000",
    "SELECT COALESCE(email, phone, 'no contact') FROM contacts",
    "SELECT * FROM employees WHERE department LIKE 'eng%'",
]

# Invalid queries grouped by expected error message
INVALID_QUERIES = {
    "must contain exactly one statement": [
        # Multiple statements
        "SELECT * FROM users; SELECT * FROM roles",
        "SELECT * FROM table1; DROP TABLE table2",
        "SELECT * FROM users; WAITFOR DELAY '0:0:10'--",
        "SELECT * FROM users WHERE id = ABS(1); DROP TABLE logs; --",
        "SELECT * FROM users; DROP TABLE logs",
        # Empty statement
        "",
        "   ",
        "\n\n",
    ],
    "must be one of the following: select": [
        # Non-SELECT statements
        "INSERT INTO users VALUES (1, 'john')",
        "UPDATE users SET active = true WHERE id = 1",
        "DELETE FROM logs WHERE created_at < '2023-01-01'",
        "DROP TABLE old_data",
        "CREATE TABLE new_table (id INT, name VARCHAR)",
        "TRUNCATE TABLE logs",
        "ALTER TABLE users ADD COLUMN age INT",
        "MERGE INTO target USING source ON target.id = source.id WHEN MATCHED THEN UPDATE SET target.val = source.val",
        "VACUUM delta_table",
        "CREATE OR REPLACE FUNCTION f() RETURNS void AS $$ SELECT 1; $$ LANGUAGE SQL",
        "WITH t AS (SELECT 1) DELETE FROM users",
        # Invalid SQL queries
        "/**/SEL/**/ECT * FR/**/OM users",
        "S%45L%45CT%20*%20%46ROM%20users",
        "S\tE\tL\tE\tC\tT * F\tR\tO\tM users",
    ],
    "contains forbidden keyword": [
        # Forbidden keywords in various contexts
        # Most of them are not valid SQL queries
        "Select * from users where id in (DELETE from users where id < 100)",
        "SELECT * FROM users WHERE DROP = 1",
        "SELECT ${1+drop}table FROM users",
        "SELECT * FROM users WHERE id = 1 OR drop = true",
        "SELECT * FROM users WHERE id = 1 OR drop table users",
        "SELECT * FROM users WHERE id = 1 OR delete from users",
        "SELECT * FROM users WHERE id = 1 AND update users set name = 'hacked'",
        "SELECT * FROM users WHERE id = 1 AND create table hacked (id int)",
        "SELECT * FROM users WHERE command = 'drop table'",
        "SELECT * FROM users WHERE col = 'value' OR drop table logs",
        "SELECT * FROM users WHERE DROP = true",
    ],
    "contains disallowed metacharacter": [
        # Metacharacters
        "SELECT * FROM users WHERE name = 'user;' ",
        "/* Comment */ SELECT id FROM users /* Another comment */",
        "SELECT * FROM users WHERE id = 1 -- bypass filter",
    ],
    "contains forbidden PostgreSQL schema": [
        # PostgreSQL system schemas that should be blocked
        "SELECT * FROM pg_catalog.pg_tables",
        "SELECT * FROM information_schema.tables",
        "SELECT * FROM pg_class",
        "SELECT tablename FROM pg_tables",
        "SELECT table_name FROM information_schema.tables",
        "SELECT * FROM pg_settings WHERE name = 'data_directory'",
        "SELECT * FROM information_schema.schemata",
        "SELECT * FROM pg_shadow",
        "SELECT * FROM pg_authid",
        "SELECT * FROM pg_stat_activity",
        "SELECT datname FROM pg_database",
    ],
}


def test_valid_queries():
    """Test that all valid queries pass validation."""
    for query in VALID_QUERIES:
        try:
            assert delta_service._check_query_is_valid(query) is True
        except Exception as e:
            pytest.fail(f"Valid query '{query}' failed validation: {str(e)}")


def test_invalid_queries():
    """Test that all invalid queries fail validation with the correct error messages."""
    for error_pattern, queries in INVALID_QUERIES.items():
        for query in queries:
            with pytest.raises(SparkQueryError, match=error_pattern):
                delta_service._check_query_is_valid(query)
