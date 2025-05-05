"""Tests for the spark_utils module."""

from src.delta_lake import spark_utils


def test_spark_utils_imports():
    """Test that spark_utils module can be imported."""
    assert spark_utils is not None


def test_noop():
    """Simple placeholder test."""
    assert 1 == 1
