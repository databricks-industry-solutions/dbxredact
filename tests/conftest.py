"""Pytest configuration and fixtures for dbxredact tests."""

import pytest

# Pre-import pyspark submodules so they occupy sys.modules before test files
# that conditionally mock them with MagicMock (test_ai_detector, test_config, etc.).
import pyspark.sql.functions  # noqa: F401
import pyspark.sql.types  # noqa: F401
import pyspark.sql.streaming  # noqa: F401
from pyspark.sql import SparkSession


@pytest.fixture(scope="session")
def spark():
    """Session-scoped local SparkSession for unit and integration tests."""
    session = (
        SparkSession.builder
        .master("local[2]")
        .appName("dbxredact-tests")
        .config("spark.sql.shuffle.partitions", "2")
        .config("spark.default.parallelism", "2")
        .config("spark.ui.enabled", "false")
        .getOrCreate()
    )
    yield session
    session.stop()


@pytest.fixture
def sample_presidio_entities():
    """Sample Presidio detection results."""
    return [
        {
            "entity": "John Smith",
            "start": 0,
            "end": 10,
            "entity_type": "PERSON",
            "score": 0.95,
        },
        {
            "entity": "test@email.com",
            "start": 20,
            "end": 34,
            "entity_type": "EMAIL",
            "score": 0.9,
        },
        {
            "entity": "555-1234",
            "start": 40,
            "end": 48,
            "entity_type": "PHONE_NUMBER",
            "score": 0.85,
        },
    ]


@pytest.fixture
def sample_ai_entities():
    """Sample AI detection results."""
    return [
        {"entity": "John Smith", "start": 0, "end": 10, "entity_type": "PERSON"},
        {"entity": "test@email.com", "start": 20, "end": 34, "entity_type": "EMAIL_ADDRESS"},
    ]


@pytest.fixture
def sample_text():
    """Sample text for testing."""
    return "John Smith emailed at test@email.com and called 555-1234."


@pytest.fixture
def sample_entities_for_redaction():
    """Sample entities formatted for redaction."""
    return [
        {"entity": "John Smith", "start": 0, "end": 9, "entity_type": "PERSON"},
        {"entity": "test@email.com", "start": 22, "end": 35, "entity_type": "EMAIL"},
    ]

