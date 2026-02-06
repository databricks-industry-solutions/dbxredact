"""Pytest configuration and fixtures for dbxredact tests."""

import pytest


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

