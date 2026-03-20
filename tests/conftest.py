"""Shared fixtures for mongospec tests.

Uses pytest-asyncio with asyncio_mode="auto" — no markers needed.
Session-scoped MongoDB client, function-scoped database for isolation.
"""
import pytest_asyncio
import mongojet
import mongospec
from mongospec._connection import _DatabaseConnection


@pytest_asyncio.fixture(scope="session", loop_scope="session")
async def mongo_client():
    """Single MongoDB client shared across the entire test session."""
    client = await mongojet.create_client("mongodb://127.0.0.1:27017")
    yield client
    await client.close()


@pytest_asyncio.fixture
async def db(mongo_client):
    """Clean database for each test, dropped after use."""
    database = mongo_client.get_database("mongospec_tests")
    yield database
    await database.drop()
    # Reset mongospec internal state without closing the shared client
    conn = _DatabaseConnection()
    conn._client = None
    conn._db = None
    conn._is_connected = False
