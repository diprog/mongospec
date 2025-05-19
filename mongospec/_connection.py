"""
Internal database connection handler.

Implements singleton pattern for managing MongoDB connections.
This private module should only be used by the public interface.
"""
import logging
from typing import Self

import mongojet

logger = logging.getLogger(__name__)


class _DatabaseConnection:
    """
    Singleton MongoDB connection manager.

    Maintains connection state and provides collection access.
    Uses Motor for async MongoDB operations.

    .. warning::
        This is an internal class - use the public interface instead.
    """

    _instance: Self | None = None
    _client: mongojet.Client | None = None
    _db: mongojet.Database | None = None
    _is_connected = False

    def __new__(cls):
        """
        Enforce singleton pattern.

        :return: Single instance of _DatabaseConnection.
        """
        if cls._instance is None:
            cls._instance = super().__new__(cls)
            logger.debug("Created new database singleton instance")
        return cls._instance

    async def connect(
            self,
            db: mongojet.Database
    ) -> None:
        """
        Do nothing.
        Passing mongojet.Database means that an active client session was successfully created and connected.

        :param db: mongojet.Database instance.
        """
        self._db = db
        self._client = db.client
        self._is_connected = True

    async def disconnect(self) -> None:
        """
        Close database connection and cleanup resources.

        .. note::
            Safe to call even when not connected.
            Logs but suppresses any errors during disconnect.
        """
        if self._is_connected and self._client:
            await self._client.close()
            self._client = None
            self._db = None
            self._is_connected = False
            logger.info("Database connection closed")

    def get_collection(self, name) -> mongojet.Collection:
        """
        Get reference to a MongoDB collection.

        :param name: Name of the collection.
        :return: AsyncIOMotorCollection instance.
        :raises RuntimeError: If not connected to database.
        """
        if not self._is_connected or self._db is None:
            raise RuntimeError("Database connection not initialized")
        return self._db.get_collection(name)
