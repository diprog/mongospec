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
    _is_connected: bool = False

    def __new__(cls) -> Self:
        """
        Enforce singleton pattern.

        :returns: Single instance of _DatabaseConnection.
        """
        if cls._instance is None:
            cls._instance = super().__new__(cls)
            logger.debug("Created new database singleton instance")
        return cls._instance

    async def connect(self, db: mongojet.Database) -> None:
        """
        Verify connection and initialize internal state.

        :param db: mongojet.Database instance.
        :raises ConnectionError: If the ping command fails.
        """
        try:
            await db.run_command({"ping": 1})
        except Exception as e:
            logger.error(f"Failed to connect to MongoDB: {e}")
            raise ConnectionError("Unable to connect to MongoDB") from e

        self._db = db
        self._client = db.client
        self._is_connected = True
        logger.info("Successfully connected to MongoDB")

    async def disconnect(self) -> None:
        """
        Close database connection and cleanup resources.

        .. note::
            Safe to call even when not connected.
            Logs but suppresses any errors during disconnect.
        """
        if self._is_connected and self._client is not None:
            try:
                await self._client.close()
            except Exception as e:
                logger.warning(f"Error while disconnecting: {e}")
            finally:
                self._client = None
                self._db = None
                self._is_connected = False
                logger.info("Database connection closed")

    def get_collection(self, name: str) -> mongojet.Collection:
        """
        Get reference to a MongoDB collection.

        :param name: Name of the collection.
        :returns: mongojet.Collection instance.
        :raises RuntimeError: If not connected to database.
        """
        if not self._is_connected or self._db is None:
            raise RuntimeError("Database connection not initialized")
        return self._db.get_collection(name)
