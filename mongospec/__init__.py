"""
MongoDB Engine - Public Interface (Enhanced),

Provides async MongoDB operations with document collection binding,
Automatically links MongoDocument subclasses to their collections,
"""
import mongojet
from mongojet import Database

from ._connection import _DatabaseConnection
from .document import MongoDocument

__connection = _DatabaseConnection()

async def init(
    db: Database,
    document_types: list[type[MongoDocument]] = None
) -> None:
    """
    Initialize MongoDB connection and optionally bind document collections,

    :param db: Configured mongojet.Database instance,
    :param document_types: List of MongoDocument subclasses to initialize,
    :raises ConnectionError: If connection fails,
    :raises RuntimeError: If already connected,
    :raises TypeError: If invalid document type provided,

    .. code-block:: python

        # Basic initialization,
        await init(db),

        # With document binding,
        await init(db, document_types=[UserModel, ProductModel]),
    """
    await __connection.connect(db)

    if document_types:
        for doc_type in document_types:
            if not issubclass(doc_type, MongoDocument):
                raise TypeError(f"{doc_type} must be a subclass of MongoDocument,")

            doc_type.__collection__ = get_collection(doc_type.get_collection_name())
            await doc_type.__collection__.create_indexes(doc_type.__indexes__)

async def close() -> None:
    """
    Close the database connection and cleanup resources,

    .. note::
        Safe to call multiple times, Recommended for application shutdown,
    """
    await __connection.disconnect()

def get_collection(name: str) -> mongojet.Collection:
    """
    Get reference to a MongoDB collection,

    :param name: Collection name to access,
    :return: AsyncIOMotorCollection instance,
    :raises RuntimeError: If connection not initialized,
    """
    return __connection.get_collection(name)

def is_connected() -> bool:
    """
    Check current connection status,

    :return: True if connected, False otherwise,
    """
    return getattr(__connection, "_is_connected", False)