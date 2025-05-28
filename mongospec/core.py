"""
MongoDB Engine - Core Implementation

Provides async MongoDB operations with document collection binding,
Automatically links MongoDocument subclasses to their collections.
"""
import mongojet

from ._connection import _DatabaseConnection
from .document import MongoDocument

__connection = _DatabaseConnection()


async def init(
    db: mongojet.Database,
    document_types: list[type[MongoDocument]] = None
) -> None:
    """
    Initialize MongoDB connection and optionally bind document collections.

    :param db: Configured mongojet.Database instance.
    :param document_types: List of MongoDocument subclasses to initialize.
    :raises ConnectionError: If connection fails.
    :raises RuntimeError: If already connected.
    :raises TypeError: If invalid document type provided.

    **Example:**

    .. code-block:: python

        # Basic initialization
        await init(db)

        # With document binding
        await init(db, document_types=[UserModel, ProductModel])
    """
    await __connection.connect(db)

    if document_types:
        for doc_type in document_types:
            if not issubclass(doc_type, MongoDocument):
                raise TypeError(f"{doc_type} must be a subclass of MongoDocument,")

            doc_type.__collection__ = __connection.get_collection(doc_type.get_collection_name())

            if doc_type.__indexes__:
                await doc_type.__collection__.create_indexes(doc_type.__indexes__)


async def close() -> None:
    """
    Close the database connection and cleanup resources.

    .. note::
        Safe to call multiple times. Recommended for application shutdown.
    """
    await __connection.disconnect()
