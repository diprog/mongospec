"""
Find operations mixin for MongoDocument.

Provides query capabilities with efficient async iteration for large result sets.
Includes cursor management to prevent memory overflows.
"""

from typing import Any, Self

from bson import ObjectId
from mongojet._cursor import Cursor

from .base import BaseOperations, TDocument


class AsyncDocumentCursor[TDocument]:
    """
    Async cursor wrapper for efficient document iteration.

    Fetches documents in batches and yields them individually.
    Maintains cursor state and cleans up resources automatically.
    """

    def __init__(
            self,
            cursor: Cursor,
            document_class: type[TDocument],
            batch_size: int = 100
    ) -> None:
        """
        Initialize async document cursor.

        :param cursor: Raw MongoDB cursor from find operation
        :param document_class: Document model class for deserialization
        :param batch_size: Number of documents to fetch per batch
        """
        self._cursor = cursor
        self.document_class = document_class
        self.batch_size = batch_size
        self._current_batch: list[Any] = []
        self._has_more = True

    def __aiter__(self) -> Self:
        return self

    async def __anext__(self) -> TDocument:
        while True:
            if self._current_batch:
                return self.document_class.load(self._current_batch.pop(0))

            if not self._has_more:
                raise StopAsyncIteration

            self._current_batch = await self._cursor.to_list(self.batch_size)
            self._has_more = bool(self._current_batch)

            if not self._current_batch:
                raise StopAsyncIteration


class FindOperationsMixin(BaseOperations):
    """Mixin class providing all find operations for MongoDocument"""

    @classmethod
    async def find_one(
            cls: type[TDocument],
            filter: dict | None = None,
            **kwargs: Any
    ) -> TDocument | None:
        """
        Find single document matching the query filter.

        :param filter: MongoDB query filter
        :param kwargs: Additional arguments for find_one()
        :return: Document instance or None if not found
        """
        doc = await cls._get_collection().find_one(filter or {}, **kwargs)
        return cls.load(doc) if doc else None

    @classmethod
    async def find_by_id(
            cls: type[TDocument],
            document_id: ObjectId | str,
            **kwargs: Any
    ) -> TDocument | None:
        """
        Find document by its _id.

        :param document_id: Document ID as ObjectId or string
        :param kwargs: Additional arguments for find_one()
        :return: Document instance or None if not found
        """
        if isinstance(document_id, str):
            document_id = ObjectId(document_id)
        return await cls.find_one({"_id": document_id}, **kwargs)

    @classmethod
    async def find(
            cls: type[TDocument],
            filter: dict | None = None,
            batch_size: int = 100,
            **kwargs: Any
    ) -> AsyncDocumentCursor[TDocument]:
        """
        Create async cursor for query results.

        :param filter: MongoDB query filter
        :param batch_size: Number of documents per batch (default: 100)
        :param kwargs: Additional arguments for find()
        :return: AsyncDocumentCursor instance for iteration

        .. code-block:: python

            # Iterate over large result set efficiently
            async for user in await User.find({"age": {"$gt": 30}}, batch_size=500):
                process_user(user)
        """
        cursor = await cls._get_collection().find(filter or {}, **kwargs)
        return AsyncDocumentCursor[TDocument](cursor, cls, batch_size)

    @classmethod
    async def find_all(
            cls: type[TDocument],
            **kwargs: Any
    ) -> list[TDocument]:
        """
        Retrieve all documents in collection (use with caution).

        :param kwargs: Additional arguments for find()
        :return: List of document instances
        :warning: Not recommended for large collections
        """
        cursor = cls._get_collection().find({}, **kwargs)
        docs = await cursor.to_list(None)  # None returns all documents
        return [cls.load(d) for d in docs]

    @classmethod
    async def count(
            cls: type[TDocument],
            filter: dict | None = None,
            **kwargs: Any
    ) -> int:
        """
        Count documents matching the filter.

        :param filter: MongoDB query filter
        :param kwargs: Additional arguments for count_documents()
        :return: Number of matching documents
        """
        return await cls._get_collection().count_documents(filter or {}, **kwargs)

    @classmethod
    async def exists(
            cls: type[TDocument],
            filter: dict,
            **kwargs: Any
    ) -> bool:
        """
        Check if any document matches the filter.

        :param filter: MongoDB query filter
        :param kwargs: Additional arguments for count_documents()
        :return: True if at least one match exists
        """
        count = await cls.count(filter, **kwargs)
        return count > 0
