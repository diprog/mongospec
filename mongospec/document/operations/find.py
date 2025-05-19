"""
Find operations mixin for MongoDocument.

Provides query capabilities with efficient async iteration for large result sets.
Includes cursor management to prevent memory overflows.
"""

from typing import Any, Self

from bson import ObjectId
from mongojet._cursor import Cursor

from .base import BaseOperations, T


class AsyncDocumentCursor:
    def __init__(
            self,
            cursor: Cursor,
            document_class: type[T],
            batch_size: int | None = None
    ) -> None:
        self._cursor = cursor
        self.document_class = document_class
        self.batch_size = batch_size

    def __aiter__(self) -> Self:
        return self

    async def __anext__(self) -> T:
        try:
            # The mongojet Cursor already implements __anext__ with StopAsyncIteration
            doc = await self._cursor.__anext__()
            return self.document_class.load(doc)
        except StopAsyncIteration:
            raise

    async def to_list(self, length: int | None = None) -> list[T]:
        """
        Convert cursor results to a list of documents.

        :param length: Maximum number of documents to return. None means no limit.
        :return: List of document instances
        """
        docs = await self._cursor.to_list(length)
        return [self.document_class.load(doc) for doc in docs]


class FindOperationsMixin(BaseOperations):
    """Mixin class providing all find operations for MongoDocument"""

    @classmethod
    async def find_one(
            cls: type[T],
            filter: dict | None = None,
            **kwargs: Any
    ) -> T | None:
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
            cls: type[T],
            document_id: ObjectId | str,
            **kwargs: Any
    ) -> T | None:
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
            cls,
            filter: dict | None = None,
            batch_size: int | None = None,
            **kwargs: Any
    ) -> AsyncDocumentCursor:
        """
        Create async cursor for query results.

        :param filter: MongoDB query filter
        :param batch_size: Number of documents per batch (default: None)
        :param kwargs: Additional arguments for find()
        :return: AsyncDocumentCursor instance for iteration

        Example::

            # Iterate over large result set efficiently
            async for user in User.find({"age": {"$gt": 30}}):
                process_user(user)
        """
        # Pass batch_size to the collection's find method if specified
        if batch_size is not None:
            kwargs['batch_size'] = batch_size

        cursor = await cls._get_collection().find(filter or {}, **kwargs)
        return AsyncDocumentCursor(cursor, cls, batch_size)

    @classmethod
    async def find_all(
            cls: type[T],
            **kwargs: Any
    ) -> list[T]:
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
            cls: type[T],
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
            cls: type[T],
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
