"""
Find operations mixin for MongoDocument.

Provides query capabilities with efficient async iteration for large result sets.
Includes cursor management to prevent memory overflows.
"""

from typing import Any, Self, Unpack, Sequence

from bson import ObjectId
from mongojet._collection import FindOptions
from mongojet._cursor import Cursor
from mongojet._session import ClientSession
from mongojet._types import CountOptions, Document, FindOneOptions, AggregateOptions

from .base import BaseOperations, T


class AsyncDocumentCursor:
    def __init__(self, cursor: Cursor, document_class: type[T]) -> None:
        self._cursor = cursor
        self.document_class = document_class

    def __aiter__(self) -> Self:
        return self

    async def __anext__(self) -> T:
        doc = await self._cursor.__anext__()
        return self.document_class.load(doc)

    async def to_list(self, length: int | None = None) -> list[T]:
        """
        Convert cursor results to a list of documents.

        :param length: Maximum number of documents to return. None means no limit.
        :return: List of document instances
        """
        docs = await self._cursor.to_list(length)
        return [self.document_class.load(doc) for doc in docs]


# noinspection PyShadowingBuiltins
class FindOperationsMixin(BaseOperations):
    """Mixin class providing all find operations for MongoDocument"""

    @classmethod
    async def find_one(
        cls: type[T],
        filter: Document | str | None = None,
        **kwargs: Unpack[FindOneOptions],
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
        cls: type[T], document_id: ObjectId | str, **kwargs: Unpack[FindOneOptions]
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
        cls: type[T], filter: Document | None = None, **kwargs: Unpack[FindOptions]
    ) -> AsyncDocumentCursor:
        """
        Create async cursor for query results.

        :param filter: MongoDB query filter
        :param kwargs: Additional arguments for find()
        :return: AsyncDocumentCursor instance for iteration

        Example::

            # Iterate over large result set efficiently
            async for user in User.find({"age": {"$gt": 30}}):
                process_user(user)
        """
        cursor = await cls._get_collection().find(filter or {}, **kwargs)
        return AsyncDocumentCursor(cursor, cls)

    @classmethod
    async def find_all(cls: type[T], **kwargs: Unpack[FindOptions]) -> list[T]:
        """
        Retrieve all documents in collection (use with caution).

        :param kwargs: Additional arguments for find()
        :return: List of document instances
        :warning: Not recommended for large collections
        """
        cursor = await cls._get_collection().find({}, **kwargs)
        docs = await cursor.to_list(None)  # None returns all documents
        return [cls.load(d) for d in docs]

    @classmethod
    async def count(
        cls: type[T], filter: Document | None = None, **kwargs: Unpack[CountOptions]
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
        cls: type[T], filter: dict[str, Any], **kwargs: Unpack[CountOptions]
    ) -> bool:
        """
        Check if any document matches the filter.

        :param filter: MongoDB query filter
        :param kwargs: Additional arguments for count_documents()
        :return: True if at least one match exists
        """
        count = await cls.count(filter, **kwargs)
        return count > 0

    @classmethod
    async def aggregate(
        cls: type[T],
        pipeline: Sequence[Document],
        session: ClientSession | None = None,
        **kwargs: Unpack[AggregateOptions],
    ) -> AsyncDocumentCursor:
        """
        Execute aggregation pipeline on collection.

        :param pipeline: Sequence of aggregation pipeline stages
        :param session: Optional client session for transaction support
        :param kwargs: Additional arguments for aggregate()
        :return: AsyncDocumentCursor instance for iteration over aggregation results

        Example::

            # Group documents by field and count
            pipeline = [
                {"$group": {"_id": "$status", "count": {"$sum": 1}}}
            ]
            cursor = await User.aggregate(pipeline)
            results = await cursor.to_list()

            # Complex aggregation with multiple stages
            pipeline = [
                {"$match": {"age": {"$gte": 18}}},
                {"$group": {"_id": "$city", "avg_age": {"$avg": "$age"}}},
                {"$sort": {"avg_age": -1}}
            ]
            async for result in User.aggregate(pipeline):
                process_aggregation(result)
        """
        cursor = await cls._get_collection().aggregate(pipeline, session, **kwargs)
        return AsyncDocumentCursor(cursor, cls)
