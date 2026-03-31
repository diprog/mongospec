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


def _load_single(cls: type[T], raw: dict[str, Any], resolve_refs: bool) -> T:
    """Load a single raw document, handling ref fields based on resolve_refs."""
    ref_fields = cls._get_ref_fields()
    if not ref_fields or resolve_refs:
        # resolve_refs=True: refs already replaced with full dicts by caller
        return cls.load(raw)
    # resolve_refs=False: replace ObjectIds with stubs so msgspec.convert succeeds
    from mongospec.refs import stub_ref_data
    return cls.load(stub_ref_data(ref_fields, raw))


def _load_many(cls: type[T], docs: list[dict[str, Any]], resolve_refs: bool) -> list[T]:
    """Load a batch of raw documents, handling ref fields based on resolve_refs."""
    ref_fields = cls._get_ref_fields()
    if not ref_fields or resolve_refs:
        return [cls.load(d) for d in docs]
    from mongospec.refs import stub_ref_data
    return [cls.load(stub_ref_data(ref_fields, d)) for d in docs]


class AsyncDocumentCursor:
    def __init__(self, cursor: Cursor, document_class: type[T], *, resolve_refs: bool = True) -> None:
        self._cursor = cursor
        self.document_class = document_class
        self._resolve_refs = resolve_refs

    def __aiter__(self) -> Self:
        return self

    async def __anext__(self) -> T:
        doc = await self._cursor.__anext__()
        if self._resolve_refs:
            ref_fields = self.document_class._get_ref_fields()
            if ref_fields:
                from mongospec.refs import resolve_ref_data
                doc = await resolve_ref_data(ref_fields, doc)
        return _load_single(self.document_class, doc, self._resolve_refs)

    async def to_list(self, length: int | None = None) -> list[T]:
        """
        Convert cursor results to a list of documents.

        :param length: Maximum number of documents to return. None means no limit.
        :return: List of document instances
        """
        docs = await self._cursor.to_list(length)
        if self._resolve_refs:
            ref_fields = self.document_class._get_ref_fields()
            if ref_fields:
                from mongospec.refs import resolve_ref_data_batch
                docs = await resolve_ref_data_batch(ref_fields, docs)
        return _load_many(self.document_class, docs, self._resolve_refs)


class DeferredCursor:
    """Wrapper over a find() coroutine enabling chainable calls.

    Supports multiple usage patterns::

        # Option 1: chainable (recommended)
        users = await User.find({"active": True}).to_list()

        # Option 2: await then async for
        cursor = await User.find({"active": True})
        async for user in cursor:
            ...

        # Option 3: direct async for
        async for user in User.find({"active": True}):
            ...
    """

    def __init__(self, coro) -> None:
        self._coro = coro
        self._cursor: AsyncDocumentCursor | None = None

    def __await__(self):
        return self._coro.__await__()

    async def _resolve(self) -> AsyncDocumentCursor:
        if self._cursor is None:
            self._cursor = await self._coro
        return self._cursor

    def __aiter__(self):
        return self._aiter_impl()

    async def _aiter_impl(self):
        cursor = await self._resolve()
        async for item in cursor:
            yield item

    async def to_list(self, length: int | None = None) -> list:
        """Collect all results into a list.

        :param length: Maximum number of documents. None means no limit.
        :returns: List of documents.
        """
        cursor = await self._resolve()
        return await cursor.to_list(length)


# noinspection PyShadowingBuiltins
class FindOperationsMixin(BaseOperations):
    """Mixin class providing all find operations for MongoDocument"""

    @classmethod
    async def find_one(
        cls: type[T],
        filter: Document | str | None = None,
        *,
        resolve_refs: bool = True,
        **kwargs: Unpack[FindOneOptions],
    ) -> T | None:
        """
        Find single document matching the query filter.

        :param filter: MongoDB query filter
        :param resolve_refs: Resolve MongoDocument reference fields (default: True)
        :param kwargs: Additional arguments for find_one()
        :return: Document instance or None if not found
        """
        doc = await cls._get_collection().find_one(filter or {}, **kwargs)
        if doc is None:
            return None
        if resolve_refs:
            ref_fields = cls._get_ref_fields()
            if ref_fields:
                from mongospec.refs import resolve_ref_data
                doc = await resolve_ref_data(ref_fields, doc)
        return _load_single(cls, doc, resolve_refs)

    @classmethod
    async def find_by_id(
        cls: type[T],
        document_id: ObjectId | str,
        *,
        resolve_refs: bool = True,
        **kwargs: Unpack[FindOneOptions],
    ) -> T | None:
        """
        Find document by its _id.

        :param document_id: Document ID as ObjectId or string
        :param resolve_refs: Resolve MongoDocument reference fields (default: True)
        :param kwargs: Additional arguments for find_one()
        :return: Document instance or None if not found
        """
        if isinstance(document_id, str):
            document_id = ObjectId(document_id)
        return await cls.find_one({"_id": document_id}, resolve_refs=resolve_refs, **kwargs)

    @classmethod
    def find(
        cls: type[T],
        filter: Document | None = None,
        *,
        resolve_refs: bool = True,
        **kwargs: Unpack[FindOptions],
    ) -> DeferredCursor:
        """Create a cursor for query results.

        Supports chainable calls::

            # Collect into a list
            users = await User.find({"age": {"$gt": 30}}).to_list()

            # Async iteration
            async for user in User.find({"age": {"$gt": 30}}):
                process(user)

            # Await then to_list
            cursor = await User.find({"age": {"$gt": 30}})
            users = await cursor.to_list()

        :param filter: MongoDB query filter
        :param resolve_refs: Resolve MongoDocument reference fields (default: True)
        :param kwargs: Additional arguments for find()
        :returns: DeferredCursor — awaitable object with .to_list() method
        """
        async def _create_cursor():
            cursor = await cls._get_collection().find(filter or {}, **kwargs)
            return AsyncDocumentCursor(cursor, cls, resolve_refs=resolve_refs)

        return DeferredCursor(_create_cursor())

    @classmethod
    async def find_all(
        cls: type[T],
        filter: Document | None = None,
        *,
        resolve_refs: bool = True,
        **kwargs: Unpack[FindOptions],
    ) -> list[T]:
        """
        Retrieve all documents matching the filter as a list.

        This helper issues a single query and collects the entire result set into memory.
        Prefer :meth:`find` for streaming iteration when dealing with large collections.

        :param filter: MongoDB query filter. None selects all documents.
        :param resolve_refs: Resolve MongoDocument reference fields (default: True)
        :param kwargs: Additional FindOptions passed to ``find()`` (e.g., projection, sort, limit, batch_size).
        :returns: List of loaded document instances.
        :warning: Loads the full result set in memory; may be slow or exhaust memory on large collections.
        """
        cursor = await cls._get_collection().find(filter, **kwargs)
        docs = await cursor.to_list()
        if resolve_refs:
            ref_fields = cls._get_ref_fields()
            if ref_fields:
                from mongospec.refs import resolve_ref_data_batch
                docs = await resolve_ref_data_batch(ref_fields, docs)
        return _load_many(cls, docs, resolve_refs)

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
    ) -> Cursor[dict[str, Any]]:
        """
        Execute aggregation pipeline on collection.

        :param pipeline: Sequence of aggregation pipeline stages
        :param session: Optional client session for transaction support
        :param kwargs: Additional arguments for aggregate()
        :return: AsyncDocumentCursor instance for iteration over aggregation results
        """
        return await cls._get_collection().aggregate(pipeline, session, **kwargs)
