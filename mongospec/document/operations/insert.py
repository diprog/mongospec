"""
Insert operations mixin for MongoDocument.

Provides all document insertion capabilities including:
- Single document insertion
- Bulk document insertion
- Insert with validation
- Recursive graph insertion with rollback support
"""

from collections.abc import Sequence
from dataclasses import dataclass, field
from typing import TYPE_CHECKING, Generic, Unpack

from bson import ObjectId
from mongojet._types import DeleteOptions, Document, InsertManyOptions, InsertOneOptions

from .base import BaseOperations, T

if TYPE_CHECKING:
    from mongospec.document.document import MongoDocument


@dataclass(slots=True)
class RollbackFailure:
    """Information about a single document rollback failure."""

    document: "MongoDocument"
    error: Exception


@dataclass(slots=True)
class RecursiveInsertResult(Generic[T]):
    """Result of a recursive document graph insertion."""

    document: T
    created_documents: list["MongoDocument"] = field(default_factory=list)
    _created_ids: set[int] = field(default_factory=set, init=False, repr=False)

    def add_created(self, document: "MongoDocument") -> None:
        """Register a document created by the current operation."""

        key = id(document)
        if key in self._created_ids:
            return
        self._created_ids.add(key)
        self.created_documents.append(document)

    async def rollback(self, **kwargs: Unpack[DeleteOptions]) -> int:
        """Roll back all documents created by this recursive insertion.

        :param kwargs: Additional arguments for delete_one()
        :return: Number of successfully deleted documents
        :raises RecursiveRollbackError: If some documents could not be deleted
        """

        deleted_count = 0
        failures: list[RollbackFailure] = []

        for document in reversed(self.created_documents):
            if document._id is None:
                continue

            try:
                deleted_count += await document.delete(**kwargs)
                document._id = None
            except Exception as exc:
                failures.append(RollbackFailure(document=document, error=exc))

        if failures:
            raise RecursiveRollbackError(
                result=self,
                failures=failures,
                deleted_count=deleted_count,
            )

        return deleted_count


class RecursiveRollbackError(RuntimeError):
    """Error rolling back documents created by a recursive insertion."""

    def __init__(
        self,
        *,
        result: RecursiveInsertResult,
        failures: list[RollbackFailure],
        deleted_count: int,
    ) -> None:
        self.result = result
        self.failures = tuple(failures)
        self.deleted_count = deleted_count

        documents = ", ".join(
            f"{failure.document.__class__.__name__}({_format_document_id(failure.document)})"
            for failure in failures
        )
        super().__init__(
            f"Failed to rollback {len(failures)} recursively inserted documents: {documents}"
        )


class RecursiveInsertError(RuntimeError):
    """Recursive insertion error with access to partial result."""

    def __init__(
        self,
        *,
        document: "MongoDocument",
        result: RecursiveInsertResult,
        cause: Exception | None = None,
        rollback_error: RecursiveRollbackError | None = None,
    ) -> None:
        self.document = document
        self.result = result
        self.cause = cause
        self.rollback_error = rollback_error

        message = f"Failed to recursively insert {document.__class__.__name__}"
        if rollback_error is not None:
            message += "; rollback also failed"
        elif result.created_documents:
            message += "; created documents were rolled back"
        elif cause is not None:
            message += f": {cause}"

        super().__init__(message)


def _format_document_id(document: "MongoDocument") -> str:
    return str(document._id) if document._id is not None else "unsaved"


async def _insert_recursive(
    document: "MongoDocument",
    *,
    result: RecursiveInsertResult,
    active_chain: list["MongoDocument"],
    **kwargs: Unpack[InsertOneOptions],
) -> None:
    from mongospec.refs import is_document_type

    if document._id is not None:
        return

    document_key = id(document)
    active_keys = {id(item) for item in active_chain}
    if document_key in active_keys:
        cycle = " -> ".join(
            [item.__class__.__name__ for item in [*active_chain, document]]
        )
        raise ValueError(
            f"Cannot recursively insert cyclic unsaved references: {cycle}"
        )

    active_chain.append(document)
    try:
        from mongospec.refs import iter_ref_documents

        for ref_document in iter_ref_documents(document._get_ref_fields(), document):
            if is_document_type(type(ref_document)) and ref_document._id is None:
                await _insert_recursive(
                    ref_document,
                    result=result,
                    active_chain=active_chain,
                    **kwargs,
                )

        await document.insert(**kwargs)
        result.add_created(document)
    finally:
        active_chain.pop()


class InsertOperationsMixin(BaseOperations):
    """Mixin class providing all insert operations for MongoDocument"""

    async def insert(self: T, **kwargs: Unpack[InsertOneOptions]) -> T:
        """
        Insert the current document instance into its collection.

        :param kwargs: Additional arguments passed to insert_one()
        :return: The inserted document with _id populated
        :raises TypeError: If document validation fails
        :raises ValueError: If any referenced document is unsaved
        :raises RuntimeError: If collection not initialized

        .. code-block:: python

            # Basic insertion
            user = User(name="Alice")
            await user.insert()

            # With additional options
            await user.insert(bypass_document_validation=True)
        """
        self._validate_document_type(self)
        self._validate_refs()
        self.__pre_save__()
        result = await self._get_collection().insert_one(
            self.dump(),
            **kwargs
        )
        self._id = result["inserted_id"]
        return self

    async def insert_recursive(self: T, **kwargs: Unpack[InsertOneOptions]) -> RecursiveInsertResult[T]:
        """Recursively insert the document and all its unsaved references.

        Unsaved child documents are inserted before the parent. Already saved
        references are left untouched. If the operation fails after a partial
        insertion, a best-effort rollback is performed and
        ``RecursiveInsertError`` is raised.

        :param kwargs: Additional arguments passed to insert_one()
        :return: Result with list of created documents and rollback()
        :raises ValueError: If root document is already saved or a cycle exists
        :raises RecursiveInsertError: If insertion or rollback fails
        """

        self._validate_document_type(self)
        if self._id is not None:
            raise ValueError("Recursive insert requires an unsaved root document")

        result = RecursiveInsertResult(document=self)
        try:
            await _insert_recursive(
                self,
                result=result,
                active_chain=[],
                **kwargs,
            )
        except Exception as exc:
            rollback_error = None
            if result.created_documents:
                try:
                    await result.rollback()
                except RecursiveRollbackError as rollback_exc:
                    rollback_error = rollback_exc

            raise RecursiveInsertError(
                document=self,
                result=result,
                cause=exc,
                rollback_error=rollback_error,
            ) from exc

        return result

    @classmethod
    async def insert_one(
            cls: type[T],
            document: T,
            **kwargs: Unpack[InsertOneOptions]
    ) -> T:
        """
        Insert a single document into the collection.

        :param document: Document instance to insert
        :param kwargs: Additional arguments passed to insert_one()
        :return: Inserted document with _id populated
        :raises TypeError: If document validation fails
        :raises ValueError: If any referenced document is unsaved
        :raises RuntimeError: If collection not initialized

        .. code-block:: python

            # Insert with explicit document
            await User.insert_one(User(name="Bob"))
        """
        cls._validate_document_type(document)
        document._validate_refs()
        document.__pre_save__()
        result = await cls._get_collection().insert_one(
            document.dump(),
            **kwargs
        )
        document._id = result["inserted_id"]
        return document

    @classmethod
    async def insert_one_recursive(
            cls: type[T],
            document: T,
            **kwargs: Unpack[InsertOneOptions]
    ) -> RecursiveInsertResult[T]:
        """Recursively insert a single document with its unsaved references.

        :param document: Root document to insert
        :param kwargs: Additional arguments passed to insert_one()
        :return: Result with list of created documents and rollback()
        """

        cls._validate_document_type(document)
        return await document.insert_recursive(**kwargs)

    @classmethod
    async def insert_many(
            cls: type[T],
            documents: list[T],
            **kwargs: Unpack[InsertManyOptions]
    ) -> Sequence[ObjectId]:
        """
        Insert multiple documents into the collection.

        :param documents: List of document instances to insert
        :param kwargs: Additional arguments passed to insert_many()
        :return: List of inserted _ids
        :raises TypeError: If any document validation fails
        :raises RuntimeError: If collection not initialized

        .. code-block:: python

            # Bulk insert
            users = [User(name=f"User_{i}") for i in range(10)]
            ids = await User.insert_many(users)
        """
        if not all(isinstance(d, cls) for d in documents):
            raise TypeError(f"All documents must be of type {cls.__name__}")

        for d in documents:
            d.__pre_save__()

        result = await cls._get_collection().insert_many(
            [d.dump() for d in documents],
            **kwargs
        )

        # Update documents with their new _ids
        for doc, doc_id in zip(documents, result["inserted_ids"]):
            doc._id = doc_id

        return result["inserted_ids"]

    @classmethod
    async def insert_if_not_exists(
            cls: type[T],
            document: T,
            filter: Document | str | None = None,
            **kwargs: Unpack[InsertOneOptions]
    ) -> T | None:
        """
        Insert document only if matching document doesn't exist.

        :param document: Document instance to insert
        :param filter: Custom filter to check existence (default uses _id)
        :param kwargs: Additional arguments passed to insert_one()
        :return: Inserted document if inserted, None if already exists
        :raises TypeError: If document validation fails
        :raises RuntimeError: If collection not initialized

        .. code-block:: python

            # Insert only if email doesn't exist
            user = User(name="Alice", email="alice@example.com")
            await User.insert_if_not_exists(
                user,
                filter={"email": "alice@example.com"}
            )
        """
        cls._validate_document_type(document)

        search_filter = filter or {"_id": document._id} if document._id else None
        if search_filter is None:
            raise ValueError("Must provide either filter or document with _id")

        existing = await cls.find_one(search_filter)
        if existing:
            return None

        return await cls.insert_one(document, **kwargs)
