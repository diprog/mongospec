"""
Insert operations mixin for MongoDocument.

Provides all document insertion capabilities including:
- Single document insertion
- Bulk document insertion
- Insert with validation
"""

from typing import Any, Optional

from bson import ObjectId

from .base import BaseOperations, T


class InsertOperationsMixin(BaseOperations):
    """Mixin class providing all insert operations for MongoDocument"""

    async def insert(self: T, **kwargs: Any) -> T:
        """
        Insert the current document instance into its collection.

        :param kwargs: Additional arguments passed to insert_one()
        :return: The inserted document with _id populated
        :raises TypeError: If document validation fails
        :raises RuntimeError: If collection not initialized

        .. code-block:: python

            # Basic insertion
            user = User(name="Alice")
            await user.insert()

            # With additional options
            await user.insert(bypass_document_validation=True)
        """
        self._validate_document_type(self)
        result = await self._get_collection().insert_one(
            self.dump(),
            **kwargs
        )
        self._id = result["inserted_id"]
        return self

    @classmethod
    async def insert_one(
            cls: type[T],
            document: T,
            **kwargs: Any
    ) -> T:
        """
        Insert a single document into the collection.

        :param document: Document instance to insert
        :param kwargs: Additional arguments passed to insert_one()
        :return: Inserted document with _id populated
        :raises TypeError: If document validation fails
        :raises RuntimeError: If collection not initialized

        .. code-block:: python

            # Insert with explicit document
            await User.insert_one(User(name="Bob"))
        """
        cls._validate_document_type(document)
        result = await cls._get_collection().insert_one(
            document.dump(),
            **kwargs
        )
        document._id = result["inserted_id"]
        return document

    @classmethod
    async def insert_many(
            cls: type[T],
            documents: list[T],
            ordered: bool = True,
            **kwargs: Any
    ) -> list[ObjectId]:
        """
        Insert multiple documents into the collection.

        :param documents: List of document instances to insert
        :param ordered: If True (default), perform ordered insert
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

        result = await cls._get_collection().insert_many(
            [d.dump() for d in documents],
            ordered=ordered,
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
            filter: Optional[dict] = None,
            **kwargs: Any
    ) -> Optional[T]:
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
