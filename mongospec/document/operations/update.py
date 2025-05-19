"""
Update operations mixin for MongoDocument.

Provides document modification capabilities including:
- Full document replacement
- Partial updates using MongoDB update operators
- Atomic field updates with concurrency control
"""

from typing import Any, Sequence, Unpack

from bson import ObjectId
from mongojet._types import Document, FindOneAndUpdateOptions, ReplaceOptions, UpdateOptions

from .base import BaseOperations, T


class UpdateOperationsMixin(BaseOperations):
    """Mixin class providing all update operations for MongoDocument"""

    async def save(self: T, upsert: bool = False, **kwargs: Unpack[ReplaceOptions]) -> T:
        """
        Persist document changes to the database.

        :param upsert: Insert document if it doesn't exist (default: False)
        :param kwargs: Additional arguments for replace_one()
        :return: Current document instance
        :raises ValueError: If _id missing and upsert=False
        :raises RuntimeError: If document not found and upsert=False

        .. code-block:: python

            # Modify and save existing document
            user = await User.find_one({"email": "alice@example.com"})
            user.name = "Alice Smith"
            await user.save()

            # Upsert new document
            new_user = User(name="Bob", email="bob@example.com")
            await new_user.save(upsert=True)
        """
        self._validate_document_type(self)

        if self._id is None:
            if upsert:
                return await self.insert(**kwargs)
            raise ValueError("Document requires _id for save without upsert")

        collection = self._get_collection()
        result = await collection.replace_one(
            {"_id": self._id},
            self.dump(),
            upsert=upsert,
            **kwargs
        )

        if not upsert and result["matched_count"] == 0:
            raise RuntimeError(f"Document {self._id} not found in collection")

        if result["upserted_id"]:
            self._id = result["upserted_id"]

        return self

    @classmethod
    async def update_one(
            cls: type[T],
            filter: Document,
            update: Document | Sequence[Document],
            **kwargs: Unpack[UpdateOptions]
    ) -> int:
        """
        Update single document matching the filter.

        :param filter: Query to match documents
        :param update: MongoDB update operations (e.g., {"$set": {"field": value}})
        :param kwargs: Additional arguments for update_one()
        :return: Number of modified documents

        .. code-block:: python

            # Increment user's login count
            await User.update_one(
                {"email": "alice@example.com"},
                {"$inc": {"login_count": 1}}
            )
        """
        result = await cls._get_collection().update_one(filter, update, **kwargs)
        return result["modified_count"]

    @classmethod
    async def update_many(
            cls: type[T],
            filter: Document,
            update: Document | Sequence[Document],
            **kwargs: Unpack[UpdateOptions]
    ) -> int:
        """
        Update multiple documents matching the filter.

        :param filter: Query to match documents
        :param update: MongoDB update operations
        :param kwargs: Additional arguments for update_many()
        :return: Number of modified documents
        """
        result = await cls._get_collection().update_many(filter, update, **kwargs)
        return result["modified_count"]

    @classmethod
    async def update_by_id(
            cls: type[T],
            document_id: ObjectId | str,
            update: Document | Sequence[Document],
            **kwargs: Unpack[UpdateOptions]
    ) -> int:
        """
        Update document by its ID with atomic operations.

        :param document_id: Document ID to update
        :param update: MongoDB update operations
        :param kwargs: Additional arguments for update_one()
        :return: Number of modified documents (0 or 1)

        .. code-block:: python

            # Update specific fields by ID
            await User.update_by_id(
                "662a3b4c1f94c72a88123456",
                {"$set": {"status": "verified"}}
            )
        """
        document_id = ObjectId(document_id) if isinstance(document_id, str) else document_id
        result = await cls._get_collection().update_one(
            {"_id": document_id},
            update,
            **kwargs
        )
        return result["modified_count"]

    @classmethod
    async def find_one_and_update(
            cls: type[T],
            filter: Document,
            update: Document | Sequence[Document],
            return_updated: bool = True,
            **kwargs: Unpack[FindOneAndUpdateOptions]
    ) -> T | None:
        """
        Atomically find and update a document.

        :param filter: Query to match document
        :param update: MongoDB update operations
        :param return_updated: Return updated document (default: True)
        :param kwargs: Additional arguments for find_one_and_update()
        :return: Updated document or None if not found

        .. code-block:: python

            # Atomic update with version check
            updated = await User.find_one_and_update(
                {"_id": user_id, "version": current_version},
                {"$set": {"data": new_data}, "$inc": {"version": 1}},
                return_updated=True
            )
        """
        options = {
            "return_document": "after" if return_updated else "before",
            **kwargs
        }

        result = await cls._get_collection().find_one_and_update(
            filter,
            update,
            **options
        )

        return cls.load(result) if result else None
