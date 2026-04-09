"""
Update operations mixin for MongoDocument.

Provides document modification capabilities including:
- Full document replacement
- Partial updates using MongoDB update operators
- Atomic field updates with concurrency control
"""

from collections.abc import Sequence
from typing import Unpack

from bson import ObjectId
from mongojet._types import Document, FindOneAndUpdateOptions, ReplaceOptions, UpdateOptions

from .base import BaseOperations, T


class UpdateOperationsMixin(BaseOperations):
    """Mixin class providing all update operations for MongoDocument"""

    async def save(
        self: T, upsert: bool = False, save_refs: bool = True, **kwargs: Unpack[ReplaceOptions]
    ) -> T:
        """
        Persist document changes to the database.

        :param upsert: Insert document if it doesn't exist (default: False)
        :param save_refs: Also save all resolved reference documents (default: True)
        :param kwargs: Additional arguments for replace_one()
        :return: Current document instance
        :raises ValueError: If _id missing and upsert=False
        :raises RuntimeError: If document not found and upsert=False

        .. code-block:: python

            # Modify referenced document and save everything
            post.author.name = "Alice Smith"
            await post.save()  # saves post AND author

            # Save only this document
            await post.save(save_refs=False)
        """
        self._validate_document_type(self)

        if self._id is None:
            if upsert:
                return await self.insert(**kwargs)
            raise ValueError("Document requires _id for save without upsert")

        if save_refs:
            await self._save_resolved_refs()

        self.__pre_save__()
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

    async def _save_resolved_refs(self: T) -> None:
        """Save all resolved MongoDocument reference fields."""
        from mongospec.refs import iter_ref_documents

        for ref_document in iter_ref_documents(self._get_ref_fields(), self):
            if ref_document._id is not None:
                await ref_document.save(save_refs=True)

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
        update = cls.__pre_update__(update)
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
        update = cls.__pre_update__(update)
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
        update = cls.__pre_update__(update)
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
            *,
            resolve_refs: bool = True,
            **kwargs: Unpack[FindOneAndUpdateOptions]
    ) -> T | None:
        """
        Atomically find and update a document.

        :param filter: Query to match document
        :param update: MongoDB update operations
        :param return_updated: Return updated document (default: True)
        :param resolve_refs: Resolve MongoDocument reference fields (default: True)
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
        update = cls.__pre_update__(update)
        options = {
            "return_document": "after" if return_updated else "before",
            **kwargs
        }

        result = await cls._get_collection().find_one_and_update(
            filter,
            update,
            **options
        )

        if result is None:
            return None
        if resolve_refs:
            ref_fields = cls._get_ref_fields()
            if ref_fields:
                from mongospec.refs import resolve_ref_data
                result = await resolve_ref_data(ref_fields, result)
        from .find import _load_single
        return _load_single(cls, result, resolve_refs)
