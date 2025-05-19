"""
Delete operations mixin for MongoDocument.

Provides document deletion capabilities including:
- Single document deletion
- Bulk document deletion
- Instance-based deletion
"""

from typing import Any, Unpack

from bson import ObjectId
from mongojet._types import DeleteOptions, Document

from .base import BaseOperations, T


class DeleteOperationsMixin(BaseOperations):
    """Mixin class providing all delete operations for MongoDocument"""

    async def delete(self: T, **kwargs: Unpack[DeleteOptions]) -> int:
        """
        Delete current document instance from collection.

        :param kwargs: Additional arguments for delete_one()
        :return: Number of deleted documents (0 or 1)
        :raises ValueError: If document lacks _id field

        .. code-block:: python

            # Delete existing document
            user = await User.find_one({"email": "alice@example.com"})
            await user.delete()
        """
        self._validate_document_type(self)

        if self._id is None:
            raise ValueError("Cannot delete document without _id")

        result = await self._get_collection().delete_one(
            {"_id": self._id},
            **kwargs
        )
        return result["deleted_count"]

    @classmethod
    async def delete_one(
            cls: type[T],
            filter: Document | str | None = None,
            **kwargs: Unpack[DeleteOptions]
    ) -> int:
        """
        Delete single document matching filter.

        :param filter: Query to match document
        :param kwargs: Additional arguments for delete_one()
        :return: Number of deleted documents (0 or 1)

        .. code-block:: python

            # Delete by query
            await User.delete_one({"email": "inactive@example.com"})
        """
        result = await cls._get_collection().delete_one(filter, **kwargs)
        return result["deleted_count"]

    @classmethod
    async def delete_many(
            cls: type[T],
            filter: Document,
            **kwargs: Unpack[DeleteOptions]
    ) -> int:
        """
        Delete multiple documents matching filter.

        :param filter: Query to match documents
        :param kwargs: Additional arguments for delete_many()
        :return: Number of deleted documents

        .. code-block:: python

            # Bulk delete
            await User.delete_many({"status": "banned"})
        """
        result = await cls._get_collection().delete_many(filter, **kwargs)
        return result["deleted_count"]

    @classmethod
    async def delete_by_id(
            cls: type[T],
            document_id: ObjectId | str,
            **kwargs: Unpack[DeleteOptions]
    ) -> int:
        """
        Delete document by ID.

        :param document_id: Document ID to delete (ObjectId or string)
        :param kwargs: Additional arguments for delete_one()
        :return: Number of deleted documents (0 or 1)

        .. code-block:: python

            # Delete by string ID
            await User.delete_by_id("662a3b4c1f94c72a88123456")
        """
        if isinstance(document_id, str):
            document_id = ObjectId(document_id)

        result = await cls._get_collection().delete_one(
            {"_id": document_id},
            **kwargs
        )
        return result["deleted_count"]
