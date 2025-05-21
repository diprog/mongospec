"""
Count operations mixin for MongoDocument.
"""

from typing import Any

from mongojet._types import Document

from .base import BaseOperations, T


class CountOperationsMixin(BaseOperations):
    """Mixin class for count operations"""

    @classmethod
    async def count_documents(
            cls: type[T],
            filter: Document | None = None,
            **kwargs: Any
    ) -> int:
        """
        Count documents matching the filter in the collection.

        :param filter: MongoDB query filter (empty filter counts all documents)
        :param kwargs: Additional arguments passed to count_documents()
        :return: Number of matching documents
        """
        return await cls._get_collection().count_documents(filter or {}, **kwargs)

    @classmethod
    async def estimated_document_count(cls: type[T], **kwargs: Any) -> int:
        """
        Get estimated document count using collection metadata.

        :param kwargs: Additional arguments passed to estimated_document_count()
        :return: Approximate document count
        """
        return await cls._get_collection().estimated_document_count(**kwargs)
