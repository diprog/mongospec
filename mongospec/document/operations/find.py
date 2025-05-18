"""
CRUD operations mixin for MongoDocument.
"""

from .base import BaseOperations, TDocument


class FindOperationsMixin(BaseOperations):
    """Mixin class for query operations"""

    @classmethod
    async def find_one(cls: type[TDocument], filter: dict | None = None) -> TDocument | None:
        """
        Find single document by query filter

        :param filter: MongoDB query filter
        :return: Document instance or None
        """
        doc = await cls._get_collection().find_one(filter or {})
        return cls.load(doc) if doc else None
