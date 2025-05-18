"""
Base MongoDB document model with automatic collection binding.

Provides collection name resolution and runtime collection access.
Uses class name as fallback when __collection_name__ is not specified.
"""
from datetime import datetime
from typing import Any, ClassVar, Self

import mongojet
import msgspec
from bson import ObjectId
from mongojet import IndexModel

from .operations import (
    CountOperationsMixin, DeleteOperationsMixin, FindOperationsMixin, InsertOperationsMixin, UpdateOperationsMixin
)


class MongoDocument(
    msgspec.Struct,
    CountOperationsMixin,
    DeleteOperationsMixin,
    FindOperationsMixin,
    InsertOperationsMixin,
    UpdateOperationsMixin,
    kw_only=True
):
    """
    Abstract base document for MongoDB collections with automatic binding.

    .. rubric:: Class Organization

    Settings:
        __collection_name__: ClassVar[Optional[str]] = None
            Explicit collection name (optional).
        __preserve_types__: ClassVar[Tuple[Type[Any], ...]] = (ObjectId, datetime)
            Types to preserve in their original form during encoding.
        __indexes__: ClassVar[List[Dict]] = []
            List of MongoDB indexes to ensure on initialization.

    Runtime:
        __collection__: ClassVar[Optional[mongojet.Collection]] = None
            Set during mongospec.init().

    Document:
        _id: Optional[ObjectId] = None
            MongoDB document ID field.
    """

    # Configuration settings
    __collection_name__: ClassVar[str | None] = None
    __preserve_types__: ClassVar[tuple[type[Any], ...]] = (ObjectId, datetime)
    __indexes__: ClassVar[list[IndexModel]] = []

    # Collection initialized externally
    __collection__: ClassVar[mongojet.Collection | None] = None

    # Primary key field
    _id: ObjectId | None = None

    @staticmethod
    def _dec_hook(type_: type[Any], obj: Any) -> Any:
        """Decode custom types like ObjectId and datetime during conversion."""
        if type_ is ObjectId:
            if isinstance(obj, ObjectId):
                return obj
            try:
                return ObjectId(obj)
            except Exception as e:
                raise ValueError(f"Invalid ObjectId: {obj}") from e
        raise NotImplementedError(f"Unsupported type: {type_}")

    @staticmethod
    def _enc_hook(obj: Any) -> Any:
        """Encode custom types during serialization. Override in subclasses."""
        raise NotImplementedError(f"Type {type(obj)} not supported")

    @classmethod
    def get_collection(cls) -> mongojet.Collection:
        """Retrieve the bound MongoDB collection."""
        if cls.__collection__ is None:
            raise RuntimeError(
                f"Collection for {cls.__name__} not initialized. "
                "Call mongospec.init() first."
            )
        return cls.__collection__

    @classmethod
    def get_collection_name(cls) -> str:
        """Determine the collection name from class settings."""
        return cls.__collection_name__ or cls.__name__

    @classmethod
    def load(cls, data: dict[str, Any]) -> Self:
        """Deserialize a dictionary into a document instance."""
        return msgspec.convert(
            data,
            cls,
            dec_hook=cls._dec_hook,
            from_attributes=True,
            strict=False
        )

    def dump(self, **kwargs: Any) -> dict[str, Any]:
        """Serialize the document into a MongoDB-compatible dictionary."""
        data = msgspec.to_builtins(
            self,
            enc_hook=self._enc_hook,
            builtin_types=self.__preserve_types__,
            **kwargs
        )
        # Strip None _id to allow MongoDB to generate it
        if data.get("_id") is None:
            del data["_id"]
        return data
