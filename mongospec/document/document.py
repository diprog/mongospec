"""
Base MongoDB document model with automatic collection binding.

Provides collection name resolution and runtime collection access.
Uses class name as fallback when __collection_name__ is not specified.
"""
from datetime import datetime
from typing import Any, ClassVar, Self, final

import mongojet
import msgspec
from bson import ObjectId
from mongojet import IndexModel

from .operations import (
    CountOperationsMixin, DeleteOperationsMixin, FindOperationsMixin,
    InsertOperationsMixin, UpdateOperationsMixin
)


def default_dec_hook(expected_type: type, obj: Any) -> Any:
    """Default decoding hook for basic type conversion.

    :param expected_type: The type to decode into.
    :param obj: The raw value to convert.
    :return: The converted value.
    :raises ValueError: If the object cannot be converted to an ObjectId.
    """
    if expected_type is ObjectId:
        if isinstance(obj, ObjectId):
            return obj
        try:
            return ObjectId(obj)
        except Exception as e:
            raise ValueError(f"Invalid ObjectId: {obj}") from e
    raise NotImplementedError(f"Unsupported type: {expected_type}")


def default_enc_hook(obj: Any) -> Any:
    """Default encoding hook that raises NotImplementedError.

    :param obj: The object to encode.
    :raises NotImplementedError: Always raised since this is a placeholder.
    """
    raise NotImplementedError(f"Type {type(obj)} not supported")


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

    @classmethod
    def dec_hook(cls, expected_type: type[Any], obj: Any) -> Any:
        """
        Hook for custom deserialization logic. Override in subclasses.

        :param expected_type: The type we're trying to deserialize into.
        :param obj: The raw value to convert.
        :return: The converted value.
        :raises NotImplementedError: By default to indicate no custom handling.

        Example usage:

        .. code-block:: python

            @classmethod
            def dec_hook(cls, expected_type: type, obj: Any) -> Any:
                if expected_type is MyCustomType:
                    return MyCustomType.from_string(obj)
                return super().dec_hook(expected_type, obj)
        """
        raise NotImplementedError(f"Type {expected_type} not supported in dec_hook")

    def enc_hook(self, obj: Any) -> Any:
        """
        Hook for custom serialization logic. Override in subclasses.

        :param obj: The value to serialize.
        :return: A serializable representation of the value.
        :raises NotImplementedError: By default to indicate no custom handling.

        Example usage:

        .. code-block:: python

            def enc_hook(self, obj: Any) -> Any:
                if isinstance(obj, MyCustomType):
                    return str(obj)
                return super().enc_hook(obj)
        """
        raise NotImplementedError(f"Type {type(obj)} not supported in enc_hook")

    @classmethod
    @final
    def _dec_hook(cls, expected_type: type[Any], obj: Any) -> Any:
        """
        Internal decoding hook combining default and custom logic.

        This method is marked final — implement `dec_hook` instead.
        """
        try:
            return cls.dec_hook(expected_type, obj)
        except NotImplementedError:
            return default_dec_hook(expected_type, obj)

    @final
    def _enc_hook(self, obj: Any) -> Any:
        """
        Internal encoding hook combining default and custom logic.

        This method is marked final — implement `enc_hook` instead.
        """
        try:
            return self.enc_hook(obj)
        except NotImplementedError:
            return default_enc_hook(obj)

    @classmethod
    def get_collection(cls) -> mongojet.Collection:
        """
        Retrieve the bound MongoDB collection.

        :raises RuntimeError: If the collection has not been initialized.
        """
        if cls.__collection__ is None:
            raise RuntimeError(
                f"Collection for {cls.__name__} not initialized. "
                "Call mongospec.init() first."
            )
        return cls.__collection__

    @classmethod
    def get_collection_name(cls) -> str:
        """
        Determine the collection name from class settings.

        :return: The collection name, either explicitly defined or derived from class name.
        """
        return cls.__collection_name__ or cls.__name__

    @classmethod
    def load(cls, data: dict[str, Any]) -> Self:
        """
        Deserialize a dictionary into a document instance.

        :param data: Raw dictionary data from MongoDB.
        :return: A new document instance.
        """
        return msgspec.convert(
            data,
            cls,
            dec_hook=cls._dec_hook,
            from_attributes=True,
            strict=False
        )

    def dump(self, **kwargs: Any) -> dict[str, Any]:
        """
        Serialize the document into a MongoDB-compatible dictionary.

        :param kwargs: Additional arguments for `msgspec.to_builtins`.
        :return: A dictionary ready for MongoDB storage.

        Notes:
            - Uses `msgspec` serialization with custom `enc_hook` handling.
            - Automatically removes `_id` if it is `None`, allowing MongoDB to generate one.
        """
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
