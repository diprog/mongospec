"""
Base class for MongoDB document operations.
Handles collection validation and provides utility methods.
"""
from typing import Any, TypeVar

TDocument = TypeVar("TDocument", bound="MongoDocument")


class BaseOperations:
    """Base class for MongoDB operations mixins"""

    @classmethod
    def _get_collection(cls: type[TDocument]) -> Any:
        """Get collection with type checking"""
        if not hasattr(cls, "get_collection"):
            raise AttributeError("Document model must implement get_collection()")
        return cls.get_collection()

    @classmethod
    def _validate_document_type(cls: type[TDocument], document: Any) -> None:
        """Ensure document matches collection type"""
        if not isinstance(document, cls):
            raise TypeError(f"Document must be of type {cls.__name__}")
