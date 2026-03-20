"""
Base class for MongoDB document operations.
Handles collection validation and provides utility methods.
"""
from typing import Any, TYPE_CHECKING, TypeVar

import mongojet

if TYPE_CHECKING:
    # noinspection PyUnresolvedReferences
    from mongospec.document.document import MongoDocument

T = TypeVar("T", bound="MongoDocument")


class BaseOperations:
    """Base class for MongoDB operations mixins"""

    @classmethod
    def _get_collection(cls: type[T]) -> mongojet.Collection:
        """Get collection with type checking"""
        if not hasattr(cls, "get_collection"):
            raise AttributeError("Document model must implement get_collection()")
        return cls.get_collection()

    @classmethod
    def _validate_document_type(cls: type[T], document: Any) -> None:
        """Ensure document matches collection type"""
        if not isinstance(document, cls):
            raise TypeError(f"Document must be of type {cls.__name__}")

    def _validate_refs(self: T) -> None:
        """Ensure all referenced MongoDocument fields have been saved (have _id)."""
        from mongospec.refs import is_document_type

        for field_name, ref_info in self._get_ref_fields().items():
            value = getattr(self, field_name, None)
            if value is None:
                continue
            if ref_info.is_list:
                for item in value:
                    if is_document_type(type(item)) and item._id is None:
                        raise ValueError(
                            f"Cannot reference unsaved {ref_info.document_class.__name__} "
                            f"in field '{field_name}' — insert it first"
                        )
            elif is_document_type(type(value)) and value._id is None:
                raise ValueError(
                    f"Cannot reference unsaved {ref_info.document_class.__name__} "
                    f"in field '{field_name}' — insert it first"
                )
