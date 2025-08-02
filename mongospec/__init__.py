# Core functionality
from .core import (close, init)

# Document models
from .document import MongoDocument
from .document.operations import (
    AsyncDocumentCursor,
    CountOperationsMixin,
    DeleteOperationsMixin,
    FindOperationsMixin,
    InsertOperationsMixin,
    UpdateOperationsMixin,
)

# Re-export commonly used dependency types
from mongojet import IndexModel

__all__ = [
    # Core functionality
    "init",
    "close",

    # Document models
    "MongoDocument",
    "AsyncDocumentCursor",
    "CountOperationsMixin",
    "DeleteOperationsMixin",
    "FindOperationsMixin",
    "InsertOperationsMixin",
    "UpdateOperationsMixin",

    # Public re-exports
    "IndexModel",
]
