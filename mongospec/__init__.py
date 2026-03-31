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
    RecursiveInsertError,
    RecursiveInsertResult,
    RecursiveRollbackError,
    RollbackFailure,
    UpdateOperationsMixin,
)

# Re-export commonly used dependency types
from mongojet import *

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
    "RecursiveInsertResult",
    "RecursiveInsertError",
    "RecursiveRollbackError",
    "RollbackFailure",
    "UpdateOperationsMixin",

    # Public re-exports
    "create_client",
    "Client",
    "Database",
    "Collection",
    "PyMongoError",
    "OperationFailure",
    "WriteError",
    "WriteConcernError",
    "DuplicateKeyError",
    "BsonSerializationError",
    "BsonDeserializationError",
    "ConnectionFailure",
    "ServerSelectionError",
    "ConfigurationError",
    "DatabaseOptions",
    "CollectionOptions",
    "ReadConcern",
    "WriteConcern",
    "ReadPreference",
    "IndexModel",
    "IndexModelDef",
    "GridfsBucket",
    "GridFSError",
    "NoFile",
    "FileExists",
]
