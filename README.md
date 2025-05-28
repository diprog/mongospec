<p align="center">
  <img src="assets/logo1024x1024.png" width="35%" alt="mongospec"/>
</p>

[![PyPI](https://img.shields.io/pypi/v/mongospec?color=blue&label=PyPI%20package)](https://pypi.org/project/mongospec/)
[![Python](https://img.shields.io/badge/python-3.13%2B-blue)](https://www.python.org/downloads/)
[![License](https://img.shields.io/badge/license-MIT-green)](https://opensource.org/licenses/MIT)

Minimal async MongoDB ODM built for **speed** and **simplicity**, featuring automatic collection binding and msgspec
integration.

## Table of Contents

- [Installation](#installation)
- [Key Features](#key-features)
- [Quick Start](#quick-start)
- [Core Concepts](#core-concepts)
    - [Document Models](#document-models)
    - [Connection Management](#connection-management)
    - [Collection Binding](#collection-binding)
    - [Index Creation](#index-creation)
- [CRUD Operations](#crud-operations)
    - [Creating Documents](#creating-documents)
    - [Reading Documents](#reading-documents)
    - [Updating Documents](#updating-documents)
    - [Deleting Documents](#deleting-documents)
    - [Counting Documents](#counting-documents)
- [Advanced Usage](#advanced-usage)
    - [Working with Cursors](#working-with-cursors)
    - [Batch Operations](#batch-operations)
    - [Atomic Updates](#atomic-updates)
    - [Upsert Operations](#upsert-operations)
- [Performance Considerations](#performance-considerations)
- [API Reference](#api-reference)
- [Development Status](#development-status)

## Installation

You can install mongospec from PyPI:

```bash
pip install mongospec
```

Dependencies:

- Python 3.13+
- mongojet ~= 0.3.1
- msgspec ~= 0.19.0

## Key Features

- ⚡ **Blazing fast** - Uses [msgspec](https://github.com/jcrist/msgspec) (fastest Python serialization)
  and [mongojet](https://github.com/romis2012/mongojet) (fastest async MongoDB wrapper)
- 🧩 **Dead simple** - No complex abstractions, just clean document handling
- 🏎️ **Zero overhead** - Optimized for performance-critical applications
- 🔄 **Async first** - Built from the ground up for asyncio
- 🧬 **Type-safe** - Full typing support for better IDE integration and code safety

## Quick Start

Here's a minimal example to get you started:

```python
import asyncio
from datetime import datetime

import mongojet
import msgspec

import mongospec
from mongospec import MongoDocument


class User(MongoDocument):
    __collection_name__ = "users"
    __indexes__ = [{"keys": [("email", 1)], "options": {"unique": True}}]

    name: str
    email: str
    created_at: datetime = msgspec.field(default_factory=datetime.now)


async def main():
    # Connect to MongoDB
    mongo_client = await mongojet.create_client("mongodb://localhost:27017")
    try:
        # Initialize mongospec with our models
        await mongospec.init(mongo_client.get_database("db"), document_types=[User])

        # Create a new user
        user = User(name="Alice", email="alice@example.com")
        await user.insert()
        print(f"Created user with ID: {user._id}")

        # Find the user
        found_user = await User.find_one({"email": "alice@example.com"})
        print(f"Found user: {found_user.name} ({found_user.email})")

        # Update the user
        found_user.name = "Alice Smith"
        await found_user.save()
        print(f"Updated user name to: {found_user.name}")

        # Delete the user
        await found_user.delete()
        print("User deleted")
    finally:
        # Always close the connection when done
        await mongospec.close()


asyncio.run(main())
```

## Core Concepts

### Document Models

Document models in mongospec are based on `msgspec.Struct` and define the schema for your MongoDB collections. Each
model automatically maps to a collection and provides methods for CRUD operations.

```python
from datetime import datetime
from typing import Optional, ClassVar, List, Dict, Any

import msgspec
from bson import ObjectId
from mongojet import IndexModel

from mongospec import MongoDocument


class Product(MongoDocument):
    # Custom collection name (optional, defaults to class name)
    __collection_name__ = "products"

    # MongoDB indexes to create
    __indexes__: ClassVar[List[Dict[str, Any]]] = [
        {"keys": [("sku", 1)], "options": {"unique": True}}
    ]

    # Document fields (all typed)
    name: str
    price: float
    description: Optional[str] = None
    sku: str
    in_stock: bool = True
    created_at: datetime = msgspec.field(default_factory=datetime.now)
    updated_at: Optional[datetime] = None

    # The _id field is already defined in MongoDocument
```

### Connection Management

mongospec handles MongoDB connections through a simple interface:

```python
import mongojet
import mongospec

# Create a mongojet client
mongo_client = await mongojet.create_client("mongodb://localhost:27017")
db = mongo_client.get_database("your_database")

# Initialize mongospec with your database
await mongospec.init(db, document_types=[User, Product, Order])

# Always close connections when shutting down
await mongospec.close()
```

### Collection Binding

When you initialize mongospec with document types, it:

1. Binds each document type to its corresponding collection
2. Creates any defined indexes
3. Makes all CRUD operations immediately available

```python
# Initialize mongospec with your models
await mongospec.init(db, document_types=[User, Product])

# After initialization, you can use all CRUD methods
user = User(name="John", email="john@example.com")
await user.insert()  # Collection is already bound
```

### Index Creation

Define indexes on your model using the `__indexes__` class variable:

```python
class User(MongoDocument):
    __collection_name__ = "users"
    __indexes__ = [
        # Simple unique index
        {"keys": [("email", 1)], "options": {"unique": True}},

        # Compound index
        {"keys": [("last_name", 1), ("first_name", 1)], "options": {}},

        # Text index
        {"keys": [("description", "text")], "options": {"weights": {"title": 10, "description": 5}}}
    ]

    first_name: str
    last_name: str
    email: str
    description: str = ""
```

The indexes are automatically created when you call `mongospec.init()` with your document types.

## CRUD Operations

### Creating Documents

mongospec provides several ways to create documents:

```python
# Create a single document
user = User(name="Alice", email="alice@example.com")
await user.insert()

# Alternative approach using class method
user = User(name="Bob", email="bob@example.com")
await User.insert_one(user)

# Batch insert multiple documents
users = [
    User(name="Charlie", email="charlie@example.com"),
    User(name="Dave", email="dave@example.com")
]
inserted_ids = await User.insert_many(users)

# Conditional insert (only if not exists)
user = User(name="Eve", email="eve@example.com")
result = await User.insert_if_not_exists(
    user,
    filter={"email": "eve@example.com"}
)
if result:
    print("User was inserted")
else:
    print("User with this email already exists")
```

### Reading Documents

Find documents with various query options:

```python
# Find by ID
user_id = "662a3b4c1f94c72a88123456"  # Can be string or ObjectId
user = await User.find_by_id(user_id)

# Find by query
user = await User.find_one({"email": "alice@example.com"})

# Find multiple documents
cursor = await User.find({"age": {"$gt": 30}})
async for user in cursor:
    print(f"Found user: {user.name}")

# Get all documents in a collection (use with caution for large collections)
all_users = await User.find_all()

# Check if document exists
if await User.exists({"email": "alice@example.com"}):
    print("User exists")
```

### Updating Documents

Update documents with various strategies:

```python
# Find and modify approach
user = await User.find_one({"email": "alice@example.com"})
if user:
    user.name = "Alice Jones"
    await user.save()  # Full document replacement

# Direct update with operators
await User.update_one(
    {"email": "bob@example.com"},
    {"$set": {"status": "active"}, "$inc": {"login_count": 1}}
)

# Update multiple documents
modified_count = await User.update_many(
    {"status": "pending"},
    {"$set": {"status": "active"}}
)
print(f"Activated {modified_count} users")

# Update by ID with operators
await User.update_by_id(
    "662a3b4c1f94c72a88123456",
    {"$set": {"verified": True}}
)

# Atomic find-and-modify operation
updated_user = await User.find_one_and_update(
    {"_id": user_id, "version": current_version},
    {"$set": {"data": new_data}, "$inc": {"version": 1}},
    return_updated=True
)
```

### Deleting Documents

Delete documents with various methods:

```python
# Delete a document instance
user = await User.find_one({"email": "alice@example.com"})
if user:
    await user.delete()

# Delete by query
deleted_count = await User.delete_one({"email": "bob@example.com"})

# Delete by ID
deleted_count = await User.delete_by_id("662a3b4c1f94c72a88123456")

# Bulk delete
deleted_count = await User.delete_many({"status": "inactive"})
print(f"Deleted {deleted_count} inactive users")
```

### Counting Documents

Count documents with different methods:

```python
# Count with filter
active_count = await User.count_documents({"status": "active"})

# Estimated count (faster but approximate)
total_count = await User.estimated_document_count()

# Alternative count method
pending_count = await User.count({"status": "pending"})
```

## Advanced Usage

### Working with Cursors

For large result sets, use cursors for memory-efficient processing:

```python
# Create a cursor with optional batch size
cursor = await User.find(
    {"department": "engineering"},
    batch_size=100  # Process in batches of 100
)

# Process documents one at a time (memory efficient)
async for user in cursor:
    process_user(user)

# Or convert a limited number to a list
first_20_users = await cursor.to_list(20)
```

### Batch Operations

Perform operations on multiple documents:

```python
# Bulk insert
users = [User(name=f"User_{i}", email=f"user{i}@example.com") for i in range(100)]
ids = await User.insert_many(users)

# Bulk update
modified = await User.update_many(
    {"status": "trial"},
    {"$set": {"status": "active"}, "$unset": {"trial_ends_at": ""}}
)

# Bulk delete
deleted = await User.delete_many({"last_login": {"$lt": one_year_ago}})
```

### Atomic Updates

Use atomic operations for concurrency control:

```python
# Implement optimistic concurrency control
updated = await User.find_one_and_update(
    {"_id": user_id, "version": 5},  # Only update if version matches
    {
        "$set": {"profile": new_profile},
        "$inc": {"version": 1}  # Increment version number
    },
    return_updated=True  # Get the updated document
)

if not updated:
    print("Update failed - document was modified by another process")
```

### Upsert Operations

Use upsert to insert or update as needed:

```python
# Save with upsert
user = User(name="Alice", email="alice@example.com")
await user.save(upsert=True)  # Insert if not exists, update if exists

# Update with upsert
await User.update_one(
    {"email": "bob@example.com"},
    {"$set": {"name": "Bob Smith", "status": "active"}},
    upsert=True  # Create if not exists
)
```

## Performance Considerations

mongospec is designed for high performance:

- **Batch Operations**: Use `insert_many`, `update_many`, and `delete_many` for better performance with multiple
  documents
- **Projected Queries**: Limit returned fields when possible using projection
- **Cursor Batching**: Set appropriate `batch_size` for large result sets
- **Indexes**: Ensure proper indexes are defined for your query patterns
- **Connection Reuse**: Initialize mongospec once and reuse for multiple operations

Example of projected query:

```python
# Only retrieve needed fields
users = await User.find(
    {"department": "sales"},
    projection={"name": 1, "email": 1, "_id": 1}  # Only these fields
).to_list(50)
```

## API Reference

### Module Functions

- `mongospec.init(db, document_types=None)` - Initialize connection and bind document types
- `mongospec.close()` - Close database connection
- `mongospec.get_collection(name)` - Get a collection by name
- `mongospec.is_connected()` - Check connection status

### MongoDocument

Base class for all document models with the following class variables:

- `__collection_name__` - Custom collection name (optional)
- `__preserve_types__` - Types to preserve during serialization
- `__indexes__` - MongoDB indexes to create
- `__collection__` - Runtime collection reference (set by mongospec.init)

Instance methods:

- `insert()` - Insert document
- `save(upsert=False)` - Save document changes
- `delete()` - Delete document
- `dump()` - Serialize to dict

Class methods:

- **Find Operations**
    - `find_one(filter)` - Find single document
    - `find_by_id(document_id)` - Find by ID
    - `find(filter, batch_size=None)` - Create cursor for query
    - `find_all()` - Get all documents
    - `exists(filter)` - Check if documents exist

- **Insert Operations**
    - `insert_one(document)` - Insert single document
    - `insert_many(documents, ordered=True)` - Insert multiple documents
    - `insert_if_not_exists(document, filter=None)` - Conditional insert

- **Update Operations**
    - `update_one(filter, update)` - Update single document
    - `update_many(filter, update)` - Update multiple documents
    - `update_by_id(document_id, update)` - Update by ID
    - `find_one_and_update(filter, update, return_updated=True)` - Atomic update

- **Delete Operations**
    - `delete_one(filter)` - Delete single document
    - `delete_many(filter)` - Delete multiple documents
    - `delete_by_id(document_id)` - Delete by ID

- **Count Operations**
    - `count_documents(filter=None)` - Count documents
    - `estimated_document_count()` - Fast approximate count
    - `count(filter=None)` - Count documents

## Development Status

mongospec is currently in **beta** stage. The core API is stable and all basic CRUD operations are fully supported.
We're working on additional features and optimizations for future releases.

Designed for:

- Rapid prototyping
- Performance-critical applications
- Modern asyncio-based projects