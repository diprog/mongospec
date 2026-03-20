<p align="center">
  <img src="assets/logo.svg" width="35%" alt="mongospec"/>
</p>

[![PyPI](https://img.shields.io/pypi/v/mongospec?color=blue&label=PyPI%20package)](https://pypi.org/project/mongospec/)
[![Python](https://img.shields.io/badge/python-3.13%2B-blue)](https://www.python.org/downloads/)
[![License](https://img.shields.io/badge/license-MIT-green)](https://opensource.org/licenses/MIT)

Minimal **async** MongoDB ODM built for *speed* and *simplicity*, featuring automatic collection binding,
[msgspec](https://github.com/jcrist/msgspec) integration, and first-class asyncio support.

---

## Table of Contents

1. [Installation](#installation)  
2. [Quick Start](#quick-start)  
3. [Examples](#examples)  
4. [Key Features](#key-features)  
5. [Core Concepts](#core-concepts)  
   - [Document Models](#document-models)  
   - [Connection Management](#connection-management)  
   - [Collection Binding](#collection-binding)  
   - [CRUD Operations](#crud-operations)
   - [Document References](#document-references)
   - [Indexes](#indexes)
   - [Lifecycle Hooks](#lifecycle-hooks)
   - [Contrib: KV Store](#contrib-kv-store)
6. [Contributing](#contributing)
7. [License](#license)

---

## Installation

```bash
pip install mongospec
```

Requires **Python 3.13+** and a running MongoDB 6.0+ server.

---

## Quick Start

```python
import asyncio
from datetime import datetime
from typing import ClassVar, Sequence

import mongojet
import msgspec

import mongospec
from mongospec import MongoDocument
from mongojet import IndexModel


class User(MongoDocument):
    __collection_name__ = "users"
    __indexes__: ClassVar[Sequence[IndexModel]] = [
        IndexModel(keys=[("email", 1)], options={"unique": True})
    ]

    name: str
    email: str
    created_at: datetime = msgspec.field(default_factory=datetime.now)


async def main() -> None:
    client = await mongojet.create_client("mongodb://localhost:27017")
    await mongospec.init(client.get_database("example_db"), document_types=[User])

    user = User(name="Alice", email="alice@example.com")
    await user.insert()
    print("Inserted:", user)

    fetched = await User.find_one({"email": "alice@example.com"})
    print("Fetched:", fetched)

    await fetched.delete()
    await mongospec.close()


if __name__ == "__main__":
    asyncio.run(main())
```

---

## Examples

All other usage examples have been moved to standalone scripts in the
[`examples/`](./examples) directory.
Each file is self-contained and can be executed directly:

| Script                     | What it covers                               |
|----------------------------|----------------------------------------------|
| `quick_start.py`           | End-to-end “hello world”                     |
| `document_models.py`       | Defining typed models & indexes              |
| `connection_management.py` | Initialising the ODM and binding collections |
| `collection_binding.py`    | Using models immediately after init          |
| `index_creation.py`        | Unique, compound & text indexes              |
| `create_documents.py`      | Single & bulk inserts, conditional insert    |
| `read_documents.py`        | Queries, cursors, projections                |
| `update_documents.py`      | Field updates, atomic & versioned updates    |
| `delete_documents.py`      | Single & batch deletes                       |
| `count_documents.py`       | Fast counts & estimated counts               |
| `working_with_cursors.py`  | Batch processing large result sets           |
| `batch_operations.py`      | Bulk insert / update / delete                |
| `atomic_updates.py`        | Optimistic-locking with version field        |
| `upsert_operations.py`     | Upsert via `save` and `update_one`           |
| `document_references.py`   | Transparent typed document references         |
| `projection_example.py`    | Field selection for performance              |

---

## Key Features

* **Zero-boilerplate models** – automatic collection resolution & binding.
* **Async first** – built on `mongojet`, fully `await`-able API.
* **Typed & fast** – data classes powered by `msgspec` for
  ultra-fast (de)serialization.
* **Document references** – transparent typed references with
  automatic resolution, batch fetching, and cascading save.
* **Declarative indexes** – define indexes right on the model with
  familiar `pymongo`/`mongojet` `IndexModel`s.
* **Batteries included** – helpers for common CRUD patterns, bulk and
  atomic operations, cursors, projections, upserts and more.

---

## Core Concepts

### Document Models

Define your schema by subclassing **`MongoDocument`**
and adding typed attributes.
See **[`examples/document_models.py`](./examples/document_models.py)**.

### Connection Management

Initialise once with `mongospec.init(...)`, passing a
`mongojet.Database` and the list of models to bind.
See **[`examples/connection_management.py`](./examples/connection_management.py)**.

### Collection Binding

After initialisation every model knows its collection and can be used
immediately – no manual wiring required.
See **[`examples/collection_binding.py`](./examples/collection_binding.py)**.

### CRUD Operations

The `MongoDocument` class (and its mixins) exposes a rich async CRUD API:
`insert`, `find`, `update`, `delete`, `count`, cursors, bulk helpers,
atomic `find_one_and_update`, upserts, etc.
See scripts in `examples/` grouped by operation type.

### Document References

Any field typed as a `MongoDocument` subclass is automatically a reference —
stored as plain `ObjectId` in MongoDB, resolved transparently on read.

```python
from mongospec import MongoDocument

class User(MongoDocument):
    name: str

class Tag(MongoDocument):
    label: str

class Post(MongoDocument):
    title: str
    author: User                          # required reference
    reviewer: User | None = None          # optional
    tags: list[Tag] = []                  # list of references

# Create — pass real document objects
post = Post(title="Hello", author=user, tags=[tag])
await post.insert()

# Read — references auto-resolved (full type support for linters)
post = await Post.find_one({"title": "Hello"})
print(post.author.name)                   # "Alice" — full autocomplete

# Batch resolve (minimal queries: 1 per referenced class)
posts = await Post.find_all({})           # auto-resolved

# Skip resolution for performance
post = await Post.find_one({...}, resolve_refs=False)

# Cascading save (enabled by default)
post.author.name = "Updated"
await post.save()                          # saves post AND author
```

See **[`examples/document_references.py`](./examples/document_references.py)** for
a complete walkthrough.

---

### Indexes

Declare indexes in `__indexes__` as a `Sequence[IndexModel]`
(unique, compound, text, …).
Indexes are created automatically at init time.
See **[`examples/index_creation.py`](./examples/index_creation.py)**.

### Automatic Discovery of Document Models

In addition to manually listing document classes when calling `mongospec.init(...)`, you can use the utility function `collect_document_types(...)` to automatically discover all models in a package:

```python
from mongospec.utils import collect_document_types

document_types = collect_document_types("myapp.db.models")
await mongospec.init(db, document_types=document_types)

```

This function supports:

* Recursive import of all submodules in the target package
* Filtering by base class (default: `MongoDocument`)
* Optional exclusion of abstract or re-exported classes
* Regex or callable-based module filtering
* Graceful handling of import errors

**Usage Example:**

```python
from mongospec.utils import collect_document_types

# Collect all document models in `myapp.db.models` and its submodules
models = collect_document_types(
    "myapp.db.models",
    ignore_abstract=True,
    local_only=True,
    on_error="warn",
)

await mongospec.init(db, document_types=models)
```

**Advanced options include:**

* `predicate=...` to filter only specific model types
* `return_map=True` to get a `{qualified_name: class}` dict
* `module_filter=".*models.*"` to restrict traversal

See the full function signature in [`mongospec/utils.py`](./mongospec/utils.py).

---

### Lifecycle Hooks

`MongoDocument` provides two hooks that subclasses can override to inject
custom logic before write operations:

| Hook | Called by | Purpose |
|------|-----------|---------|
| `__pre_save__(self) -> None` | `insert()`, `insert_one()`, `insert_many()`, `save()` | Mutate instance fields before serialization |
| `__pre_update__(cls, update) -> dict` | `update_one()`, `update_many()`, `update_by_id()`, `find_one_and_update()` | Modify the update document before execution |

**Example — automatic `updated_at`:**

```python
from datetime import datetime, UTC
from typing import Any

import msgspec

from mongospec import MongoDocument


class Document(MongoDocument):
    created_at: datetime = msgspec.field(default_factory=lambda: datetime.now(UTC))
    updated_at: datetime = msgspec.field(default_factory=lambda: datetime.now(UTC))

    def __pre_save__(self) -> None:
        self.updated_at = datetime.now(UTC)

    @classmethod
    def __pre_update__(cls, update: dict[str, Any]) -> dict[str, Any]:
        update.setdefault("$set", {}).setdefault("updated_at", datetime.now(UTC))
        return update
```

Now every insert, save, or update operation automatically keeps `updated_at`
in sync — no caller-side boilerplate needed.

---

### Contrib: KV Store

`mongospec.contrib.kv_store` provides a ready-made async key-value store
backed by a MongoDB collection. Designed for multiple inheritance with
project-specific base documents.

```python
from mongospec.contrib.kv_store import KVStore, KVStoreItem
from myapp.db import Document  # your base with timestamps, hooks, etc.


class AppStorage(KVStore, Document):
    __collection_name__ = "app_storage"
```

A unique index on `key` is created automatically at init time.

**Direct usage:**

```python
await AppStorage.set("theme", "dark")
theme = await AppStorage.get("theme")            # "dark"
theme = await AppStorage.get_or_default("x", 0)  # 0 (no KeyError)
await AppStorage.set_default("theme", "light")   # "dark" (atomic, no overwrite)

await AppStorage.set_many({"a": 1, "b": 2})
all_pairs = await AppStorage.get_all()            # {"theme": "dark", "a": 1, "b": 2}
all_keys  = await AppStorage.keys()               # ["theme", "a", "b"]

await AppStorage.has("theme")                     # True
await AppStorage.delete_key("theme")              # True
```

**Typed accessor (`KVStoreItem`):**

```python
AppStorageItem = KVStoreItem.of(AppStorage)

max_retries = AppStorageItem[int]("max_retries", default=3)

value = await max_retries.get()          # 3 (default, not persisted)
await max_retries.set_default()          # atomically persist default if missing
await max_retries.set(10)
await max_retries.has()                  # True
await max_retries.delete()               # True
```

| `KVStore` method | Description |
|------------------|-------------|
| `set(key, value)` | Upsert a value by key |
| `get(key)` | Get value or raise `KeyError` |
| `get_or_default(key, default)` | Get value or return default |
| `set_default(key, value)` | Atomic insert-if-absent (`$setOnInsert`) |
| `delete_key(key)` | Delete a key, return `True` if existed |
| `has(key)` | Check key existence |
| `get_all()` | Return all pairs as `dict` |
| `keys()` | Return all key names |
| `set_many(items)` | Upsert multiple pairs |

---

## Contributing

Contributions, issues and feature requests are welcome.

---

## License

[MIT](https://opensource.org/licenses/MIT)