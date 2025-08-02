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
   - [Indexes](#indexes)  
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
from mongospec import IndexModel


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
| `projection_example.py`    | Field selection for performance              |

---

## Key Features

* **Zero-boilerplate models** – automatic collection resolution & binding.
* **Async first** – built on `mongojet`, fully `await`-able API.
* **Typed & fast** – data classes powered by `msgspec` for
  ultra-fast (de)serialization.
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