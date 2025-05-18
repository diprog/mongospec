# mongospec

Minimal async MongoDB ODM built for **speed** and **simplicity**, featuring automatic collection binding and msgspec
integration.

## Why mongospec?

- ⚡ **Blazing fast** - Uses [msgspec](https://github.com/jcrist/msgspec) (fastest Python serialization)
  and [mongojet](https://github.com/romis2012/mongojet) (fastest async MongoDB wrapper)
- 🧩 **Dead simple** - No complex abstractions, just clean document handling
- 🏎️ **Zero overhead** - Optimized for performance-critical applications

## Installation

```bash
pip install mongospec
```

## Quick Start

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
    mongo_client = await mongojet.create_client("mongodb://localhost:27017")
    try:
        await mongospec.init(mongo_client.get_database("db"), document_types=[User])

        # Create and insert user
        user = User(name="Alice", email="alice@example.com")
        await user.insert()

        # Find document
        found = await User.find_one({"email": "alice@example.com"})
        print(found.dump())
    finally:
        await mongospec.close()


asyncio.run(main())
```

## Performance Features

- 🚀 **msgspec-powered** serialization (2-10x faster than alternatives)
- ⚡ **mongojet backend** (faster than Motor with identical API)
- 🧬 **Compiled structs** for document models
- 📉 **Minimal abstraction** overhead

⚠️ **Early Alpha Notice**  
Basic CRUD operations supported. Designed for rapid prototyping. Not production-ready. API will evolve.