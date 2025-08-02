import asyncio
from datetime import datetime
from typing import Optional

import mongojet
import msgspec

import mongospec
from mongospec import IndexModel, MongoDocument


class User(MongoDocument):
    __collection_name__ = "users"
    __indexes__ = [IndexModel(keys=[("email", 1)], options={"unique": True})]

    name: str
    email: str
    created_at: datetime = msgspec.field(default_factory=datetime.now)

class Product(MongoDocument):
    __collection_name__ = "products"
    __indexes__ = [IndexModel(keys=[("sku", 1)], options={"unique": True})]

    name: str
    price: float
    sku: str
    description: Optional[str] = None
    in_stock: bool = True
    created_at: datetime = msgspec.field(default_factory=datetime.now)
    updated_at: Optional[datetime] = None

async def main():
    mongo_client = await mongojet.create_client("mongodb://localhost:27017")
    db = mongo_client.get_database("example_db")
    await mongospec.init(db, document_types=[User, Product])

    # After initialization, you can use all CRUD methods without extra setup
    user = User(name="John", email="john@example.com")
    await user.insert()  # Collection is already bound
    print(f"Inserted user with ID: {user._id}")

    await mongospec.close()

asyncio.run(main())
