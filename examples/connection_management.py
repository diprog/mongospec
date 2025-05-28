import asyncio
from datetime import datetime
from typing import Optional

import mongojet
import msgspec
from mongojet import IndexModel

import mongospec
from mongospec import MongoDocument


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

class Order(MongoDocument):
    __collection_name__ = "orders"
    # No specific indexes for Order in this example
    item: str
    quantity: int
    created_at: datetime = msgspec.field(default_factory=datetime.now)

async def main():
    # Create a mongojet client
    mongo_client = await mongojet.create_client("mongodb://localhost:27017")
    db = mongo_client.get_database("example_db")

    # Initialize mongospec with your database and document models
    await mongospec.init(db, document_types=[User, Product, Order])

    # (At this point, collections are bound and indexes created)
    # Always close connections when shutting down
    await mongospec.close()

asyncio.run(main())
