from datetime import datetime
from typing import Optional

import msgspec

from mongospec import IndexModel, MongoDocument


class Product(MongoDocument):
    # Custom collection name (optional, defaults to class name)
    __collection_name__ = "products"

    # MongoDB indexes to create
    __indexes__ = [IndexModel(keys=[("sku", 1)], options={"unique": True})]

    # Document fields (all typed)
    name: str
    price: float
    sku: str
    description: Optional[str] = None
    in_stock: bool = True
    created_at: datetime = msgspec.field(default_factory=datetime.now)
    updated_at: Optional[datetime] = None

    # The _id field is already defined in MongoDocument
