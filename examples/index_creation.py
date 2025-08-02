from mongospec import IndexModel

from mongospec import MongoDocument


class User(MongoDocument):
    __collection_name__ = "users"
    __indexes__ = [
        # Simple unique index
        IndexModel(keys=[("email", 1)], options={"unique": True}),
        # Compound index on two fields
        IndexModel(keys=[("last_name", 1), ("first_name", 1)], options={}),
        # Text index with weights
        IndexModel(keys=[("description", "text")], options={"weights": {"title": 10, "description": 5}})
    ]

    first_name: str
    last_name: str
    email: str
    description: str = ""
