import asyncio
from datetime import datetime

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

async def main():
    mongo_client = await mongojet.create_client("mongodb://localhost:27017")
    await mongospec.init(mongo_client.get_database("example_db"), document_types=[User])
    try:
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
    finally:
        await mongospec.close()

asyncio.run(main())
