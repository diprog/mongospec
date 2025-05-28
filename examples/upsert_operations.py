import asyncio
from datetime import datetime

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
    status: str = "inactive"
    created_at: datetime = msgspec.field(default_factory=datetime.now)

async def main():
    mongo_client = await mongojet.create_client("mongodb://localhost:27017")
    await mongospec.init(mongo_client.get_database("example_db"), document_types=[User])
    try:
        # Save with upsert (insert if not exists, otherwise update)
        user = User(name="Alice", email="alice@example.com", status="active")
        await user.save(upsert=True)  # this will insert Alice since she doesn't exist yet

        # Update with upsert (will insert a new document if filter finds nothing)
        await User.update_one(
            {"email": "bob@example.com"},
            {"$set": {"name": "Bob Smith", "status": "active"}},
            upsert=True
        )
    finally:
        await mongospec.close()

asyncio.run(main())
