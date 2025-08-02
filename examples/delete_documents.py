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
    status: str = "active"
    created_at: datetime = msgspec.field(default_factory=datetime.now)


async def main():
    mongo_client = await mongojet.create_client("mongodb://localhost:27017")
    await mongospec.init(mongo_client.get_database("example_db"), document_types=[User])
    try:
        # Insert sample users to demonstrate deletions
        alice = User(name="Alice", email="alice@example.com", status="active")
        bob = User(name="Bob", email="bob@example.com", status="active")
        charlie = User(name="Charlie", email="charlie@example.com", status="inactive")
        dave = User(name="Dave", email="dave@example.com", status="inactive")
        eve = User(name="Eve", email="eve@example.com", status="inactive")
        await User.insert_many([alice, bob, charlie, dave, eve])

        # Delete a document instance
        user = await User.find_one({"email": "alice@example.com"})
        if user:
            await user.delete()

        # Delete by query (single document)
        deleted_count = await User.delete_one({"email": "bob@example.com"})

        # Delete by ID
        deleted_count = await User.delete_by_id(charlie._id)

        # Bulk delete by filter
        deleted_count = await User.delete_many({"status": "inactive"})
        print(f"Deleted {deleted_count} inactive users")
    finally:
        await mongospec.close()


asyncio.run(main())
