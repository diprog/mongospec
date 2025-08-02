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
        # Insert sample users to count
        alice = User(name="Alice", email="alice@example.com", status="active")
        bob = User(name="Bob", email="bob@example.com", status="active")
        charlie = User(name="Charlie", email="charlie@example.com", status="pending")
        dave = User(name="Dave", email="dave@example.com", status="pending")
        await User.insert_many([alice, bob, charlie, dave])

        # Count with filter
        active_count = await User.count_documents({"status": "active"})

        # Estimated count (faster but approximate)
        total_count = await User.estimated_document_count()

        # Alternative count method (alias for count_documents)
        pending_count = await User.count({"status": "pending"})

        print(f"Active users: {active_count}")
        print(f"Total users (estimated): {total_count}")
        print(f"Pending users: {pending_count}")
    finally:
        await mongospec.close()

asyncio.run(main())
