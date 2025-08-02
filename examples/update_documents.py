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
    login_count: int = 0
    version: int = 0
    created_at: datetime = msgspec.field(default_factory=datetime.now)


async def main():
    mongo_client = await mongojet.create_client("mongodb://localhost:27017")
    await mongospec.init(mongo_client.get_database("example_db"), document_types=[User])
    try:
        # Insert sample users for the update examples
        alice = User(name="Alice", email="alice@example.com", status="active")
        await alice.insert()
        bob = User(
            name="Bob", email="bob@example.com", status="inactive", login_count=0
        )
        await bob.insert()
        charlie = User(name="Charlie", email="charlie@example.com", status="pending")
        dave = User(name="Dave", email="dave@example.com", status="pending")
        await User.insert_many([charlie, dave])
        eve = User(name="Eve", email="eve@example.com", status="active", version=5)
        await eve.insert()

        # Find and modify approach (full document replacement)
        user = await User.find_one({"email": "alice@example.com"})
        if user:
            user.name = "Alice Jones"
            await user.save()  # Save changes (replaces the document)

        # Direct update with operators
        await User.update_one(
            {"email": "bob@example.com"},
            {"$set": {"status": "active"}, "$inc": {"login_count": 1}},
        )

        # Update multiple documents
        modified_count = await User.update_many(
            {"status": "pending"}, {"$set": {"status": "active"}}
        )
        print(f"Activated {modified_count} users")

        # Update by ID with operators
        await User.update_by_id(charlie._id, {"$set": {"verified": True}})

        # Atomic find-and-modify operation (optimistic concurrency control)
        user_id = eve._id
        current_version = 5
        new_profile = {"nickname": "Evie"}  # example profile data
        updated_user = await User.find_one_and_update(
            {
                "_id": user_id,
                "version": current_version,
            },  # Only update if version matches
            {
                "$set": {"profile": new_profile},
                "$inc": {"version": 1},  # increment version number
            },
            return_updated=True,
        )
        if not updated_user:
            print("Update failed - document was modified by another process")
    finally:
        await mongospec.close()


asyncio.run(main())
