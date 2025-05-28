import asyncio
from datetime import datetime

import mongojet
import msgspec

import mongospec
from mongospec import MongoDocument


class User(MongoDocument):
    __collection_name__ = "users"

    name: str
    email: str
    version: int = 0
    created_at: datetime = msgspec.field(default_factory=datetime.now)

async def main():
    mongo_client = await mongojet.create_client("mongodb://localhost:27017")
    await mongospec.init(mongo_client.get_database("example_db"), document_types=[User])
    try:
        # Insert a user with initial version 0
        user = User(name="Alice", email="alice@example.com", version=0)
        await user.insert()

        user_id = user._id
        current_version = 5  # intentionally incorrect version for demonstration
        new_profile = {"newsletter": False}

        # Attempt to update the user with a mismatched version (expected to fail)
        updated_user = await User.find_one_and_update(
            {"_id": user_id, "version": current_version},  # Only update if version matches (which it won't)
            {
                "$set": {"profile": new_profile},
                "$inc": {"version": 1}
            },
            return_updated=True
        )
        if not updated_user:
            print("Update failed - document was modified by another process")
    finally:
        await mongospec.close()

asyncio.run(main())
