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
        # Prepare a sample document to query
        sample_user = User(name="Alice", email="alice@example.com")
        await sample_user.insert()
        user_id = str(sample_user._id)  # Can be string or ObjectId

        # Find by ID
        user = await User.find_by_id(user_id)

        # Find by query
        user = await User.find_one({"email": "alice@example.com"})

        # Find multiple documents
        cursor = await User.find({"age": {"$gt": 30}})
        async for user in cursor:
            print(f"Found user: {user.name}")

        # Get all documents in a collection (use with caution for large collections)
        all_users = await User.find_all()

        # Check if a document exists
        if await User.exists({"email": "alice@example.com"}):
            print("User exists")
    finally:
        await mongospec.close()

asyncio.run(main())
