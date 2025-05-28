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
    created_at: datetime = msgspec.field(default_factory=datetime.now)

async def main():
    # Connect to MongoDB
    mongo_client = await mongojet.create_client("mongodb://localhost:27017")
    try:
        # Initialize mongospec with our models
        await mongospec.init(mongo_client.get_database("example_db"), document_types=[User])

        # Create a new user
        user = User(name="Alice", email="alice@example.com")
        await user.insert()
        print(f"Created user with ID: {user._id}")

        # Find the user
        found_user = await User.find_one({"email": "alice@example.com"})
        print(f"Found user: {found_user.name} ({found_user.email})")

        # Update the user
        found_user.name = "Alice Smith"
        await found_user.save()
        print(f"Updated user name to: {found_user.name}")

        # Delete the user
        await found_user.delete()
        print("User deleted")
    finally:
        # Always close the connection when done
        await mongospec.close()

asyncio.run(main())
