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
    department: str
    created_at: datetime = msgspec.field(default_factory=datetime.now)

# Example processing function for demonstration
def process_user(user: User) -> None:
    print(f"Processing user {user.name} from {user.department}")

async def main():
    mongo_client = await mongojet.create_client("mongodb://localhost:27017")
    await mongospec.init(mongo_client.get_database("example_db"), document_types=[User])
    try:
        # Insert many users for cursor demonstration (e.g., 25 engineering users)
        users = [User(name=f"User{i}", email=f"user{i}@example.com", department="engineering") for i in range(25)]
        await User.insert_many(users)

        # Create a cursor with optional batch size
        cursor = await User.find(
            {"department": "engineering"},
            batch_size=100  # process in batches of 100
        )

        # Process documents one at a time (memory efficient)
        async for user in cursor:
            process_user(user)

        # Alternatively, convert a limited number of results to a list
        cursor_again = await User.find({"department": "engineering"})
        first_20_users = await cursor_again.to_list(20)
        print(f"First 20 users: {[user.email for user in first_20_users]}")
    finally:
        await mongospec.close()

asyncio.run(main())
