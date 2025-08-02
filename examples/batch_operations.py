import asyncio
from datetime import datetime, timedelta
from typing import ClassVar, Optional, Sequence

import mongojet
import msgspec

import mongospec
from mongospec import IndexModel, MongoDocument


class User(MongoDocument):
    __collection_name__ = "users"
    __indexes__: ClassVar[Sequence[IndexModel]] = [
        IndexModel(keys=[("email", 1)], options={"unique": True})
    ]

    name: str
    email: str
    status: str = "active"
    trial_ends_at: Optional[datetime] = None
    last_login: datetime = msgspec.field(default_factory=datetime.now)
    created_at: datetime = msgspec.field(default_factory=datetime.now)


async def main():
    mongo_client = await mongojet.create_client("mongodb://localhost:27017")
    await mongospec.init(mongo_client.get_database("example_db"), document_types=[User])
    try:
        # Bulk insert 100 users
        one_year_ago = datetime.now() - timedelta(days=365)
        users = []
        for i in range(100, 200):
            status = "trial"
            trial_end = datetime.now() + timedelta(days=30)  # trial ends in 30 days
            last_login = datetime.now()
            # Make 10 users with last_login over a year ago for deletion demo
            if i < 110:
                last_login = datetime.now() - timedelta(days=366)
            users.append(
                User(
                    name=f"User{i}",
                    email=f"user{i}@example.com",
                    status=status,
                    trial_ends_at=trial_end,
                    last_login=last_login,
                )
            )
        ids = await User.insert_many(users)
        print(f"Inserted {len(ids)} users")

        # Bulk update – set all "trial" users to "active" and remove trial_ends_at
        modified = await User.update_many(
            {"status": "trial"},
            {"$set": {"status": "active"}, "$unset": {"trial_ends_at": ""}},
        )
        print(f"Updated {modified} trial users to active")

        # Bulk delete – remove users whose last_login is older than one year
        deleted = await User.delete_many({"last_login": {"$lt": one_year_ago}})
        print(f"Deleted {deleted} users with last login > 1 year ago")
    finally:
        await mongospec.close()


asyncio.run(main())
