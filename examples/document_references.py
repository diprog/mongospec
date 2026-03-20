"""
Document References
===================

Demonstrates transparent typed document references.
Any field typed as a MongoDocument subclass is automatically treated as a
reference — stored as plain ObjectId in MongoDB and resolved on read.
"""
import asyncio

import mongojet

import mongospec
from mongospec import MongoDocument


# ---------------------------------------------------------------------------
# Models
# ---------------------------------------------------------------------------

class User(MongoDocument):
    __collection_name__ = "users"
    name: str
    email: str


class Tag(MongoDocument):
    __collection_name__ = "tags"
    label: str


class Category(MongoDocument):
    __collection_name__ = "categories"
    title: str
    parent: "Category | None" = None  # self-referencing


class Post(MongoDocument):
    __collection_name__ = "posts"
    title: str
    author: User                    # required reference
    reviewer: User | None = None    # optional reference
    tags: list[Tag] = []            # list of references


async def main():
    client = await mongojet.create_client("mongodb://localhost:27017")
    db = client.get_database("example_refs")
    try:
        await mongospec.init(db, document_types=[User, Tag, Category, Post])

        # --- Create referenced documents ---
        alice = User(name="Alice", email="alice@example.com")
        bob = User(name="Bob", email="bob@example.com")
        await User.insert_many([alice, bob])

        python_tag = Tag(label="python")
        async_tag = Tag(label="async")
        mongo_tag = Tag(label="mongodb")
        await Tag.insert_many([python_tag, async_tag, mongo_tag])

        # --- Create a post with references (pass real objects) ---
        post = Post(
            title="Getting Started with mongospec",
            author=alice,
            reviewer=bob,
            tags=[python_tag, async_tag, mongo_tag],
        )
        await post.insert()
        print(f"Created post: {post.title}")

        # --- Fetch from DB — references auto-resolved ---
        found = await Post.find_one({"title": post.title})
        print(f"\nLoaded post from DB: {found.title}")
        print(f"  author: {found.author.name} ({found.author.email})")
        print(f"  reviewer: {found.reviewer.name}")
        print(f"  tags: {[t.label for t in found.tags]}")

        # --- Without resolution (performance) ---
        raw = await Post.find_one({"title": post.title}, resolve_refs=False)
        print(f"\nWithout resolution:")
        print(f"  author._id: {raw.author._id}")  # ObjectId available
        print(f"  author.name: '{raw.author.name}'")  # zero-value (empty string)

        # --- Batch resolve (minimal queries) ---
        for i in range(5):
            await Post(title=f"Extra-{i}", author=alice, tags=[python_tag]).insert()

        posts = await Post.find_all({})
        print(f"\nBatch loaded {len(posts)} posts — all refs auto-resolved:")
        for p in posts:
            print(f"  {p.title} by {p.author.name}, tags: {[t.label for t in p.tags]}")

        # --- Cascading save ---
        found = await Post.find_one({"title": post.title})
        found.author.name = "Alice Updated"
        await found.save()  # save_refs=True by default — saves post AND author
        print(f"\nAfter cascading save:")
        db_author = await User.find_by_id(alice._id)
        print(f"  Author in DB: {db_author.name}")  # "Alice Updated"

        # Disable cascade
        found.author.name = "Should Not Change"
        await found.save(save_refs=False)  # only saves the post
        db_author2 = await User.find_by_id(alice._id)
        print(f"  After save_refs=False: {db_author2.name}")  # still "Alice Updated"

        # --- Nested references (self-referencing) ---
        root = Category(title="Technology")
        await root.insert()
        databases = Category(title="Databases", parent=root)
        await databases.insert()
        nosql = Category(title="NoSQL", parent=databases)
        await nosql.insert()

        print(f"\nNested categories:")
        leaf = await Category.find_one({"title": "NoSQL"})
        print(f"  {leaf.title} → {leaf.parent.title} → {leaf.parent.parent.title}")

        # --- Query by reference field (just use ObjectId) ---
        alice_posts = await Post.find_all({"author": alice._id})
        print(f"\nPosts by Alice: {[p.title for p in alice_posts]}")

    finally:
        await db.drop()
        await mongospec.close()


asyncio.run(main())
