"""Tests for transparent document references (MongoDocument as field type)."""
import pytest
import pytest_asyncio
from bson import ObjectId

import mongospec
from mongospec import MongoDocument


# ---------------------------------------------------------------------------
# Test models
# ---------------------------------------------------------------------------

class User(MongoDocument):
    __collection_name__ = "test_users"
    name: str

class Tag(MongoDocument):
    __collection_name__ = "test_tags"
    label: str

class Category(MongoDocument):
    __collection_name__ = "test_categories"
    title: str
    parent: "Category | None" = None  # self-referencing

class Post(MongoDocument):
    __collection_name__ = "test_posts"
    title: str
    author: User
    reviewer: User | None = None
    tags: list[Tag] = []


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------

@pytest_asyncio.fixture
async def init_models(db):
    await mongospec.init(db, document_types=[User, Tag, Category, Post])


@pytest_asyncio.fixture
async def saved_user(init_models):
    user = User(name="Alice")
    await user.insert()
    return user


@pytest_asyncio.fixture
async def saved_tags(init_models):
    tags = [Tag(label=f"tag-{i}") for i in range(3)]
    await Tag.insert_many(tags)
    return tags


# ===========================================================================
# Serialization (dump)
# ===========================================================================

class TestDump:

    async def test_dump_ref_field(self, saved_user):
        post = Post(title="Hello", author=saved_user)
        data = post.dump()
        assert isinstance(data["author"], ObjectId)
        assert data["author"] == saved_user._id

    async def test_dump_optional_ref_none(self, saved_user):
        post = Post(title="Hello", author=saved_user)
        data = post.dump()
        assert data.get("reviewer") is None

    async def test_dump_optional_ref_set(self, saved_user):
        reviewer = User(name="Bob")
        await reviewer.insert()
        post = Post(title="Hello", author=saved_user, reviewer=reviewer)
        data = post.dump()
        assert isinstance(data["reviewer"], ObjectId)
        assert data["reviewer"] == reviewer._id

    async def test_dump_list_refs(self, saved_user, saved_tags):
        post = Post(title="Hello", author=saved_user, tags=saved_tags)
        data = post.dump()
        assert isinstance(data["tags"], list)
        assert all(isinstance(t, ObjectId) for t in data["tags"])
        assert len(data["tags"]) == 3

    async def test_dump_unsaved_ref_raises(self, init_models):
        unsaved = User(name="Ghost")
        post = Post(title="Fail", author=unsaved)
        with pytest.raises(ValueError, match="unsaved"):
            post.dump()


# ===========================================================================
# Deserialization (load / find with resolve_refs)
# ===========================================================================

class TestResolveRefs:

    async def test_find_one_resolves(self, saved_user, saved_tags):
        post = Post(title="Resolved", author=saved_user, tags=saved_tags)
        await post.insert()

        found = await Post.find_one({"title": "Resolved"})
        assert found is not None
        assert isinstance(found.author, User)
        assert found.author.name == "Alice"
        assert found.author._id == saved_user._id

    async def test_find_one_resolve_refs_false(self, saved_user):
        post = Post(title="Unresolved", author=saved_user)
        await post.insert()

        found = await Post.find_one({"title": "Unresolved"}, resolve_refs=False)
        assert found is not None
        assert found.author._id == saved_user._id
        # Without resolution, required fields get zero-values (not real data)
        assert found.author.name == ""

    async def test_find_by_id_resolves(self, saved_user):
        post = Post(title="ById", author=saved_user)
        await post.insert()

        found = await Post.find_by_id(post._id)
        assert found is not None
        assert found.author.name == "Alice"

    async def test_find_by_id_resolve_refs_false(self, saved_user):
        post = Post(title="ByIdNoRef", author=saved_user)
        await post.insert()

        found = await Post.find_by_id(post._id, resolve_refs=False)
        assert found.author._id == saved_user._id
        assert found.author.name == ""  # zero-value, not real data

    async def test_find_all_batch_resolves(self, saved_user, saved_tags):
        for i in range(5):
            post = Post(title=f"Batch-{i}", author=saved_user, tags=saved_tags)
            await post.insert()

        posts = await Post.find_all({})
        assert len(posts) == 5
        for p in posts:
            assert isinstance(p.author, User)
            assert p.author.name == "Alice"
            assert len(p.tags) == 3
            assert all(isinstance(t, Tag) for t in p.tags)

    async def test_find_all_resolve_refs_false(self, saved_user):
        post = Post(title="NoResolve", author=saved_user)
        await post.insert()

        posts = await Post.find_all({}, resolve_refs=False)
        assert len(posts) == 1
        assert posts[0].author._id == saved_user._id
        assert posts[0].author.name == ""

    async def test_find_cursor_resolves(self, saved_user, saved_tags):
        for i in range(3):
            post = Post(title=f"Cursor-{i}", author=saved_user, tags=saved_tags)
            await post.insert()

        cursor = await Post.find({})
        async for p in cursor:
            assert isinstance(p.author, User)
            assert p.author.name == "Alice"

    async def test_find_cursor_to_list_batch_resolves(self, saved_user, saved_tags):
        for i in range(3):
            post = Post(title=f"ToList-{i}", author=saved_user, tags=saved_tags)
            await post.insert()

        cursor = await Post.find({})
        posts = await cursor.to_list()
        assert len(posts) == 3
        for p in posts:
            assert p.author.name == "Alice"
            assert len(p.tags) == 3

    async def test_find_cursor_resolve_refs_false(self, saved_user):
        post = Post(title="CursorNoRef", author=saved_user)
        await post.insert()

        cursor = await Post.find({}, resolve_refs=False)
        async for p in cursor:
            assert p.author._id == saved_user._id
            assert p.author.name == ""

    async def test_optional_ref_resolved(self, saved_user):
        reviewer = User(name="Bob")
        await reviewer.insert()
        post = Post(title="OptRef", author=saved_user, reviewer=reviewer)
        await post.insert()

        found = await Post.find_one({"title": "OptRef"})
        assert found.reviewer is not None
        assert isinstance(found.reviewer, User)
        assert found.reviewer.name == "Bob"

    async def test_optional_ref_none(self, saved_user):
        post = Post(title="OptNone", author=saved_user)
        await post.insert()

        found = await Post.find_one({"title": "OptNone"})
        assert found.reviewer is None

    async def test_list_refs_resolved(self, saved_user, saved_tags):
        post = Post(title="ListRef", author=saved_user, tags=saved_tags)
        await post.insert()

        found = await Post.find_one({"title": "ListRef"})
        assert len(found.tags) == 3
        labels = {t.label for t in found.tags}
        assert labels == {"tag-0", "tag-1", "tag-2"}


# ===========================================================================
# Nested / self-referencing
# ===========================================================================

class TestNestedRefs:

    async def test_chain_fetch(self, init_models):
        root = Category(title="Root")
        await root.insert()
        child = Category(title="Child", parent=root)
        await child.insert()
        leaf = Category(title="Leaf", parent=child)
        await leaf.insert()

        found = await Category.find_one({"title": "Leaf"})
        assert found is not None
        assert isinstance(found.parent, Category)
        assert found.parent.title == "Child"
        assert isinstance(found.parent.parent, Category)
        assert found.parent.parent.title == "Root"
        assert found.parent.parent.parent is None

    async def test_nested_resolve_refs_false(self, init_models):
        root = Category(title="Root")
        await root.insert()
        child = Category(title="Child", parent=root)
        await child.insert()

        found = await Category.find_one({"title": "Child"}, resolve_refs=False)
        assert found.parent._id == root._id
        assert found.parent.title == ""  # zero-value, not real data


# ===========================================================================
# Cascading save
# ===========================================================================

class TestCascadingSave:

    async def test_save_refs_true(self, saved_user):
        post = Post(title="Cascade", author=saved_user)
        await post.insert()

        found = await Post.find_one({"title": "Cascade"})
        found.author.name = "Alice Updated"

        await found.save()  # save_refs=True by default

        db_author = await User.find_by_id(saved_user._id)
        assert db_author.name == "Alice Updated"

    async def test_save_refs_false(self, saved_user):
        post = Post(title="NoSave", author=saved_user)
        await post.insert()

        found = await Post.find_one({"title": "NoSave"})
        found.author.name = "Should Not Save"

        await found.save(save_refs=False)

        db_author = await User.find_by_id(saved_user._id)
        assert db_author.name == "Alice"  # unchanged

    async def test_save_refs_list(self, saved_user, saved_tags):
        post = Post(title="ListSave", author=saved_user, tags=saved_tags)
        await post.insert()

        found = await Post.find_one({"title": "ListSave"})
        found.tags[0].label = "modified-tag"

        await found.save()

        db_tag = await Tag.find_by_id(saved_tags[0]._id)
        assert db_tag.label == "modified-tag"

    async def test_save_refs_unresolved_skipped(self, saved_user, saved_tags):
        """With resolve_refs=False + save_refs=False, stub refs should not crash."""
        post = Post(title="SkipUnresolved", author=saved_user, tags=saved_tags)
        await post.insert()

        found = await Post.find_one({"title": "SkipUnresolved"}, resolve_refs=False)
        # save_refs=False skips cascading save; stub refs have _id so dump() works
        await found.save(save_refs=False)

        db_user = await User.find_by_id(saved_user._id)
        assert db_user.name == "Alice"


# ===========================================================================
# Insert validation
# ===========================================================================

class TestInsertValidation:

    async def test_insert_unsaved_ref_raises(self, init_models):
        unsaved = User(name="Ghost")
        post = Post(title="Fail", author=unsaved)
        with pytest.raises(ValueError, match="unsaved"):
            await post.insert()

    async def test_insert_unsaved_list_ref_raises(self, saved_user, init_models):
        unsaved_tag = Tag(label="ghost")
        post = Post(title="Fail", author=saved_user, tags=[unsaved_tag])
        with pytest.raises(ValueError, match="unsaved"):
            await post.insert()

    async def test_insert_one_unsaved_ref_raises(self, init_models):
        unsaved = User(name="Ghost")
        post = Post(title="Fail", author=unsaved)
        with pytest.raises(ValueError, match="unsaved"):
            await Post.insert_one(post)


# ===========================================================================
# Batch resolve performance (deduplication)
# ===========================================================================

class TestBatchResolve:

    async def test_many_posts_same_author(self, saved_user):
        """100 posts referencing the same author — resolved in 1 query."""
        for i in range(100):
            post = Post(title=f"P-{i}", author=saved_user)
            await post.insert()

        posts = await Post.find_all({})
        assert len(posts) == 100
        for p in posts:
            assert p.author.name == "Alice"

    async def test_many_posts_many_tags(self, saved_user, saved_tags):
        """Posts with tag lists — batch resolved."""
        for i in range(10):
            post = Post(title=f"T-{i}", author=saved_user, tags=saved_tags)
            await post.insert()

        posts = await Post.find_all({})
        assert len(posts) == 10
        for p in posts:
            assert len(p.tags) == 3
            labels = {t.label for t in p.tags}
            assert labels == {"tag-0", "tag-1", "tag-2"}


# ===========================================================================
# Roundtrip
# ===========================================================================

class TestRoundtrip:

    async def test_insert_find_roundtrip(self, saved_user, saved_tags):
        post = Post(
            title="Roundtrip",
            author=saved_user,
            tags=saved_tags,
        )
        await post.insert()

        found = await Post.find_one({"title": "Roundtrip"})
        assert found is not None
        assert found.author.name == "Alice"
        assert len(found.tags) == 3
        assert {t.label for t in found.tags} == {"tag-0", "tag-1", "tag-2"}

    async def test_query_by_ref_field(self, saved_user):
        """Querying by ref field uses plain ObjectId."""
        post = Post(title="Query", author=saved_user)
        await post.insert()

        found = await Post.find_all({"author": saved_user._id})
        assert len(found) == 1
        assert found[0].title == "Query"

    async def test_find_one_and_update_resolves(self, saved_user):
        post = Post(title="FAU", author=saved_user)
        await post.insert()

        updated = await Post.find_one_and_update(
            {"title": "FAU"},
            {"$set": {"title": "FAU-Updated"}},
            return_updated=True,
        )
        assert updated is not None
        assert updated.title == "FAU-Updated"
        assert isinstance(updated.author, User)
        assert updated.author.name == "Alice"

    async def test_find_one_and_update_resolve_refs_false(self, saved_user):
        post = Post(title="FAU2", author=saved_user)
        await post.insert()

        updated = await Post.find_one_and_update(
            {"title": "FAU2"},
            {"$set": {"title": "FAU2-Updated"}},
            return_updated=True,
            resolve_refs=False,
        )
        assert updated is not None
        assert updated.author._id == saved_user._id
        assert updated.author.name == ""  # zero-value, not real data
