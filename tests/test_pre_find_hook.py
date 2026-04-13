"""Тесты для async lifecycle-хука `__pre_find__`.

Проверяет, что хук вызывается во всех read/update/delete операциях,
обходится через ``skip_hooks=True``, и не вмешивается в instance-level
``delete()`` и ``find_by_id``/``update_by_id``/``delete_by_id``
(addressed-by-id операции).
"""
from __future__ import annotations

import asyncio
from typing import Any

import pytest_asyncio

import mongospec
from mongospec import MongoDocument


# ── Test models ─────────────────────────────────────────────────────────────

# Модуль-уровневый флаг для управления поведением хука внутри тестов.
_scope_active = False


class ScopedDoc(MongoDocument):
    __collection_name__ = "test_scoped_docs"

    name: str = ""
    archived: bool = False
    tag: str = ""

    @classmethod
    async def __pre_find__(cls, filter: dict[str, Any]) -> dict[str, Any]:
        # Имитация реальной async-логики: реальный await
        await asyncio.sleep(0)
        if not _scope_active:
            return filter
        # Инжектируем archived=False
        return {**filter, "archived": False}

    @classmethod
    async def __pre_update__(cls, update: dict[str, Any]) -> dict[str, Any]:
        # Маркируем все update-документы для проверки порядка
        update.setdefault("$set", {})["tag"] = "touched"
        return update


# ── Fixtures ────────────────────────────────────────────────────────────────


@pytest_asyncio.fixture
async def init_models(db):
    await mongospec.init(db, document_types=[ScopedDoc])


@pytest_asyncio.fixture
async def seeded(init_models):
    """Засевает 3 документа: 2 активных + 1 архивный."""
    global _scope_active
    _scope_active = False
    docs = [
        ScopedDoc(name="alpha", archived=False),
        ScopedDoc(name="beta", archived=False),
        ScopedDoc(name="gamma", archived=True),
    ]
    for doc in docs:
        await doc.insert()
    yield docs
    _scope_active = False


# ── find* ───────────────────────────────────────────────────────────────────


async def test_find_one_applies_hook(seeded):
    global _scope_active
    _scope_active = True
    # Архивный документ не должен находиться
    assert await ScopedDoc.find_one({"name": "gamma"}) is None
    assert (await ScopedDoc.find_one({"name": "alpha"})).name == "alpha"


async def test_find_one_skip_hooks(seeded):
    global _scope_active
    _scope_active = True
    found = await ScopedDoc.find_one({"name": "gamma"}, skip_hooks=True)
    assert found is not None
    assert found.archived is True


async def test_find_all_applies_hook(seeded):
    global _scope_active
    _scope_active = True
    docs = await ScopedDoc.find_all({})
    assert len(docs) == 2
    assert all(d.archived is False for d in docs)


async def test_find_all_skip_hooks(seeded):
    global _scope_active
    _scope_active = True
    docs = await ScopedDoc.find_all({}, skip_hooks=True)
    assert len(docs) == 3


async def test_find_cursor_applies_hook(seeded):
    global _scope_active
    _scope_active = True
    docs = await ScopedDoc.find({}).to_list()
    assert len(docs) == 2


async def test_find_cursor_async_for(seeded):
    global _scope_active
    _scope_active = True
    names = []
    async for doc in ScopedDoc.find({}):
        names.append(doc.name)
    assert sorted(names) == ["alpha", "beta"]


async def test_find_cursor_skip_hooks(seeded):
    global _scope_active
    _scope_active = True
    docs = await ScopedDoc.find({}, skip_hooks=True).to_list()
    assert len(docs) == 3


async def test_count_applies_hook(seeded):
    global _scope_active
    _scope_active = True
    assert await ScopedDoc.count() == 2
    assert await ScopedDoc.count(skip_hooks=True) == 3


async def test_exists_applies_hook(seeded):
    global _scope_active
    _scope_active = True
    assert await ScopedDoc.exists({"name": "alpha"}) is True
    # Архивный не существует с точки зрения скоупа
    assert await ScopedDoc.exists({"name": "gamma"}) is False
    assert await ScopedDoc.exists({"name": "gamma"}, skip_hooks=True) is True


async def test_find_by_id_applies_hook(seeded):
    global _scope_active
    _scope_active = True
    archived_id = next(d._id for d in seeded if d.archived)
    # Хук применяется и к find_by_id (по плану)
    assert await ScopedDoc.find_by_id(archived_id) is None
    assert await ScopedDoc.find_by_id(archived_id, skip_hooks=True) is not None


# ── delete* ─────────────────────────────────────────────────────────────────


async def test_delete_one_applies_hook(seeded):
    global _scope_active
    _scope_active = True
    deleted = await ScopedDoc.delete_one({"name": "gamma"})
    assert deleted == 0  # архивный недостижим из-за хука
    deleted = await ScopedDoc.delete_one({"name": "gamma"}, skip_hooks=True)
    assert deleted == 1


async def test_delete_many_applies_hook(seeded):
    global _scope_active
    _scope_active = True
    # Без обхода хука "удалить всё" удалит только незаархивированные
    deleted = await ScopedDoc.delete_many({})
    assert deleted == 2
    assert await ScopedDoc.count(skip_hooks=True) == 1


async def test_delete_by_id_bypasses_hook(seeded):
    global _scope_active
    _scope_active = True
    archived_id = next(d._id for d in seeded if d.archived)
    # delete_by_id всегда обходит хук — удаление по известному id
    deleted = await ScopedDoc.delete_by_id(archived_id)
    assert deleted == 1


async def test_instance_delete_bypasses_hook(seeded):
    global _scope_active
    _scope_active = True
    archived = next(d for d in seeded if d.archived)
    deleted = await archived.delete()
    assert deleted == 1


# ── update* ─────────────────────────────────────────────────────────────────


async def test_update_one_applies_both_hooks(seeded):
    global _scope_active
    _scope_active = True
    # Архивный недостижим — модификация не произойдёт
    modified = await ScopedDoc.update_one(
        {"name": "gamma"}, {"$set": {"name": "gamma2"}}
    )
    assert modified == 0

    # Активный обновится, и __pre_update__ добавит tag="touched"
    modified = await ScopedDoc.update_one(
        {"name": "alpha"}, {"$set": {"name": "alpha2"}}
    )
    assert modified == 1
    found = await ScopedDoc.find_one({"name": "alpha2"})
    assert found.tag == "touched"


async def test_update_many_applies_hook(seeded):
    global _scope_active
    _scope_active = True
    modified = await ScopedDoc.update_many({}, {"$set": {"name": "renamed"}})
    assert modified == 2  # архивный нетронут


async def test_update_by_id_bypasses_pre_find(seeded):
    global _scope_active
    _scope_active = True
    archived = next(d for d in seeded if d.archived)
    # update_by_id обходит __pre_find__, но __pre_update__ всё равно работает
    modified = await ScopedDoc.update_by_id(
        archived._id, {"$set": {"name": "renamed"}}
    )
    assert modified == 1
    found = await ScopedDoc.find_by_id(archived._id, skip_hooks=True)
    assert found.name == "renamed"
    assert found.tag == "touched"  # __pre_update__ сработал


async def test_find_one_and_update_applies_hooks(seeded):
    global _scope_active
    _scope_active = True
    # Архивный недостижим
    result = await ScopedDoc.find_one_and_update(
        {"name": "gamma"}, {"$set": {"name": "gamma2"}}
    )
    assert result is None

    # Активный обновляется и помечается tag="touched"
    result = await ScopedDoc.find_one_and_update(
        {"name": "alpha"}, {"$set": {"name": "alpha2"}}
    )
    assert result is not None
    assert result.name == "alpha2"
    assert result.tag == "touched"


async def test_skip_hooks_disables_both_hooks_in_update(seeded):
    global _scope_active
    _scope_active = True
    # skip_hooks=True должен отключить и __pre_find__ и __pre_update__
    modified = await ScopedDoc.update_one(
        {"name": "gamma"},
        {"$set": {"name": "gamma2"}},
        skip_hooks=True,
    )
    assert modified == 1
    found = await ScopedDoc.find_one({"name": "gamma2"}, skip_hooks=True)
    assert found.tag == ""  # __pre_update__ не сработал


# ── Hook off ────────────────────────────────────────────────────────────────


async def test_hook_passthrough_when_disabled(seeded):
    global _scope_active
    _scope_active = False
    docs = await ScopedDoc.find_all({})
    assert len(docs) == 3
