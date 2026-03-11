"""
Key-value store built on top of MongoDocument.

Provides a simple async key-value interface over a MongoDB collection.
Designed for use via multiple inheritance with project-specific base documents.

.. code-block:: python

    from mongospec.contrib.kv_store import KVStore, KVStoreItem

    class AppStorage(KVStore, Document):
        __collection_name__ = "app_storage"

    max_retries = KVStoreItem[int](AppStorage, "max_retries", default=3)
    value = await max_retries.get()
    await max_retries.set(5)
"""

from __future__ import annotations

from collections.abc import Sequence
from typing import Any, ClassVar, Generic, TypeVar

import mongojet

from mongospec.document.document import MongoDocument

T = TypeVar("T")


class KVStore(MongoDocument, kw_only=True):
    """
    Async key-value store backed by a MongoDB collection.

    Each document represents a single key-value pair.
    Combine with a project-specific base document via multiple inheritance
    to inherit lifecycle hooks, timestamps, etc.

    .. code-block:: python

        class AppStorage(KVStore, Document):
            __collection_name__ = "app_storage"

        await AppStorage.set("theme", "dark")
        theme = await AppStorage.get("theme")
    """

    __indexes__: ClassVar[Sequence[mongojet.IndexModel]] = [
        mongojet.IndexModel(keys=[("key", 1)], unique=True),  # type: ignore[call-arg]
    ]

    key: str
    value: Any | None = None

    @classmethod
    async def set(cls, key: str, value: Any | None) -> None:
        """
        Upsert a value by key.

        :param key: Unique setting key.
        :param value: Value to store; may be ``None``.
        """
        await cls.update_one(
            {"key": key},
            {"$set": {"value": value}},
            upsert=True,
        )

    @classmethod
    async def get(cls, key: str) -> Any | None:
        """
        Retrieve a value by key.

        :param key: Unique setting key.
        :return: The stored value (may be ``None``).
        :raises KeyError: If the key does not exist.
        """
        document = await cls.find_one({"key": key})
        if document is None:
            raise KeyError(key)
        return document.value

    @classmethod
    async def get_or_default(cls, key: str, default: Any | None = None) -> Any | None:
        """
        Retrieve a value by key, returning *default* if missing.

        Unlike :meth:`get`, this never raises :exc:`KeyError`.

        :param key: Unique setting key.
        :param default: Fallback value when the key is absent.
        :return: The stored value or *default*.
        """
        document = await cls.find_one({"key": key})
        if document is None:
            return default
        return document.value

    @classmethod
    async def set_default(cls, key: str, value: Any | None) -> Any | None:
        """
        Atomically set a value only if the key does not already exist.

        :param key: Unique setting key.
        :param value: Value to store when the key is missing.
        :return: The existing value if present, otherwise *value*.
        """
        doc = await cls.find_one_and_update(
            {"key": key},
            {"$setOnInsert": {"key": key, "value": value}},
            upsert=True,
            return_updated=True,
        )
        return doc.value if doc is not None else value

    @classmethod
    async def delete_key(cls, key: str) -> bool:
        """
        Delete a key-value pair.

        :param key: Unique setting key.
        :return: ``True`` if the key existed and was deleted.
        """
        return await cls.delete_one({"key": key}) > 0

    @classmethod
    async def has(cls, key: str) -> bool:
        """
        Check whether a key exists.

        :param key: Unique setting key.
        :return: ``True`` if the key is present.
        """
        return await cls.exists({"key": key})

    @classmethod
    async def get_all(cls) -> dict[str, Any | None]:
        """
        Retrieve all key-value pairs as a dictionary.

        :return: Mapping of all stored keys to their values.
        """
        docs = await cls.find_all({})
        return {doc.key: doc.value for doc in docs}

    @classmethod
    async def keys(cls) -> list[str]:
        """
        Retrieve all stored keys.

        :return: List of key names.
        """
        docs = await cls.find_all({})
        return [doc.key for doc in docs]

    @classmethod
    async def set_many(cls, items: dict[str, Any | None]) -> None:
        """
        Upsert multiple key-value pairs at once.

        :param items: Mapping of keys to values.
        """
        for key, value in items.items():
            await cls.set(key, value)


class KVStoreItem(Generic[T]):
    """
    Typed accessor for a single key in a :class:`KVStore` collection.

    .. code-block:: python

        max_retries = KVStoreItem[int](AppStorage, "max_retries", default=3)

        value = await max_retries.get()   # int | None
        await max_retries.set(10)

    :param store: The ``KVStore`` subclass to use.
    :param key: Unique setting key.
    :param default: Default value returned (and persisted) when the key is missing.
    """

    def __init__(
        self,
        store: type[KVStore],
        key: str,
        default: T | None = None,
    ) -> None:
        self._store = store
        self._key = key
        self._default = default

    @classmethod
    def of(cls, store: type[KVStore]) -> type[KVStoreItem]:
        """
        Create a ``KVStoreItem`` subclass bound to a specific store.

        .. code-block:: python

            AppStorageItem = KVStoreItem.of(AppStorage)
            max_retries = AppStorageItem[int]("max_retries", default=3)

        :param store: The ``KVStore`` subclass to bind.
        :return: A new ``KVStoreItem`` subclass with *store* pre-filled.
        """
        bound_store = store

        class BoundKVStoreItem(cls):  # type: ignore[misc]
            def __init__(
                self,
                key: str,
                default: T | None = None,
            ) -> None:
                super().__init__(store=bound_store, key=key, default=default)

        BoundKVStoreItem.__name__ = f"{store.__name__}Item"
        BoundKVStoreItem.__qualname__ = f"{store.__name__}Item"
        return BoundKVStoreItem

    async def get(self) -> T | None:
        """
        Get the stored value, returning *default* if missing.

        Does **not** persist the default — use :meth:`set_default`
        to atomically initialize a key.

        :return: The stored value cast to ``T``, or the default.
        """
        return await self._store.get_or_default(self._key, self._default)  # type: ignore[return-value]

    async def set(self, value: T | None) -> None:
        """
        Set the stored value.

        :param value: New value to store; may be ``None``.
        """
        await self._store.set(self._key, value)

    async def set_default(self) -> T | None:
        """
        Atomically persist *default* only if the key is missing.

        :return: The existing value if present, otherwise the default.
        """
        return await self._store.set_default(self._key, self._default)  # type: ignore[return-value]

    async def delete(self) -> bool:
        """
        Delete the key from the store.

        :return: ``True`` if the key existed.
        """
        return await self._store.delete_key(self._key)

    async def has(self) -> bool:
        """
        Check whether the key exists.

        :return: ``True`` if the key is present.
        """
        return await self._store.has(self._key)
