"""Transparent document reference detection and resolution.

Any field typed as a MongoDocument subclass is automatically treated as
a reference — stored as ObjectId in MongoDB and resolved on read.
"""
from __future__ import annotations

import types
from typing import Any, NamedTuple, Union, get_args, get_origin

from bson import ObjectId


def is_document_type(tp: Any) -> bool:
    """Check if *tp* is a MongoDocument subclass (any inheritance depth)."""
    from mongospec.document.document import MongoDocument
    return isinstance(tp, type) and issubclass(tp, MongoDocument) and tp is not MongoDocument


class RefFieldInfo(NamedTuple):
    document_class: type
    is_list: bool
    is_optional: bool


def _analyze_type(hint: Any) -> RefFieldInfo | None:
    """Return RefFieldInfo if *hint* resolves to a MongoDocument reference."""
    origin = get_origin(hint)

    # list[SomeDoc]
    if origin is list:
        args = get_args(hint)
        if args and is_document_type(args[0]):
            return RefFieldInfo(args[0], is_list=True, is_optional=False)
        return None

    # SomeDoc | None  or  list[SomeDoc] | None
    if origin is Union or isinstance(hint, types.UnionType):
        args = get_args(hint)
        non_none = [a for a in args if a is not type(None)]
        if len(non_none) == 1:
            inner = non_none[0]
            if get_origin(inner) is list:
                list_args = get_args(inner)
                if list_args and is_document_type(list_args[0]):
                    return RefFieldInfo(list_args[0], is_list=True, is_optional=True)
            elif is_document_type(inner):
                return RefFieldInfo(inner, is_list=False, is_optional=True)
        return None

    # SomeDoc (direct)
    if is_document_type(hint):
        return RefFieldInfo(hint, is_list=False, is_optional=False)

    return None


def detect_ref_fields(cls: type) -> dict[str, RefFieldInfo]:
    """Detect all reference fields on *cls* by inspecting type hints.

    Called once per class, result is cached on ``cls.__ref_fields__``.
    """
    from typing import get_type_hints
    try:
        hints = get_type_hints(cls)
    except Exception:
        return {}
    result: dict[str, RefFieldInfo] = {}
    for name, hint in hints.items():
        if name.startswith("__"):
            continue
        info = _analyze_type(hint)
        if info is not None:
            result[name] = info
    return result


async def resolve_ref_data(ref_fields: dict[str, RefFieldInfo], raw: dict[str, Any]) -> dict[str, Any]:
    """Replace ObjectIds with full document dicts for a single raw document.

    Recursively resolves nested ref fields in fetched documents.
    """
    if not ref_fields:
        return raw

    ids_by_class: dict[type, set[ObjectId]] = {}
    for field_name, info in ref_fields.items():
        value = raw.get(field_name)
        if value is None:
            continue
        if info.is_list:
            for item in value:
                if isinstance(item, ObjectId):
                    ids_by_class.setdefault(info.document_class, set()).add(item)
        elif isinstance(value, ObjectId):
            ids_by_class.setdefault(info.document_class, set()).add(value)

    if not ids_by_class:
        return raw

    fetched = await _batch_fetch(ids_by_class)
    result = _replace_ids(ref_fields, raw, fetched)

    # Recursively resolve ref fields inside the fetched documents
    for field_name, info in ref_fields.items():
        nested_ref_fields = info.document_class._get_ref_fields()
        if not nested_ref_fields:
            continue
        value = result.get(field_name)
        if value is None:
            continue
        if info.is_list:
            for i, item in enumerate(value):
                if isinstance(item, dict):
                    result[field_name][i] = await resolve_ref_data(nested_ref_fields, item)
        elif isinstance(value, dict):
            result[field_name] = await resolve_ref_data(nested_ref_fields, value)

    return result


async def resolve_ref_data_batch(
    ref_fields: dict[str, RefFieldInfo], docs: list[dict[str, Any]]
) -> list[dict[str, Any]]:
    """Replace ObjectIds with full document dicts across a batch of raw documents.

    Recursively resolves nested ref fields in fetched documents.
    """
    if not ref_fields or not docs:
        return docs

    ids_by_class: dict[type, set[ObjectId]] = {}
    for doc in docs:
        for field_name, info in ref_fields.items():
            value = doc.get(field_name)
            if value is None:
                continue
            if info.is_list:
                for item in value:
                    if isinstance(item, ObjectId):
                        ids_by_class.setdefault(info.document_class, set()).add(item)
            elif isinstance(value, ObjectId):
                ids_by_class.setdefault(info.document_class, set()).add(value)

    if not ids_by_class:
        return docs

    fetched = await _batch_fetch(ids_by_class)
    results = [_replace_ids(ref_fields, doc, fetched) for doc in docs]

    # Recursively resolve nested refs in the fetched documents
    needs_recursion = {
        name: info for name, info in ref_fields.items()
        if info.document_class._get_ref_fields()
    }
    if needs_recursion:
        for result in results:
            for field_name, info in needs_recursion.items():
                nested_ref_fields = info.document_class._get_ref_fields()
                value = result.get(field_name)
                if value is None:
                    continue
                if info.is_list:
                    for i, item in enumerate(value):
                        if isinstance(item, dict):
                            result[field_name][i] = await resolve_ref_data(nested_ref_fields, item)
                elif isinstance(value, dict):
                    result[field_name] = await resolve_ref_data(nested_ref_fields, value)

    return results


async def _batch_fetch(ids_by_class: dict[type, set[ObjectId]]) -> dict[ObjectId, dict[str, Any]]:
    """Fetch documents for each class with a single $in query, return by _id."""
    result: dict[ObjectId, dict[str, Any]] = {}
    for doc_class, ids in ids_by_class.items():
        collection = doc_class.get_collection()
        cursor = await collection.find({"_id": {"$in": list(ids)}})
        for doc in await cursor.to_list():
            result[doc["_id"]] = doc
    return result


def _replace_ids(
    ref_fields: dict[str, RefFieldInfo],
    raw: dict[str, Any],
    fetched: dict[ObjectId, dict[str, Any]],
) -> dict[str, Any]:
    """Replace ObjectIds in *raw* with fetched document dicts."""
    out = dict(raw)
    for field_name, info in ref_fields.items():
        value = out.get(field_name)
        if value is None:
            continue
        if info.is_list:
            out[field_name] = [
                fetched.get(item, item) if isinstance(item, ObjectId) else item
                for item in value
            ]
        elif isinstance(value, ObjectId):
            resolved = fetched.get(value)
            if resolved is not None:
                out[field_name] = resolved
    return out


# ---------------------------------------------------------------------------
# Stub helpers for resolve_refs=False
# ---------------------------------------------------------------------------

def _make_stub_dict(doc_class: type, oid: ObjectId) -> dict[str, Any]:
    """Create a minimal dict that passes ``msgspec.convert`` for *doc_class*.

    Only ``_id`` is meaningful; required fields get type-appropriate zeros.
    Recursively stubs nested ref fields.
    """
    import msgspec.structs

    stub: dict[str, Any] = {"_id": oid}
    for f in msgspec.structs.fields(doc_class):
        if f.name == "_id":
            continue
        if f.default is not msgspec.NODEFAULT or f.default_factory is not msgspec.NODEFAULT:
            continue
        # Required field — use type-based zero
        stub[f.name] = _zero_for_annotation(f.type)
    return stub


def _zero_for_annotation(tp: Any) -> Any:
    """Return a type-appropriate zero value for a required struct field."""
    _ZEROS: dict[type, Any] = {str: "", int: 0, float: 0.0, bool: False, bytes: b""}
    if tp in _ZEROS:
        return _ZEROS[tp]
    origin = get_origin(tp)
    if origin is list:
        return []
    if origin is dict:
        return {}
    if origin is Union or isinstance(tp, types.UnionType):
        args = get_args(tp)
        if type(None) in args:
            return None
    return None


def stub_ref_data(ref_fields: dict[str, RefFieldInfo], raw: dict[str, Any]) -> dict[str, Any]:
    """Replace ObjectIds in ref fields with stub dicts (no DB queries).

    Used when ``resolve_refs=False`` — allows ``msgspec.convert`` to succeed.
    The resulting instances have correct ``_id`` and zero-values for other fields.
    """
    out = dict(raw)
    for field_name, info in ref_fields.items():
        value = out.get(field_name)
        if value is None:
            continue
        if info.is_list:
            out[field_name] = [
                _make_stub_dict(info.document_class, item)
                if isinstance(item, ObjectId) else item
                for item in value
            ]
        elif isinstance(value, ObjectId):
            out[field_name] = _make_stub_dict(info.document_class, value)
    return out
