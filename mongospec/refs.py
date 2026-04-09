"""Transparent document reference detection and resolution.

Any field typed as a MongoDocument subclass is automatically treated as
a reference — stored as ObjectId in MongoDB and resolved on read.
Nested references inside ``msgspec.Struct`` are supported too.
"""
from __future__ import annotations

import types
from dataclasses import dataclass, field, replace
from typing import Any, Union, get_args, get_origin

import msgspec
from bson import ObjectId


def is_document_type(tp: Any) -> bool:
    """Check if *tp* is a MongoDocument subclass (any inheritance depth)."""
    from mongospec.document.document import MongoDocument
    return isinstance(tp, type) and issubclass(tp, MongoDocument) and tp is not MongoDocument


def is_struct_type(tp: Any) -> bool:
    """Check if *tp* is a non-document msgspec.Struct subclass."""
    return (
        isinstance(tp, type)
        and issubclass(tp, msgspec.Struct)
        and not is_document_type(tp)
    )


@dataclass(slots=True, frozen=True)
class RefFieldInfo:
    document_class: type | None = None
    is_list: bool = False
    is_optional: bool = False
    nested_fields: dict[str, "RefFieldInfo"] = field(default_factory=dict)

    @property
    def is_document_ref(self) -> bool:
        return self.document_class is not None


def _analyze_type(hint: Any, *, seen: set[type]) -> RefFieldInfo | None:
    """Return RefFieldInfo if *hint* contains a MongoDocument reference."""
    origin = get_origin(hint)

    if origin is Union or isinstance(hint, types.UnionType):
        args = get_args(hint)
        non_none = [a for a in args if a is not type(None)]
        if len(non_none) == 1:
            inner = _analyze_type(non_none[0], seen=seen)
            if inner is not None:
                return replace(inner, is_optional=True)
        return None

    if origin is list:
        args = get_args(hint)
        if not args:
            return None
        inner = _analyze_type(args[0], seen=seen)
        if inner is None or inner.is_list:
            return None
        return replace(inner, is_list=True)

    if is_document_type(hint):
        return RefFieldInfo(document_class=hint)

    if is_struct_type(hint):
        if hint in seen:
            return None
        nested_fields = detect_ref_fields(hint, _seen=seen | {hint})
        if nested_fields:
            return RefFieldInfo(nested_fields=nested_fields)

    return None


def detect_ref_fields(cls: type, *, _seen: set[type] | None = None) -> dict[str, RefFieldInfo]:
    """Detect all reference fields on *cls* by inspecting type hints.

    Called once per class, result is cached on ``cls.__ref_fields__``.
    """
    from typing import get_type_hints

    seen = (_seen or set()) | {cls}
    try:
        hints = get_type_hints(cls)
    except Exception:
        return {}
    result: dict[str, RefFieldInfo] = {}
    for name, hint in hints.items():
        if name.startswith("__"):
            continue
        info = _analyze_type(hint, seen=seen)
        if info is not None:
            result[name] = info
    return result


def iter_ref_documents(ref_fields: dict[str, RefFieldInfo], source: Any):
    """Yield resolved MongoDocument instances found by ref metadata."""
    for field_name, info in ref_fields.items():
        value = _get_value(source, field_name)
        if value is None:
            continue
        yield from _iter_ref_documents_for_value(info, value)


def collapse_ref_data(ref_fields: dict[str, RefFieldInfo], raw: dict[str, Any]) -> dict[str, Any]:
    """Collapse resolved references back to ObjectIds for MongoDB storage."""
    out = dict(raw)
    for field_name, info in ref_fields.items():
        if field_name not in out:
            continue
        out[field_name] = _collapse_value(field_name, info, out[field_name])
    return out


async def resolve_ref_data(ref_fields: dict[str, RefFieldInfo], raw: dict[str, Any]) -> dict[str, Any]:
    """Replace ObjectIds with full document dicts for a single raw document."""
    if not ref_fields:
        return raw

    ids_by_class: dict[type, set[ObjectId]] = {}
    _collect_ref_ids(ref_fields, raw, ids_by_class)
    fetched = await _batch_fetch(ids_by_class) if ids_by_class else {}
    return await _resolve_fields(ref_fields, raw, fetched)


async def resolve_ref_data_batch(
    ref_fields: dict[str, RefFieldInfo], docs: list[dict[str, Any]]
) -> list[dict[str, Any]]:
    """Replace ObjectIds with full document dicts across a batch of raw documents."""
    if not ref_fields or not docs:
        return docs

    ids_by_class: dict[type, set[ObjectId]] = {}
    for doc in docs:
        _collect_ref_ids(ref_fields, doc, ids_by_class)
    fetched = await _batch_fetch(ids_by_class) if ids_by_class else {}
    return [await _resolve_fields(ref_fields, doc, fetched) for doc in docs]


async def _batch_fetch(ids_by_class: dict[type, set[ObjectId]]) -> dict[ObjectId, dict[str, Any]]:
    """Fetch documents for each class with a single $in query, return by _id."""
    result: dict[ObjectId, dict[str, Any]] = {}
    for doc_class, ids in ids_by_class.items():
        collection = doc_class.get_collection()
        cursor = await collection.find({"_id": {"$in": list(ids)}})
        for doc in await cursor.to_list():
            result[doc["_id"]] = doc
    return result


async def _resolve_fields(
    ref_fields: dict[str, RefFieldInfo],
    raw: dict[str, Any],
    fetched: dict[ObjectId, dict[str, Any]],
) -> dict[str, Any]:
    out = dict(raw)
    for field_name, info in ref_fields.items():
        value = out.get(field_name)
        if value is None:
            continue
        out[field_name] = await _resolve_value(info, value, fetched)
    return out


async def _resolve_value(
    info: RefFieldInfo,
    value: Any,
    fetched: dict[ObjectId, dict[str, Any]],
) -> Any:
    if value is None:
        return value

    if info.is_document_ref:
        if info.is_list:
            return [
                await _resolve_document_value(info.document_class, item, fetched)
                for item in value
            ]
        return await _resolve_document_value(info.document_class, value, fetched)

    if info.is_list:
        return [
            await _resolve_struct_value(info.nested_fields, item, fetched)
            for item in value
        ]
    return await _resolve_struct_value(info.nested_fields, value, fetched)


async def _resolve_document_value(
    doc_class: type,
    value: Any,
    fetched: dict[ObjectId, dict[str, Any]],
) -> Any:
    if isinstance(value, ObjectId):
        resolved = fetched.get(value)
        if resolved is None:
            return value
        value = resolved

    if isinstance(value, dict):
        nested_ref_fields = doc_class._get_ref_fields()
        if nested_ref_fields:
            return await resolve_ref_data(nested_ref_fields, value)
    return value


async def _resolve_struct_value(
    nested_fields: dict[str, RefFieldInfo],
    value: Any,
    fetched: dict[ObjectId, dict[str, Any]],
) -> Any:
    if isinstance(value, dict):
        return await _resolve_fields(nested_fields, value, fetched)
    return value


def _collect_ref_ids(
    ref_fields: dict[str, RefFieldInfo],
    raw: dict[str, Any],
    ids_by_class: dict[type, set[ObjectId]],
) -> None:
    for field_name, info in ref_fields.items():
        value = raw.get(field_name)
        if value is None:
            continue
        _collect_value_ids(info, value, ids_by_class)


def _collect_value_ids(
    info: RefFieldInfo,
    value: Any,
    ids_by_class: dict[type, set[ObjectId]],
) -> None:
    if value is None:
        return

    if info.is_document_ref:
        if info.is_list:
            for item in value:
                if isinstance(item, ObjectId):
                    ids_by_class.setdefault(info.document_class, set()).add(item)
                elif isinstance(item, dict):
                    nested_ref_fields = info.document_class._get_ref_fields()
                    if nested_ref_fields:
                        _collect_ref_ids(nested_ref_fields, item, ids_by_class)
            return

        if isinstance(value, ObjectId):
            ids_by_class.setdefault(info.document_class, set()).add(value)
        elif isinstance(value, dict):
            nested_ref_fields = info.document_class._get_ref_fields()
            if nested_ref_fields:
                _collect_ref_ids(nested_ref_fields, value, ids_by_class)
        return

    if info.is_list:
        for item in value:
            if isinstance(item, dict):
                _collect_ref_ids(info.nested_fields, item, ids_by_class)
        return

    if isinstance(value, dict):
        _collect_ref_ids(info.nested_fields, value, ids_by_class)


def _get_value(source: Any, field_name: str) -> Any:
    if isinstance(source, dict):
        return source.get(field_name)
    return getattr(source, field_name, None)


def _iter_ref_documents_for_value(info: RefFieldInfo, value: Any):
    if value is None:
        return

    if info.is_document_ref:
        if info.is_list:
            for item in value:
                if is_document_type(type(item)):
                    yield item
            return

        if is_document_type(type(value)):
            yield value
        return

    if info.is_list:
        for item in value:
            yield from iter_ref_documents(info.nested_fields, item)
        return

    yield from iter_ref_documents(info.nested_fields, value)


def _collapse_value(field_name: str, info: RefFieldInfo, value: Any) -> Any:
    if value is None:
        return value

    if info.is_document_ref:
        if info.is_list:
            collapsed = []
            for item in value:
                if isinstance(item, dict):
                    oid = item.get("_id")
                    if oid is None:
                        raise ValueError(
                            f"Cannot reference unsaved {info.document_class.__name__} "
                            f"in field '{field_name}'"
                        )
                    collapsed.append(oid)
                else:
                    collapsed.append(item)
            return collapsed

        if isinstance(value, dict):
            oid = value.get("_id")
            if oid is None:
                raise ValueError(
                    f"Cannot reference unsaved {info.document_class.__name__} "
                    f"in field '{field_name}'"
                )
            return oid
        return value

    if info.is_list:
        return [
            collapse_ref_data(info.nested_fields, item)
            if isinstance(item, dict) else item
            for item in value
        ]

    if isinstance(value, dict):
        return collapse_ref_data(info.nested_fields, value)
    return value


# ---------------------------------------------------------------------------
# Stub helpers for resolve_refs=False
# ---------------------------------------------------------------------------

def _make_stub_dict(doc_class: type, oid: ObjectId) -> dict[str, Any]:
    """Create a minimal dict that passes ``msgspec.convert`` for *doc_class*."""
    import msgspec.structs

    stub: dict[str, Any] = {"_id": oid}
    for f in msgspec.structs.fields(doc_class):
        if f.name == "_id":
            continue
        if f.default is not msgspec.NODEFAULT or f.default_factory is not msgspec.NODEFAULT:
            continue
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
    if is_struct_type(tp):
        stub: dict[str, Any] = {}
        for field_info in msgspec.structs.fields(tp):
            if field_info.default is not msgspec.NODEFAULT or field_info.default_factory is not msgspec.NODEFAULT:
                continue
            stub[field_info.name] = _zero_for_annotation(field_info.type)
        return stub
    return None


def stub_ref_data(ref_fields: dict[str, RefFieldInfo], raw: dict[str, Any]) -> dict[str, Any]:
    """Replace ObjectIds in ref fields with stub dicts (no DB queries)."""
    out = dict(raw)
    for field_name, info in ref_fields.items():
        value = out.get(field_name)
        if value is None:
            continue
        out[field_name] = _stub_value(info, value)
    return out


def _stub_value(info: RefFieldInfo, value: Any) -> Any:
    if value is None:
        return value

    if info.is_document_ref:
        if info.is_list:
            return [_stub_document_value(info.document_class, item) for item in value]
        return _stub_document_value(info.document_class, value)

    if info.is_list:
        return [_stub_struct_value(info.nested_fields, item) for item in value]
    return _stub_struct_value(info.nested_fields, value)


def _stub_document_value(doc_class: type, value: Any) -> Any:
    if isinstance(value, ObjectId):
        value = _make_stub_dict(doc_class, value)
    if isinstance(value, dict):
        nested_ref_fields = doc_class._get_ref_fields()
        if nested_ref_fields:
            return stub_ref_data(nested_ref_fields, value)
    return value


def _stub_struct_value(nested_fields: dict[str, RefFieldInfo], value: Any) -> Any:
    if isinstance(value, dict):
        return stub_ref_data(nested_fields, value)
    return value
