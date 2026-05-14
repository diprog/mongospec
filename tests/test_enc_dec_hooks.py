"""Тесты для ``default_enc_hook`` и ``default_dec_hook``.

Проверяют, что BSON-специфичные типы (``Int64``, ``Decimal128``)
прозрачно конвертируются в нативные питон-типы — без необходимости
определять ``enc_hook``/``dec_hook`` на каждом документе.

Раньше mongo возвращал ``Int64`` для int-полей, и любая попытка
``msgspec.to_builtins`` на ``dict[str, Any]`` со значением, прочитанным
из mongo, валилась ``NotImplementedError`` в ``default_enc_hook``.
"""
from __future__ import annotations

from decimal import Decimal

import msgspec
import pytest
from bson import int64
from bson.decimal128 import Decimal128

from mongospec.document.document import default_dec_hook, default_enc_hook


# ── default_enc_hook ────────────────────────────────────────────────────────


def test_enc_hook_int64_to_int():
    assert default_enc_hook(int64.Int64(42)) == 42
    assert isinstance(default_enc_hook(int64.Int64(42)), int)


def test_enc_hook_decimal128_to_decimal():
    out = default_enc_hook(Decimal128("3.14"))
    assert out == Decimal("3.14")
    assert isinstance(out, Decimal)


def test_enc_hook_decimal_to_str():
    # Plain Decimal isn't natively serializable by msgspec — surface as str
    # so callers using free-form ``dict[str, Any]`` payloads don't blow up.
    assert default_enc_hook(Decimal("1.23")) == "1.23"


def test_enc_hook_unknown_type_raises():
    class Custom: ...
    with pytest.raises(NotImplementedError):
        default_enc_hook(Custom())


# ── default_dec_hook ────────────────────────────────────────────────────────


def test_dec_hook_int64_to_int():
    assert default_dec_hook(int, int64.Int64(7)) == 7


def test_dec_hook_decimal128_to_decimal():
    assert default_dec_hook(Decimal, Decimal128("9.99")) == Decimal("9.99")


# ── End-to-end: msgspec.to_builtins with default_enc_hook ───────────────────


def test_to_builtins_with_int64_in_any_field():
    """Симулирует реальный кейс: ``meta`` приехал из mongo с ``Int64``."""

    class Doc(msgspec.Struct, kw_only=True):
        meta: dict[str, object]

    doc = Doc(meta={"old_points": int64.Int64(5100), "count": 3})
    out = msgspec.to_builtins(doc, enc_hook=default_enc_hook)
    assert out == {"meta": {"old_points": 5100, "count": 3}}
