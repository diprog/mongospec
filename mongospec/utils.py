import importlib
import inspect
import pkgutil
import re
import types
import warnings
from collections.abc import Callable, Iterable
from typing import TypeVar

from .document import MongoDocument

T = TypeVar("T", bound=MongoDocument)


def collect_document_types(
    targets: str | types.ModuleType | Iterable[str | types.ModuleType],
    *,
    base: type | tuple[type, ...] = MongoDocument,
    include_base: bool = False,
    local_only: bool = True,
    ignore_abstract: bool = True,
    predicate: Callable[[type], bool] | None = None,
    module_filter: str | Callable[[str], bool] | None = None,
    on_error: str | Callable[[str, BaseException], None] = "warn",
    recursive: bool = True,
    unique: bool = True,
    return_map: bool = False,
) -> list[type[T]] | dict[str, type[T]]:
    """
    Discover and collect document classes under one or more packages/modules.

    :param targets: Package/module name (str), module object, or an iterable of those.
    :param base: Base class or tuple of base classes to match. Defaults to MongoDocument.
    :param include_base: Include the base class itself in results.
    :param local_only: If True, only classes defined in the module where found
        (i.e. obj.__module__ == module.__name__). If False, include re-exported classes.
    :param ignore_abstract: If True, skip classes that declare abstract methods.
    :param predicate: Optional callable to apply as an additional filter: predicate(cls) -> bool.
    :param module_filter: Regex pattern string or callable (name: str) -> bool to filter module names.
        Applied to fully qualified module names. If provided and returns False, the module is skipped.
    :param on_error: How to handle import errors while traversing:
        - "ignore": silently skip
        - "warn": warnings.warn (default)
        - "raise": re-raise the exception
        - callable(name, exc): custom handler
    :param recursive: If True, walk subpackages with pkgutil.walk_packages. If False, only inspect the target module itself.
    :param unique: If True, de-duplicate classes by identity while preserving deterministic order.
    :param return_map: If True, return {qualified_name: cls}. Otherwise return [cls, ...].

    :returns: A list of classes (default) or a mapping qualified_name -> class if return_map=True.
    """
    bases: tuple[type, ...] = (base,) if isinstance(base, type) else tuple(base)

    if isinstance(targets, (str, types.ModuleType)):
        targets = [targets]

    # Prepare module name allowlist via regex/callable
    if isinstance(module_filter, str):
        pattern = re.compile(module_filter)

        def _mod_ok(name: str) -> bool:
            return bool(pattern.search(name))
    elif callable(module_filter):
        _mod_ok = module_filter
    else:
        def _mod_ok(_name: str) -> bool:
            return True

    def _handle_error(mod_name: str, exc: BaseException) -> None:
        if on_error == "ignore":
            return
        if on_error == "warn":
            warnings.warn(f"collect_document_types: failed to import {mod_name!r}: {exc}", RuntimeWarning)
            return
        if on_error == "raise":
            raise
        if callable(on_error):
            on_error(mod_name, exc)
            return
        warnings.warn(f"collect_document_types: unknown on_error={on_error!r}, falling back to 'warn': {exc}",
                      RuntimeWarning)

    # Normalize targets to module objects
    modules: list[types.ModuleType] = []
    for target in targets:
        if isinstance(target, str):
            try:
                modules.append(importlib.import_module(target))
            except BaseException as exc:
                _handle_error(target, exc)
        elif isinstance(target, types.ModuleType):
            modules.append(target)
        else:
            raise TypeError(f"Unsupported target type: {type(target).__name__}")

    # Walk modules
    to_visit: list[str] = []
    for mod in modules:
        # Always include the root module itself
        to_visit.append(mod.__name__)
        # Optionally include submodules
        if recursive and hasattr(mod, "__path__"):
            for _finder, name, _is_pkg in pkgutil.walk_packages(mod.__path__, prefix=mod.__name__ + "."):
                if _mod_ok(name):
                    to_visit.append(name)

    # Import all target module names (with filtering applied)
    imported: list[types.ModuleType] = []
    seen_mods: set[str] = set()
    for name in to_visit:
        if name in seen_mods:
            continue
        seen_mods.add(name)
        if not _mod_ok(name):
            continue
        try:
            imported.append(importlib.import_module(name))
        except BaseException as exc:
            _handle_error(name, exc)

    # Collect classes
    acc_list: list[type] = []
    acc_set: set[type] = set()

    for module in imported:
        for _n, obj in inspect.getmembers(module, inspect.isclass):
            # Base check
            try:
                is_sub = issubclass(obj, bases)
            except Exception:
                # Some dynamic classes may raise here; skip them
                continue
            if not is_sub:
                continue
            if not include_base and obj in bases:
                continue
            if local_only and obj.__module__ != module.__name__:
                continue
            if ignore_abstract and getattr(obj, "__abstractmethods__", None):
                if len(getattr(obj, "__abstractmethods__", ())) > 0:
                    continue
            if predicate and not predicate(obj):
                continue

            if unique:
                if obj in acc_set:
                    continue
                acc_set.add(obj)
            acc_list.append(obj)

    # Deterministic order: sort by fully qualified name
    acc_list.sort(key=lambda c: f"{c.__module__}.{c.__name__}")

    if return_map:
        return {f"{c.__module__}.{c.__name__}": c for c in acc_list}
    return acc_list
