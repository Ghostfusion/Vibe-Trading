"""Provider registry: name -> adapter instance.

Adapters self-register with :func:`register_provider` at import time, mirroring
``backtest.loaders.registry`` and ``src.tools`` discovery. Every provider named
by a category chain must appear here, or the resolver reports the category as
unsupported rather than silently serving nothing — a missing adapter is a
visible gap, not an empty result.
"""

from __future__ import annotations

import logging

from src.data_providers.base import Provider

logger = logging.getLogger(__name__)

#: name -> adapter instance.
PROVIDERS: dict[str, Provider] = {}

_imported = False


def register_provider(cls: type[Provider]) -> type[Provider]:
    """Class decorator: register an adapter under its ``name``.

    Args:
        cls: The adapter class.

    Returns:
        ``cls`` unchanged, so the decorator can be stacked freely.

    Raises:
        ValueError: When ``cls.name`` is empty or already registered under a
            different class.
    """
    if not cls.name:
        raise ValueError(f"{cls.__name__} declares no provider name")
    existing = PROVIDERS.get(cls.name)
    if existing is not None and type(existing) is not cls:
        raise ValueError(
            f"provider name {cls.name!r} is already registered by "
            f"{type(existing).__name__}"
        )
    PROVIDERS[cls.name] = cls()
    return cls


def _ensure_imported() -> None:
    """Import every adapter module so the decorators fire."""
    global _imported
    if _imported:
        return
    from src.data_providers.providers import load_all  # noqa: PLC0415

    load_all()
    _imported = True


def get_provider(name: str) -> Provider | None:
    """Return the adapter registered as *name*, or ``None``.

    Args:
        name: Provider identifier used in category chains.
    """
    _ensure_imported()
    return PROVIDERS.get(name)


def registered_providers() -> tuple[str, ...]:
    """Return every registered provider name (sorted)."""
    _ensure_imported()
    return tuple(sorted(PROVIDERS))
