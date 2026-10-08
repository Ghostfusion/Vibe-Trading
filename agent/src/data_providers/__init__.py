"""Category-based provider failover for market and market-adjacent data.

Public surface::

    from src.data_providers import resolve, resolve_with_trace, call_provider

    result = resolve("core_stock_apis", symbol="AAPL.US", start="2026-01-01")
    result.provider   # who served it
    result.data       # normalised payload

One category, one ordered chain (``categories.CATEGORY_CHAINS``), first provider
that answers wins, no provider is ever retried within a request. See
``resolver`` for the rule stated in full and ``schemas`` for the normal form of
each category's payload.
"""

from __future__ import annotations

from src.data_providers.base import Provider, ProviderResult, env_value
from src.data_providers.categories import (
    CATEGORIES,
    CATEGORY_CHAIN_EXTENSIONS,
    CATEGORY_CHAINS,
    categories_for_provider,
    chain_for,
    known_categories,
    providers_in_chains,
)
from src.data_providers.errors import (
    ChainExhausted,
    ProviderError,
    ProviderHTTPError,
    ProviderUnavailable,
)
from src.data_providers.registry import get_provider, registered_providers
from src.data_providers.resolver import (
    Attempt,
    Resolution,
    call_provider,
    describe_chains,
    resolve,
    resolve_with_trace,
)
from src.data_providers.schemas import CATEGORY_SHAPES, shape_for

__all__ = [
    "CATEGORIES",
    "CATEGORY_CHAIN_EXTENSIONS",
    "CATEGORY_CHAINS",
    "CATEGORY_SHAPES",
    "Attempt",
    "ChainExhausted",
    "Provider",
    "ProviderError",
    "ProviderHTTPError",
    "ProviderResult",
    "ProviderUnavailable",
    "Resolution",
    "call_provider",
    "categories_for_provider",
    "chain_for",
    "describe_chains",
    "env_value",
    "get_provider",
    "known_categories",
    "providers_in_chains",
    "registered_providers",
    "resolve",
    "resolve_with_trace",
    "shape_for",
]
