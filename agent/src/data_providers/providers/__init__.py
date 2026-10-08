"""Provider adapters.

Each module registers exactly one adapter via
:func:`~src.data_providers.registry.register_provider`. Optional third-party
SDKs (``futu-api``) are imported lazily inside the methods that need them so a
missing SDK degrades to "unavailable" instead of breaking the whole layer.
"""

from __future__ import annotations

import importlib
import logging

logger = logging.getLogger(__name__)

#: Every adapter module that must be imported for the chains to be complete.
PROVIDER_MODULES: tuple[str, ...] = (
    "alpha_vantage",
    "benzinga",
    "cboe",
    "eodhd",
    "federal_reserve",
    "finnhub",
    "fred",
    "gdelt",
    "massive",
    "moomoo",
    "newsapi",
    "polymarket",
    "sec_edgar",
    "stockdata",
    "tiingo",
    "twelve_data",
    "yfinance",
)


def load_all() -> dict[str, str]:
    """Import every adapter module, returning ``{module: reason}`` for failures.

    A module that fails to import is a visible gap: the resolver will report its
    provider as unregistered, which is preferable to a chain that silently has
    one fewer source than the operator asked for.
    """
    failures: dict[str, str] = {}
    for module_name in PROVIDER_MODULES:
        try:
            importlib.import_module(f"src.data_providers.providers.{module_name}")
        except Exception as exc:  # noqa: BLE001 - one bad adapter must not hide the rest
            reason = f"{type(exc).__name__}: {exc}"
            failures[module_name] = reason
            logger.warning("data_providers: skipped adapter %s: %s", module_name, reason)
    return failures
